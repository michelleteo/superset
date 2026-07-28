# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Normalize error events into a canonical form and hash it.

Two occurrences of the same bug rarely produce byte-identical text: memory
addresses, UUIDs, timestamps and interpolated values differ every time. We
template those out, keep the structural parts (the call sequence of function
names plus the exception type, with line numbers), and hash the result. That
gives an O(1) dedup key for identical traces.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from error_orchestrator.models import ErrorEvent

#: ``File "/app/superset/x.py", line 12, in do_thing``
_FRAME_RE = re.compile(
    r'^\s*File "(?P<file>[^"]+)", line (?P<line>\d+), in (?P<func>\S+)', re.MULTILINE
)
#: Final ``superset.errors.SupersetError: boom`` line of a traceback.
_EXC_LINE_RE = re.compile(
    r"^(?P<type>[A-Za-z_][\w.]*(?:Error|Exception|Exit|Interrupt|Warning|Timeout))"
    r"(?::\s*(?P<msg>.*))?$"
)

# Ordered: the more specific patterns must win over the generic number rule.
_SCRUBBERS: tuple[tuple[re.Pattern[str], str], ...] = (
    # 0x7f3c1a2b3c4d, <object at 0x...>
    (re.compile(r"0x[0-9a-fA-F]{4,}"), "<ADDR>"),
    (
        re.compile(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
        ),
        "<UUID>",
    ),
    # 2024-05-06T07:08:09.123456+00:00 / 2024-05-06 07:08:09
    (
        re.compile(
            r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
        ),
        "<TIMESTAMP>",
    ),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}\b"), "<DATE>"),
    (re.compile(r"\b\d{2}:\d{2}:\d{2}(?:\.\d+)?\b"), "<TIME>"),
    (re.compile(r"\b\d{10,}(?:\.\d+)?\b"), "<EPOCH>"),
    (re.compile(r"\b[0-9a-fA-F]{32,}\b"), "<HASH>"),
    (re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.]+\b"), "<EMAIL>"),
    (re.compile(r"\bhttps?://\S+"), "<URL>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<IP>"),
    (re.compile(r"(?<![\w/])(?:/[\w.\-@+]+){2,}/?"), "<PATH>"),
    (re.compile(r"'[^']*'|\"[^\"]*\""), "<STR>"),
    # Durations and sizes: "after 30s", "1.5 MiB" - the unit is structural.
    (
        re.compile(
            r"(?<![\w.])-?\d+(?:\.\d+)?(?=\s?"
            r"(?:ns|us|ms|s|m|h|d|[kmgt]i?b|b|%)\b)",
            re.IGNORECASE,
        ),
        "<NUM>",
    ),
    (re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w.])"), "<NUM>"),
)

_WS_RE = re.compile(r"\s+")
#: Site-packages / stdlib frames add noise without identifying the bug.
_VENDOR_MARKERS = ("/site-packages/", "/dist-packages/", "/lib/python")


def normalize_text(text: str) -> str:
    """Template out every instance-specific token in a free-form string."""
    normalized = text
    for pattern, placeholder in _SCRUBBERS:
        normalized = pattern.sub(placeholder, normalized)
    return _WS_RE.sub(" ", normalized).strip()


@dataclass(frozen=True)
class Frame:
    module: str
    func: str
    line: int

    def render(self) -> str:
        return f"{self.module}:{self.func}:{self.line}"


@dataclass(frozen=True)
class CanonicalError:
    """Instance-independent description of an error."""

    exception_type: str
    message_template: str
    frames: tuple[Frame, ...] = ()
    logger: str = ""
    #: False when the single frame was synthesized from the log record's
    #: module/func/line rather than parsed out of a real traceback.
    frames_from_traceback: bool = False

    def render(self) -> str:
        """Human-readable canonical form (also the pool's stored signature)."""
        call_sequence = "|".join(frame.render() for frame in self.frames)
        return "\n".join(
            [
                f"exc={self.exception_type}",
                f"msg={self.message_template}",
                f"frames={call_sequence}",
            ]
        )

    def hash_form(self) -> str:
        """What actually gets hashed.

        A real call sequence plus the exception type is the identity of the
        failure, and the message is left out because scrubbing cannot reliably
        template interpolated identifiers ("dataset sales" vs "dataset
        marketing"). Without a traceback there is no call sequence to rely on -
        one log site emits many unrelated errors - so the templated message
        joins whatever location we do have.
        """
        call_sequence = "|".join(frame.render() for frame in self.frames)
        if self.frames_from_traceback:
            return f"exc={self.exception_type}\nframes={call_sequence}"
        return (
            f"exc={self.exception_type}\nframes={call_sequence}"
            f"\nmsg={self.message_template}"
        )

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.hash_form().encode("utf-8")).hexdigest()[:32]

    @property
    def title(self) -> str:
        message = self.message_template[:120]
        return f"{self.exception_type}: {message}" if message else self.exception_type


@dataclass
class _ParsedTraceback:
    frames: list[Frame] = field(default_factory=list)
    exception_type: str = ""
    exception_message: str = ""


def _module_of(path: str) -> str:
    """Turn ``/app/superset/models/core.py`` into ``superset.models.core``."""
    trimmed = path.replace("\\", "/").removesuffix(".py")
    parts = [part for part in trimmed.split("/") if part not in ("", ".")]
    for marker in ("superset", "site-packages", "dist-packages"):
        if marker in parts:
            index = parts.index(marker)
            parts = parts[index:] if marker == "superset" else parts[index + 1 :]
            break
    else:
        parts = parts[-2:]
    return ".".join(parts)


def parse_traceback(traceback_text: str) -> _ParsedTraceback:
    parsed = _ParsedTraceback()
    for match in _FRAME_RE.finditer(traceback_text):
        path = match.group("file")
        if any(marker in path for marker in _VENDOR_MARKERS):
            continue
        parsed.frames.append(
            Frame(
                module=_module_of(path),
                func=match.group("func"),
                line=int(match.group("line")),
            )
        )
    for raw_line in reversed(traceback_text.strip().splitlines()):
        line = raw_line.strip()
        if not line or line.startswith(("File ", "Traceback", "During handling")):
            continue
        exc_match = _EXC_LINE_RE.match(line)
        if exc_match:
            parsed.exception_type = exc_match.group("type").rsplit(".", 1)[-1]
            parsed.exception_message = exc_match.group("msg") or ""
        break
    return parsed


def canonicalize(event: ErrorEvent) -> CanonicalError:
    """Reduce an event to its instance-independent, hashable form."""
    parsed = parse_traceback(event.traceback) if event.traceback else _ParsedTraceback()
    frames = tuple(parsed.frames)
    from_traceback = bool(frames)
    if not frames and event.module:
        frames = (Frame(module=event.module, func=event.func, line=event.line),)
    message = parsed.exception_message or event.message
    return CanonicalError(
        exception_type=parsed.exception_type or _guess_exception_type(event.message),
        message_template=normalize_text(message),
        frames=frames,
        logger=event.logger,
        frames_from_traceback=from_traceback,
    )


def _guess_exception_type(message: str) -> str:
    """Best effort for log lines that carry no traceback."""
    if match := _EXC_LINE_RE.match(message.strip()):
        return match.group("type").rsplit(".", 1)[-1]
    return "LogError"


def fingerprint_event(event: ErrorEvent) -> str:
    return canonicalize(event).fingerprint
