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

"""Forward MCP service error logs to an external webhook.

Everything here is best-effort and non-blocking: the HTTP POST happens on a
daemon thread fed by a bounded queue, so webhook latency (or an unreachable
webhook) never blocks an MCP tool call, and no failure is ever propagated to
the caller.
"""

from __future__ import annotations

import atexit
import logging
import os
import queue
import re
import threading
from typing import Any, Iterable, Mapping

import requests

from superset.mcp_service.middleware import (
    _sanitize_error_for_logging,
    _SENSITIVE_PARAM_KEYS,
)
from superset.utils import json

logger = logging.getLogger(__name__)

DEFAULT_MIN_LEVEL = logging.WARNING
DEFAULT_TIMEOUT = 3
DEFAULT_QUEUE_SIZE = 1000

#: Loggers the handler is attached to. Covers Superset's own errors as well as
#: the FastMCP / MCP SDK error paths, which log outside of Superset's tree.
WEBHOOK_LOGGER_NAMES = ("superset", "mcp", "fastmcp")

# Matches ``password=...``, ``"token": "..."``, ``api_key: ...`` and friends so
# secrets embedded in free-form log text are stripped before leaving the pod.
_SENSITIVE_TEXT_RE = re.compile(
    r"(?i)([\"']?\b(?:" + "|".join(sorted(_SENSITIVE_PARAM_KEYS)) + r")\b[\"']?"
    r"\s*[:=]\s*)([\"']?)([^\s,;'\"}\)]{1,200})\2"
)


def _sanitize_text(text: str) -> str:
    """Redact connection strings, tokens, emails, IPs and sensitive keys."""
    sanitized = _sanitize_error_for_logging(Exception(text))
    return _SENSITIVE_TEXT_RE.sub(r"\1\g<2>[REDACTED]\2", sanitized)


class _AsyncPoster:
    """Bounded queue + daemon thread that POSTs JSON payloads."""

    _SENTINEL = object()

    def __init__(self, queue_size: int = DEFAULT_QUEUE_SIZE) -> None:
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=queue_size)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="mcp-webhook-poster", daemon=True
            )
            self._thread.start()

    def submit(
        self,
        url: str,
        payload: dict[str, Any],
        timeout: float,
        headers: Mapping[str, str] | None = None,
        on_error: Any = None,
    ) -> bool:
        """Enqueue a POST. Returns False when the queue is full (payload dropped)."""
        self._ensure_thread()
        try:
            self._queue.put_nowait(
                (url, payload, timeout, dict(headers or {}), on_error)
            )
        except queue.Full:
            return False
        return True

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is self._SENTINEL:
                return
            url, payload, timeout, headers, on_error = item
            try:
                self._post(url, payload, timeout, headers)
            except Exception:  # pylint: disable=broad-except
                if on_error is not None:
                    try:
                        on_error()
                    except Exception:  # pylint: disable=broad-except
                        logger.debug("Webhook error callback failed", exc_info=True)
            finally:
                self._queue.task_done()

    @staticmethod
    def _post(
        url: str, payload: dict[str, Any], timeout: float, headers: dict[str, str]
    ) -> None:
        requests.post(url, json=payload, timeout=timeout, headers=headers or None)

    def flush(self, timeout: float = 2.0) -> None:
        """Best-effort wait for pending payloads (used by tests and shutdown)."""
        drained = threading.Event()

        def _wait() -> None:
            self._queue.join()
            drained.set()

        threading.Thread(target=_wait, daemon=True).start()
        drained.wait(timeout)


#: Shared poster so the log handler and the event logger use one thread.
_POSTER = _AsyncPoster()


def send_webhook_async(
    url: str,
    payload: dict[str, Any],
    timeout: float = DEFAULT_TIMEOUT,
    headers: Mapping[str, str] | None = None,
) -> bool:
    """Fire-and-forget JSON POST. Never raises."""
    try:
        return _POSTER.submit(url, payload, timeout, headers)
    except Exception:  # pylint: disable=broad-except
        return False


atexit.register(_POSTER.flush, 1.0)


class WebhookLogHandler(logging.Handler):
    """Logging handler that forwards records to an HTTP webhook.

    The handler never raises and never blocks: payloads are built inline (cheap)
    and handed to a bounded queue drained by a daemon thread.
    """

    def __init__(
        self,
        url: str,
        min_level: int = DEFAULT_MIN_LEVEL,
        timeout: float = DEFAULT_TIMEOUT,
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(level=min_level)
        self.url = url
        self.timeout = timeout
        self.extra_headers = dict(extra_headers or {})

    def build_payload(self, record: logging.LogRecord) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timestamp": record.created,
            "level": record.levelname,
            "logger": record.name,
            "message": _sanitize_text(record.getMessage()),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["traceback"] = _sanitize_text(
                logging.Formatter().formatException(record.exc_info)
            )
        return payload

    def emit(self, record: logging.LogRecord) -> None:
        # Avoid feedback loops if this module ever logs while forwarding.
        if record.name == __name__:
            return
        try:
            payload = self.build_payload(record)
            send_webhook_async(
                self.url,
                payload,
                timeout=self.timeout,
                headers=self.extra_headers,
            )
        except Exception:  # pylint: disable=broad-except
            self.handleError(record)


def _parse_level(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        resolved = logging.getLevelName(value.strip().upper())
        if isinstance(resolved, int):
            return resolved
    return DEFAULT_MIN_LEVEL


def _parse_headers(value: Any) -> dict[str, str]:
    if isinstance(value, Mapping):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:  # pylint: disable=broad-except
            return {}
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def get_webhook_config(flask_app: Any = None) -> dict[str, Any] | None:
    """Resolve webhook settings from Flask config, falling back to env vars.

    Returns ``None`` when no URL is configured (feature disabled).
    """

    def _get(name: str) -> Any:
        if flask_app is not None:
            value = flask_app.config.get(name)
            if value not in (None, ""):
                return value
        return os.environ.get(name)

    url = _get("MCP_ERROR_WEBHOOK_URL")
    if not url:
        return None
    timeout_raw = _get("MCP_ERROR_WEBHOOK_TIMEOUT")
    try:
        timeout = (
            float(timeout_raw) if timeout_raw not in (None, "") else DEFAULT_TIMEOUT
        )
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT
    return {
        "url": str(url),
        "min_level": _parse_level(_get("MCP_ERROR_WEBHOOK_MIN_LEVEL")),
        "timeout": timeout,
        "headers": _parse_headers(_get("MCP_ERROR_WEBHOOK_HEADERS")),
    }


def build_webhook_handler(flask_app: Any = None) -> WebhookLogHandler | None:
    """Create a handler from config, or ``None`` when the webhook is disabled."""
    config = get_webhook_config(flask_app)
    if config is None:
        return None
    return WebhookLogHandler(
        url=config["url"],
        min_level=config["min_level"],
        timeout=config["timeout"],
        extra_headers=config["headers"],
    )


def attach_webhook_handler(
    flask_app: Any = None,
    logger_names: Iterable[str] = WEBHOOK_LOGGER_NAMES,
) -> WebhookLogHandler | None:
    """Attach a :class:`WebhookLogHandler` to the MCP-relevant root loggers.

    No-op (returns ``None``) when ``MCP_ERROR_WEBHOOK_URL`` is not configured.
    Idempotent: a logger that already has a webhook handler is skipped.
    """
    handler = build_webhook_handler(flask_app)
    if handler is None:
        return None
    for name in logger_names:
        target = logging.getLogger(name)
        if any(isinstance(h, WebhookLogHandler) for h in target.handlers):
            continue
        target.addHandler(handler)
        # Ensure records at the handler's level actually reach it.
        if target.level > handler.level:
            target.setLevel(handler.level)
    return handler
