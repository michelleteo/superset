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

"""Thin async client for the Devin API used by the handler lanes.

Handlers only need one operation — "run this prompt to completion and give me
the structured output" — so that is the whole protocol. Capacity is enforced
by the orchestrator's lane semaphores, not here.
"""

from __future__ import annotations

import asyncio

# This package never imports Superset, so superset.utils.json is unavailable.
import json  # noqa: TID251
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

import httpx

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://api.devin.ai/v1"
DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_TIMEOUT = 60 * 60.0
#: A blocked session is waiting on us, so it is asked for its answer rather than
#: polled until the timeout.
DEFAULT_NUDGE_INTERVAL = 60.0
DEFAULT_MAX_NUDGES = 2
NUDGE_MESSAGE = (
    "You are blocked and no machine-readable answer has reached the caller. "
    "Reply with ONE fenced ```json block containing exactly the keys the "
    "prompt asked for, and nothing else \u2014 no prose, no attachments, no "
    'pointer to an earlier message. Put any patch inline in "diff" as '
    "`git diff` prints it. If you could not do the work, say so in the same "
    "JSON shape instead of asking a question."
)
_TERMINAL_STATUSES = frozenset({"stopped", "finished", "expired"})
#: ``blocked`` also covers a session pausing to ask a question, so it only
#: counts as an answer once the session has produced its structured output.
_ANSWERED_STATUSES = frozenset({"blocked"})
SESSION_URL = "https://app.devin.ai/sessions/{session_id}"
#: Sessions reliably *write* the requested JSON, but do not always publish it
#: as structured output, so the last fenced JSON block is read as a fallback.
_JSON_BLOCK = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
#: A session often renders its patch as its own block and leaves the JSON's
#: ``diff`` empty, which would otherwise read as "fixed nothing".
_DIFF_BLOCK = re.compile(r"```diff\s*(.*?)```", re.DOTALL)
_PATCH_MARKERS = ("diff --git", "--- ", "@@")


def _is_patch(value: object) -> bool:
    """A patch, rather than a session describing where it put one."""
    return isinstance(value, str) and any(m in value for m in _PATCH_MARKERS)


def _reported_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    """The answer a session wrote into its last message, when it wrote one."""
    messages = payload.get("messages") or []
    for message in reversed(list(messages)):
        match = _JSON_BLOCK.search(str(message.get("message") or ""))
        if match is None:
            continue
        try:
            parsed = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        if not _is_patch(parsed.get("diff")):
            block = _DIFF_BLOCK.search(str(message.get("message") or ""))
            # Prose about where the patch went is not a patch: an empty diff is
            # a state the orchestrator handles, a fake one is not.
            parsed["diff"] = block.group(1).strip() if block else ""
        return parsed
    return {}


@dataclass
class _BlockedWatch:
    """How long a session has been blocked with nothing machine-readable."""

    interval: float
    max_nudges: int
    since: float | None = None
    nudges: int = 0

    def clear(self) -> None:
        self.since = None

    def due(self, now: float) -> bool:
        """Whether the session has been blocked long enough to be asked again."""
        if self.since is None:
            self.since = now
            return False
        if now - self.since < self.interval:
            return False
        self.since = now
        return True

    @property
    def exhausted(self) -> bool:
        return self.nudges >= self.max_nudges


class DevinError(RuntimeError):
    """Raised when a Devin session cannot be created or does not complete."""


@dataclass
class DevinSessionResult:
    session_id: str
    url: str
    status: str
    structured_output: dict[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status in ("blocked", "finished") and bool(self.structured_output)


class DevinClient(Protocol):
    """Everything the handlers need from Devin."""

    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> DevinSessionResult: ...


class HttpDevinClient:
    """Real client: create a session, poll until it reaches a terminal status."""

    def __init__(
        self,
        api_key: str,
        api_base: str = DEFAULT_API_BASE,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        timeout: float = DEFAULT_TIMEOUT,
        playbook_id: str | None = None,
        nudge_interval: float = DEFAULT_NUDGE_INTERVAL,
        max_nudges: int = DEFAULT_MAX_NUDGES,
    ) -> None:
        if not api_key:
            raise DevinError("DEVIN_API_KEY is required for HttpDevinClient")
        self._api_key = api_key
        self._api_base = api_base.rstrip("/")
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._playbook_id = playbook_id
        self._nudge_interval = nudge_interval
        self._max_nudges = max_nudges

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> DevinSessionResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            session_id, url = await self._create(
                client, prompt, title=title, tags=tags, idempotent=idempotent
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._timeout
            watch = _BlockedWatch(self._nudge_interval, self._max_nudges)
            while True:
                if loop.time() > deadline:
                    raise DevinError(f"session {session_id} timed out")
                await asyncio.sleep(self._poll_interval)
                detail = await client.get(
                    f"{self._api_base}/session/{session_id}", headers=self._headers
                )
                if detail.status_code >= 400:
                    continue
                payload = detail.json()
                status = str(payload.get("status_enum") or payload.get("status") or "")
                output = payload.get("structured_output") or _reported_output(payload)
                answered = status in _ANSWERED_STATUSES and bool(output)
                if status in _TERMINAL_STATUSES or answered:
                    return DevinSessionResult(
                        session_id=session_id,
                        url=payload.get("url") or url,
                        status=status,
                        structured_output=output,
                    )
                if status not in _ANSWERED_STATUSES:
                    watch.clear()
                    continue
                # Blocked with nothing to read: the session is waiting on an
                # answer, or wrote its result as prose. Either way it will not
                # move again on its own, so ask, then give the lane its worker
                # back rather than holding it for the whole timeout.
                if not watch.due(loop.time()):
                    continue
                if watch.exhausted:
                    raise DevinError(
                        f"session {payload.get('url') or url} stayed blocked "
                        f"without a machine-readable answer after "
                        f"{watch.nudges} nudge(s)"
                    )
                watch.nudges += 1
                await self._nudge(client, session_id, watch.nudges)

    async def _create(
        self,
        client: httpx.AsyncClient,
        prompt: str,
        *,
        title: str | None,
        tags: list[str] | None,
        idempotent: bool,
    ) -> tuple[str, str]:
        """Create the session and return its id plus the page to link to."""
        body: dict[str, Any] = {"prompt": prompt, "idempotent": idempotent}
        if title:
            body["title"] = title
        if tags:
            body["tags"] = tags
        if self._playbook_id:
            body["playbook_id"] = self._playbook_id
        response = await client.post(
            f"{self._api_base}/sessions", json=body, headers=self._headers
        )
        if response.status_code >= 400:
            raise DevinError(
                f"session creation failed ({response.status_code}): {response.text}"
            )
        created = response.json()
        session_id = str(created["session_id"])
        # The page is keyed by the bare id, while the API returns it prefixed.
        url = created.get("url") or SESSION_URL.format(
            session_id=session_id.removeprefix("devin-")
        )
        return session_id, url

    async def _nudge(
        self, client: httpx.AsyncClient, session_id: str, attempt: int
    ) -> None:
        """Ask a blocked session for its answer in the shape the caller reads."""
        logger.info("nudging blocked session %s (attempt %s)", session_id, attempt)
        try:
            response = await client.post(
                f"{self._api_base}/session/{session_id}/message",
                json={"message": NUDGE_MESSAGE},
                headers=self._headers,
            )
        except httpx.HTTPError:
            logger.warning("could not nudge session %s", session_id, exc_info=True)
            return
        if response.status_code >= 400:
            logger.warning(
                "nudge for session %s rejected (%s)", session_id, response.status_code
            )


@dataclass
class ScriptedDevinClient:
    """Test/dry-run double: maps a prompt to a canned structured output.

    ``responder`` receives the prompt and returns the structured output the
    session would have produced.
    """

    responder: Callable[[str], Mapping[str, Any]]
    latency: float = 0.0
    prompts: list[str] = field(default_factory=list)
    _counter: int = 0

    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> DevinSessionResult:
        self.prompts.append(prompt)
        self._counter += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        output = dict(self.responder(prompt))
        session_id = f"scripted-{self._counter}"
        return DevinSessionResult(
            session_id=session_id,
            url=f"https://app.devin.ai/sessions/{session_id}",
            status="finished",
            structured_output=output,
        )
