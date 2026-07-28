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
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

import httpx

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://api.devin.ai/v1"
DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_TIMEOUT = 60 * 60.0
_TERMINAL_STATUSES = frozenset({"blocked", "stopped", "finished", "expired"})


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
    ) -> None:
        if not api_key:
            raise DevinError("DEVIN_API_KEY is required for HttpDevinClient")
        self._api_key = api_key
        self._api_base = api_base.rstrip("/")
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._playbook_id = playbook_id

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
        body: dict[str, Any] = {"prompt": prompt, "idempotent": idempotent}
        if title:
            body["title"] = title
        if tags:
            body["tags"] = tags
        if self._playbook_id:
            body["playbook_id"] = self._playbook_id

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self._api_base}/sessions", json=body, headers=self._headers
            )
            if response.status_code >= 400:
                raise DevinError(
                    f"session creation failed ({response.status_code}): {response.text}"
                )
            created = response.json()
            session_id = created["session_id"]
            url = created.get("url", "")
            deadline = asyncio.get_running_loop().time() + self._timeout
            while True:
                if asyncio.get_running_loop().time() > deadline:
                    raise DevinError(f"session {session_id} timed out")
                await asyncio.sleep(self._poll_interval)
                detail = await client.get(
                    f"{self._api_base}/session/{session_id}", headers=self._headers
                )
                if detail.status_code >= 400:
                    continue
                payload = detail.json()
                status = str(payload.get("status_enum") or payload.get("status") or "")
                if status in _TERMINAL_STATUSES:
                    return DevinSessionResult(
                        session_id=session_id,
                        url=payload.get("url", url),
                        status=status,
                        structured_output=payload.get("structured_output") or {},
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
