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

"""What the real Devin API hands back, and how the client reads it."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from error_orchestrator.devin_client import HttpDevinClient


def _transport(*polls: dict[str, Any]) -> httpx.MockTransport:
    """Serve one session creation followed by ``polls`` session details."""
    remaining = list(polls)

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"session_id": "devin-abc", "url": None})
        return httpx.Response(200, json=remaining.pop(0))

    return httpx.MockTransport(handle)


async def _run(monkeypatch: pytest.MonkeyPatch, *polls: dict[str, Any]) -> Any:
    transport = _transport(*polls)
    original = httpx.AsyncClient

    def client(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=transport, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    devin = HttpDevinClient(api_key="k", poll_interval=0.0)
    return await devin.run_session("prompt")


@pytest.mark.asyncio
async def test_a_session_without_a_url_still_gets_a_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _run(
        monkeypatch, {"status_enum": "finished", "structured_output": {"ok": True}}
    )

    assert result.url == "https://app.devin.ai/sessions/devin-abc"


@pytest.mark.asyncio
async def test_a_blocked_session_is_only_an_answer_once_it_has_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _run(
        monkeypatch,
        {"status_enum": "blocked", "structured_output": None},
        {"status_enum": "blocked", "structured_output": {"reproduced": False}},
    )

    assert result.structured_output == {"reproduced": False}


@pytest.mark.asyncio
async def test_an_answer_written_as_a_json_block_still_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answer = '```json\n{"reproduced": true, "diff": "--- a\\n+++ b\\n"}\n```'
    result = await _run(
        monkeypatch,
        {
            "status_enum": "blocked",
            "structured_output": None,
            "messages": [{"message": "working"}, {"message": answer}],
        },
    )

    assert result.structured_output["reproduced"] is True
