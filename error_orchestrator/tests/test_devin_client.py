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

# This package never imports Superset, so superset.utils.json is unavailable.
import json  # noqa: TID251
from typing import Any, Callable

import httpx
import pytest

from error_orchestrator.devin_client import (
    DevinError,
    DevinSessionResult,
    HttpDevinClient,
    ScriptedDevinClient,
)


def _transport(
    *polls: dict[str, Any],
    messages: list[str] | None = None,
    created: httpx.Response | None = None,
    nudge_response: Callable[[], httpx.Response] | None = None,
    requests: list[httpx.Request] | None = None,
) -> httpx.MockTransport:
    """Serve one session creation followed by ``polls`` session details.

    The last poll repeats once exhausted, so a test can keep a session blocked
    for as long as the client is willing to wait. Nudges sent to the session are
    collected in ``messages``. A poll given as an ``int`` is served as that
    status code instead of a body.
    """
    remaining = list(polls)

    def handle(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(request)
        if request.method == "POST":
            if request.url.path.endswith("/message"):
                if messages is not None:
                    messages.append(request.read().decode())
                return nudge_response() if nudge_response else httpx.Response(200)
            if created is not None:
                return created
            return httpx.Response(200, json={"session_id": "devin-abc", "url": None})
        poll = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        if isinstance(poll, int):
            return httpx.Response(poll, json={"error": "nope"})
        return httpx.Response(200, json=poll)

    return httpx.MockTransport(handle)


async def _run(
    monkeypatch: pytest.MonkeyPatch,
    *polls: dict[str, Any],
    messages: list[str] | None = None,
    created: httpx.Response | None = None,
    nudge_response: Callable[[], httpx.Response] | None = None,
    requests: list[httpx.Request] | None = None,
    session_kwargs: dict[str, Any] | None = None,
    **client_kwargs: Any,
) -> Any:
    transport = _transport(
        *polls,
        messages=messages,
        created=created,
        nudge_response=nudge_response,
        requests=requests,
    )
    original = httpx.AsyncClient

    def client(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=transport, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    devin = HttpDevinClient(api_key="k", poll_interval=0.0, **client_kwargs)
    return await devin.run_session("prompt", **(session_kwargs or {}))


@pytest.mark.asyncio
async def test_a_session_without_a_url_still_gets_a_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _run(
        monkeypatch, {"status_enum": "finished", "structured_output": {"ok": True}}
    )

    assert result.url == "https://app.devin.ai/sessions/abc"


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
async def test_a_session_blocked_with_nothing_to_read_is_asked_for_its_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nudges: list[str] = []
    answer = '```json\n{"reproduced": false}\n```'

    result = await _run(
        monkeypatch,
        {"status_enum": "blocked", "structured_output": None},
        {"status_enum": "blocked", "structured_output": None},
        {
            "status_enum": "blocked",
            "structured_output": None,
            "messages": [{"message": answer}],
        },
        messages=nudges,
        nudge_interval=0.0,
    )

    assert len(nudges) == 1
    assert "json" in nudges[0]
    assert result.structured_output == {"reproduced": False, "diff": ""}


@pytest.mark.asyncio
async def test_a_session_that_stays_blocked_fails_instead_of_holding_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nudges: list[str] = []

    with pytest.raises(DevinError, match="stayed blocked"):
        await _run(
            monkeypatch,
            {"status_enum": "blocked", "structured_output": None},
            messages=nudges,
            nudge_interval=0.0,
            max_nudges=2,
            timeout=30.0,
        )

    assert len(nudges) == 2


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


@pytest.mark.asyncio
async def test_a_patch_rendered_as_its_own_block_is_still_the_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answer = (
        '```json\n{"reproduced": true, "diff": ""}\n```\n'
        "Diff:\n```diff\n--- a/x.py\n+++ b/x.py\n```"
    )
    result = await _run(
        monkeypatch,
        {
            "status_enum": "blocked",
            "structured_output": None,
            "messages": [{"message": answer}],
        },
    )

    assert result.structured_output["diff"].startswith("--- a/x.py")


@pytest.mark.asyncio
async def test_a_description_of_a_patch_is_not_taken_for_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    answer = '```json\n{"reproduced": true, "diff": "see attached fix.diff"}\n```'
    result = await _run(
        monkeypatch,
        {
            "status_enum": "blocked",
            "structured_output": None,
            "messages": [{"message": answer}],
        },
    )

    assert result.structured_output["diff"] == ""


# ------------------------------------------------- creating and polling


def test_the_client_refuses_to_exist_without_a_key() -> None:
    with pytest.raises(DevinError, match="DEVIN_API_KEY"):
        HttpDevinClient(api_key="")


@pytest.mark.asyncio
async def test_a_rejected_creation_is_reported_with_its_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(DevinError, match="session creation failed \\(402\\)"):
        await _run(
            monkeypatch,
            {"status_enum": "finished", "structured_output": {"ok": True}},
            created=httpx.Response(402, text="out of credits"),
        )


@pytest.mark.asyncio
async def test_the_session_is_created_with_the_title_tags_and_playbook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[httpx.Request] = []

    await _run(
        monkeypatch,
        {"status_enum": "finished", "structured_output": {"ok": True}},
        requests=seen,
        playbook_id="playbook-1",
        session_kwargs={"title": "triage", "tags": ["a"], "idempotent": False},
    )

    body = json.loads(seen[0].read())
    assert body == {
        "prompt": "prompt",
        "idempotent": False,
        "title": "triage",
        "tags": ["a"],
        "playbook_id": "playbook-1",
    }
    assert seen[0].headers["Authorization"] == "Bearer k"


@pytest.mark.asyncio
async def test_a_failing_poll_is_retried_rather_than_taken_for_an_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _run(
        monkeypatch,
        503,  # type: ignore[arg-type]
        {"status_enum": "finished", "structured_output": {"ok": True}},
    )

    assert result.structured_output == {"ok": True}


@pytest.mark.asyncio
async def test_a_session_that_never_finishes_gives_the_worker_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(DevinError, match="timed out"):
        await _run(
            monkeypatch,
            {"status_enum": "running", "structured_output": None},
            timeout=-1.0,
        )


@pytest.mark.asyncio
async def test_a_session_that_unblocks_itself_is_not_nudged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nudges: list[str] = []

    result = await _run(
        monkeypatch,
        {"status_enum": "blocked", "structured_output": None},
        {"status_enum": "running", "structured_output": None},
        {"status_enum": "blocked", "structured_output": None},
        {"status_enum": "finished", "structured_output": {"ok": True}},
        messages=nudges,
        nudge_interval=30.0,
    )

    assert nudges == []
    assert result.succeeded is True


@pytest.mark.asyncio
async def test_a_nudge_that_cannot_be_delivered_does_not_kill_the_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = iter(
        [
            httpx.Response(429, json={"error": "slow down"}),
            httpx.Response(200),
        ]
    )

    def respond() -> httpx.Response:
        try:
            return next(attempts)
        except StopIteration:
            raise httpx.ConnectError("no route") from None

    with pytest.raises(DevinError, match="stayed blocked"):
        await _run(
            monkeypatch,
            {"status_enum": "blocked", "structured_output": None},
            nudge_response=respond,
            nudge_interval=0.0,
            max_nudges=3,
            timeout=30.0,
        )


# --------------------------------------------------- reading the answer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    ["no json here", "```json\n{not json}\n```", "```json\n[1, 2]\n```"],
)
async def test_a_message_that_is_not_a_json_object_is_no_answer(
    monkeypatch: pytest.MonkeyPatch, message: str
) -> None:
    with pytest.raises(DevinError, match="stayed blocked"):
        await _run(
            monkeypatch,
            {
                "status_enum": "blocked",
                "structured_output": None,
                "messages": [{"message": message}],
            },
            nudge_interval=0.0,
            max_nudges=1,
            timeout=30.0,
        )


@pytest.mark.asyncio
async def test_structured_output_is_preferred_over_the_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await _run(
        monkeypatch,
        {
            "status_enum": "finished",
            "structured_output": {"from": "api"},
            "messages": [{"message": '```json\n{"from": "prose"}\n```'}],
        },
    )

    assert result.structured_output == {"from": "api"}


def test_a_terminal_session_without_output_did_not_succeed() -> None:
    assert not DevinSessionResult("s", "u", "finished").succeeded
    assert not DevinSessionResult("s", "u", "expired", {"ok": True}).succeeded
    assert DevinSessionResult("s", "u", "blocked", {"ok": True}).succeeded


# ------------------------------------------------------ the scripted double


@pytest.mark.asyncio
async def test_the_scripted_client_records_prompts_and_numbers_its_sessions() -> None:
    client = ScriptedDevinClient(responder=lambda prompt: {"echo": prompt})

    first = await client.run_session("one")
    second = await client.run_session("two")

    assert client.prompts == ["one", "two"]
    assert (first.session_id, second.session_id) == ("scripted-1", "scripted-2")
    assert second.structured_output == {"echo": "two"}
    assert second.succeeded is True
