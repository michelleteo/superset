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

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Mapping

import pytest

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.devin_client import ScriptedDevinClient
from error_orchestrator.handlers.triage import TriageAction, TriageDecision
from error_orchestrator.models import ErrorPool, PoolState
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.review import ReviewError
from error_orchestrator.simulation import classify_prompt, RISKY_DIFF, SAFE_DIFF
from error_orchestrator.tests.conftest import make_event

pytestmark = pytest.mark.asyncio


def _config(**overrides: Any) -> OrchestratorConfig:
    defaults: dict[str, Any] = {
        "triage_workers": 2,
        "remediation_workers": 2,
        "risk_check_workers": 1,
    }
    defaults.update(overrides)
    return OrchestratorConfig(**defaults)


def _responder(
    diff: str = SAFE_DIFF,
    reproduced: bool = True,
    approved: bool = True,
    latency: float = 0.0,
) -> ScriptedDevinClient:
    def respond(prompt: str) -> Mapping[str, Any]:
        stage = classify_prompt(prompt)
        if stage == "triage":
            return {"action": "new_category", "title": "Explore blows up"}
        if stage == "remediate":
            if not reproduced:
                return {"reproduced": False, "reason": "not reproducible in CI"}
            return {
                "reproduced": True,
                "diff": diff,
                "summary": "guard the empty case",
                "test_added": True,
            }
        return {"approved": approved, "concerns": []}

    return ScriptedDevinClient(responder=respond, latency=latency)


async def _run(orchestrator: Orchestrator, events: int = 1, **kwargs: Any) -> None:
    orchestrator.start()
    try:
        for index in range(events):
            orchestrator.submit(make_event(user_id=f"u{index}", **kwargs))
        assert await orchestrator.drain(timeout=5.0)
    finally:
        await orchestrator.stop()


async def test_happy_path_reaches_auto_merged() -> None:
    merged: list[ErrorPool] = []

    async def on_merge(pool: ErrorPool, _: Any) -> None:
        merged.append(pool)

    orchestrator = Orchestrator(
        config=_config(), devin=_responder(), merge_callback=on_merge
    )
    await _run(orchestrator, events=3)

    pools = list(orchestrator.store.all_pools())
    assert len(pools) == 1  # three identical traces dedup into one category
    pool = pools[0]
    assert pool.occurrences == 3
    assert pool.state is PoolState.AUTO_MERGED
    assert merged == [pool]
    assert [item.to_state.value for item in pool.history] == [
        "triaged",
        "reproducing",
        "fix_proposed",
        "auto_merged",
    ]


async def test_risky_diff_stops_at_awaiting_review_without_a_review_session() -> None:
    devin = _responder(diff=RISKY_DIFF)
    orchestrator = Orchestrator(config=_config(), devin=devin)
    await _run(orchestrator)

    pool = next(iter(orchestrator.store.all_pools()))
    assert pool.state is PoolState.AWAITING_REVIEW
    assert pool.risk is not None
    assert "db_migration" in pool.risk.triggered_rules
    assert [classify_prompt(prompt) for prompt in devin.prompts] == ["remediate"]


async def test_unreproducible_error_ends_in_could_not_reproduce() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder(reproduced=False))
    await _run(orchestrator)

    pool = next(iter(orchestrator.store.all_pools()))
    assert pool.state is PoolState.COULD_NOT_REPRODUCE
    assert pool.claimed is False
    assert len(orchestrator.store.heap) == 0


async def test_rejected_review_flags_for_a_human() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder(approved=False))
    await _run(orchestrator)

    pool = next(iter(orchestrator.store.all_pools()))
    assert pool.state is PoolState.AWAITING_REVIEW
    assert pool.review_reason


async def test_identical_traces_spend_one_triage_session() -> None:
    devin = _responder()
    orchestrator = Orchestrator(config=_config(), devin=devin)
    orchestrator.start()
    try:
        for index in range(25):
            orchestrator.submit(make_event(user_id=f"u{index}"))
        assert await orchestrator.drain(timeout=5.0)
    finally:
        await orchestrator.stop()

    # First fingerprint has no merge candidates, so triage short-circuits and
    # only the remediation + review sessions are spent for 25 occurrences.
    assert [classify_prompt(prompt) for prompt in devin.prompts] == [
        "remediate",
        "risk_check",
    ]


async def test_remediation_lane_never_exceeds_its_worker_count() -> None:
    devin = _responder(latency=0.02)
    orchestrator = Orchestrator(config=_config(remediation_workers=2), devin=devin)
    orchestrator.start()
    try:
        for index in range(8):
            variant = (make_event().traceback or "").replace("412", str(400 + index))
            orchestrator.submit(make_event(user_id=f"u{index}", traceback=variant))
        assert await orchestrator.drain(timeout=10.0)
    finally:
        await orchestrator.stop()

    assert len(list(orchestrator.store.all_pools())) == 8
    assert orchestrator.remediation_lane.stats.max_in_flight <= 2
    assert orchestrator.risk_lane.stats.max_in_flight <= 1
    assert orchestrator.triage_lane.stats.max_in_flight <= 2


async def test_remediation_pulls_the_highest_priority_pool_first() -> None:
    order: list[str] = []
    devin = _responder(latency=0.01)
    orchestrator = Orchestrator(config=_config(remediation_workers=1), devin=devin)

    original = orchestrator.build_remediation_request

    def spy(pool: ErrorPool) -> Any:
        order.append(pool.title)
        return original(pool)

    orchestrator.build_remediation_request = spy  # type: ignore[method-assign]

    # Seed three categories directly so priorities are unambiguous.
    for index, occurrences in enumerate((1, 30, 5)):
        variant = (make_event().traceback or "").replace("412", str(500 + index))
        result = orchestrator.store.record_event(make_event(traceback=variant))
        pool = orchestrator.store.create_pool(
            result.fingerprint, result.canonical, title=f"pool-{occurrences}"
        )
        for user in range(occurrences):
            pool.add_event(make_event(user_id=f"u{index}-{user}", traceback=variant))
        orchestrator.store.reprioritize(pool)

    orchestrator.start()
    try:
        assert await orchestrator.drain(timeout=10.0)
    finally:
        await orchestrator.stop()

    assert order == ["pool-30", "pool-5", "pool-1"]


async def test_stats_expose_lane_and_pool_counters() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    await _run(orchestrator)
    stats = orchestrator.stats()
    assert stats["pools"][PoolState.AUTO_MERGED.value] == 1
    assert stats["lanes"]["remediate"]["processed"] == 1
    assert stats["queues"]["ingest"] == 0


async def test_full_ingest_queue_drops_instead_of_blocking() -> None:
    orchestrator = Orchestrator(config=_config(ingest_queue_size=1), devin=_responder())
    assert orchestrator.submit(make_event()) is True
    assert orchestrator.submit(make_event()) is False
    assert orchestrator.stats()["queues"]["dropped_events"] == 1


async def test_an_ingest_failure_does_not_stop_the_next_event() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    original = orchestrator.handle_event
    calls = 0

    async def flaky(event: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("store unavailable")
        return await original(event)

    orchestrator.handle_event = flaky  # type: ignore[method-assign]
    await _run(orchestrator, events=2)

    assert calls == 2
    assert len(list(orchestrator.store.all_pools())) == 1


async def test_triage_merges_a_variant_into_the_category_it_named() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    seed = orchestrator.store.record_event(make_event())
    pool = orchestrator.store.create_pool(seed.fingerprint, seed.canonical)
    variant = make_event(
        traceback=(make_event().traceback or "").replace("explore", "explore_json")
    )
    result = orchestrator.store.record_event(variant)

    merged = orchestrator.apply_triage(
        result,
        TriageDecision(
            action=TriageAction.MERGE,
            fingerprint=result.fingerprint,
            pool_id=pool.pool_id,
            summary="same root cause",
        ),
    )

    assert merged is pool
    assert result.fingerprint in pool.merged_fingerprints
    assert orchestrator.decisions[-1].detail["merged"] is True


@pytest.mark.parametrize(
    ("enabled", "expected"), [(False, "auto-merge disabled"), (True, "auto-merging")]
)
async def test_auto_merge_is_a_dry_run_unless_it_is_enabled(
    caplog: pytest.LogCaptureFixture, enabled: bool, expected: str
) -> None:
    orchestrator = Orchestrator(
        config=_config(auto_merge_enabled=enabled), devin=_responder()
    )
    with caplog.at_level(logging.INFO, logger="error_orchestrator.orchestrator"):
        await _run(orchestrator)

    pool = next(iter(orchestrator.store.all_pools()))
    assert pool.state is PoolState.AUTO_MERGED
    assert expected in caplog.text


async def test_a_human_can_revert_an_auto_merge_but_only_a_real_one() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    await _run(orchestrator)
    pool = next(iter(orchestrator.store.all_pools()))

    item = orchestrator.revert_merge(pool.pool_id, by="ana", reason="regression")

    assert pool.state is PoolState.AWAITING_REVIEW
    assert pool.review_reason == "regression"
    assert item.open

    with pytest.raises(ReviewError, match="not auto_merged"):
        orchestrator.revert_merge(pool.pool_id)
    with pytest.raises(ReviewError, match="unknown pool"):
        orchestrator.revert_merge("nope")


async def test_clearing_a_terminal_pool_is_recorded_for_the_dashboard() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    await _run(orchestrator)
    pool = next(iter(orchestrator.store.all_pools()))

    item = orchestrator.clear_review(pool.pool_id, by="bo", resolution="shipped")

    assert (item.cleared_by, item.resolution) == ("bo", "shipped")
    assert orchestrator.review_queue.stats()["open"] == 0


async def test_a_risk_check_for_a_pool_that_moved_on_is_skipped() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    seed = orchestrator.store.record_event(make_event())
    pool = orchestrator.store.create_pool(seed.fingerprint, seed.canonical)

    await orchestrator._run_risk_check("nope")  # noqa: SLF001 - internal lane step
    await orchestrator._run_risk_check(pool.pool_id)  # noqa: SLF001

    assert pool.state is PoolState.TRIAGED
    with pytest.raises(ValueError, match="no proposed fix"):
        orchestrator.build_risk_request(pool)


async def test_draining_gives_up_when_the_work_never_finishes() -> None:
    orchestrator = Orchestrator(config=_config(), devin=_responder())
    orchestrator.submit(make_event())

    # The lanes were never started, so the event cannot leave the queue.
    assert await orchestrator.drain(timeout=0.05) is False


@pytest.fixture(autouse=True)
async def _fail_fast() -> AsyncIterator[None]:
    """Surface unhandled task exceptions instead of hanging the suite."""
    loop = asyncio.get_running_loop()
    errors: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context))
    yield
    loop.set_exception_handler(previous)
    assert errors == []
