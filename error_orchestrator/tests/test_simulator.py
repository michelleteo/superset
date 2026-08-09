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
from typing import Callable, Sequence

import httpx
import pytest

from error_orchestrator.fingerprint import canonicalize
from error_orchestrator.models import ErrorEvent
from error_orchestrator.simulator import (
    BudgetedDevinClient,
    DemoDevinClient,
    ErrorSimulator,
    LatencyProfile,
    SCENARIOS,
    SCENARIOS_BY_KEY,
    SimulatorConfig,
    WebhookSink,
)

TOKEN = "s3cret"  # noqa: S105

INSTANT = LatencyProfile(triage=(0, 0), remediate=(0, 0), risk_check=(0, 0))


class RecordingSink:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    async def __call__(self, payloads: Sequence[dict[str, object]]) -> int:
        self.payloads.extend(payloads)
        return len(payloads)


def _simulator(**overrides: float) -> tuple[ErrorSimulator, RecordingSink]:
    sink = RecordingSink()
    config = SimulatorConfig(seed=7, **overrides)
    return ErrorSimulator(sink, config), sink


def _fingerprint(payload: dict[str, object]) -> str:
    return canonicalize(ErrorEvent.from_webhook_payload(payload)).fingerprint


def test_payloads_match_the_webhook_schema() -> None:
    simulator, _ = _simulator()
    payload = simulator.next_payload()

    event = ErrorEvent.from_webhook_payload(payload)
    assert event.level == "ERROR"
    assert event.traceback is not None
    assert "Traceback (most recent call last)" in event.traceback
    assert event.module.startswith("superset")


def test_repeating_a_scenario_reuses_its_fingerprint() -> None:
    """Exact repeats must dedup in O(1) instead of spending a triage session."""
    simulator, _ = _simulator()
    scenario = SCENARIOS_BY_KEY["datasource_none"]

    first = simulator.build_payload(scenario)
    second = simulator.build_payload(scenario)

    assert _fingerprint(first) == _fingerprint(second)


def test_a_variant_is_the_same_bug_with_a_different_fingerprint() -> None:
    simulator, _ = _simulator()
    scenario = SCENARIOS_BY_KEY["datasource_none"]

    base = simulator.build_payload(scenario)
    variant = simulator.build_payload(scenario, scenario.variants[0])

    assert _fingerprint(base) != _fingerprint(variant)


def test_mutation_produces_a_distinct_category() -> None:
    simulator, _ = _simulator()
    base = SCENARIOS_BY_KEY["datasource_none"]

    mutated = simulator.mutate(base)

    assert mutated.is_mutation
    assert mutated.key != base.key
    assert mutated in simulator.scenarios
    assert _fingerprint(simulator.build_payload(mutated)) != _fingerprint(
        simulator.build_payload(base)
    )


def test_mutations_do_not_collide_with_each_other() -> None:
    """``_path4`` must not be mistaken for a prefix of ``_path44``."""
    simulator, _ = _simulator()
    base = SCENARIOS_BY_KEY["datasource_none"]
    mutations = [simulator.mutate(base) for _ in range(45)]

    tokens = [mutation.match_token() for mutation in mutations]
    assert len(set(tokens)) == len(tokens)
    for mutation in mutations:
        others = [
            other
            for other in mutations
            if other is not mutation and other.match_token() in mutation.match_token()
        ]
        assert not others


def test_the_stream_keeps_producing_new_categories() -> None:
    simulator, _ = _simulator(duplicate_rate=0.0, variant_rate=0.0)

    fingerprints = {_fingerprint(simulator.next_payload()) for _ in range(40)}

    assert len(fingerprints) > len(SCENARIOS)


@pytest.mark.asyncio
async def test_inject_emits_a_named_scenario() -> None:
    simulator, sink = _simulator()

    accepted = await simulator.inject("redis_timeout", count=3)

    assert accepted == 3
    assert {payload["scenario"] for payload in sink.payloads} == {"redis_timeout"}


@pytest.mark.asyncio
async def test_inject_rejects_an_unknown_scenario() -> None:
    simulator, _ = _simulator()
    with pytest.raises(KeyError):
        await simulator.inject("not-a-scenario")


@pytest.mark.asyncio
async def test_a_paused_simulator_emits_nothing_on_tick() -> None:
    simulator, sink = _simulator()
    simulator.running = False
    simulator.config.rate = 1000.0

    simulator.start()
    try:
        assert not sink.payloads
    finally:
        await simulator.stop()


def test_a_simulator_without_scenarios_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one scenario"):
        ErrorSimulator(RecordingSink(), SimulatorConfig(), scenarios=[])


@pytest.mark.asyncio
async def test_a_running_simulator_emits_and_reports_what_it_has_sent() -> None:
    simulator, sink = _simulator(rate=200.0)

    simulator.start()
    try:
        deadline = asyncio.get_running_loop().time() + 2.0
        while not sink.payloads and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
    finally:
        await simulator.stop()

    status = simulator.status()
    assert sink.payloads
    assert status["emitted"] == len(sink.payloads)
    assert status["running"] is True


@pytest.mark.asyncio
async def test_a_sink_that_fails_does_not_kill_the_stream() -> None:
    class _BrokenSink:
        calls = 0

        async def __call__(self, payloads: Sequence[dict[str, object]]) -> int:
            type(self).calls += 1
            raise RuntimeError("orchestrator gone")

    sink = _BrokenSink()
    simulator = ErrorSimulator(sink, SimulatorConfig(rate=200.0, seed=7))

    simulator.start()
    try:
        deadline = asyncio.get_running_loop().time() + 2.0
        while _BrokenSink.calls < 2 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
    finally:
        await simulator.stop()

    assert _BrokenSink.calls >= 2
    assert simulator.emitted == 0


@pytest.mark.asyncio
async def test_stopping_a_simulator_that_never_started_is_harmless() -> None:
    simulator, _ = _simulator()
    await simulator.stop()

    assert simulator.emitted == 0


# ------------------------------------------------------- scripted Devin side


@pytest.mark.asyncio
async def test_demo_client_returns_a_scenario_specific_diff() -> None:
    client = DemoDevinClient(latency=INSTANT, seed=1)
    scenario = SCENARIOS_BY_KEY["migration_lock"]
    prompt = (
        "Reproduce and fix a production error in michelleteo/superset\n"
        f"Category: {scenario.title}\n{scenario.match_token()}"
    )

    result = await client.run_session(prompt)

    assert result.structured_output["reproduced"] is True
    assert "migrations" in result.structured_output["diff"]


@pytest.mark.asyncio
async def test_demo_client_merges_a_variant_into_its_existing_pool() -> None:
    client = DemoDevinClient(latency=INSTANT, seed=1)
    scenario = SCENARIOS_BY_KEY["datasource_none"]
    prompt = (
        "You are triaging a production error for the repo michelleteo/superset\n"
        f"{scenario.match_token()}\n"
        "Existing categories:\n"
        f"- pool_id=abc123 occurrences=4\n  title: {scenario.title}\n"
    )

    output = (await client.run_session(prompt)).structured_output

    assert output["action"] == "merge"
    assert output["pool_id"] == "abc123"


@pytest.mark.asyncio
async def test_demo_client_keeps_a_mutation_out_of_its_base_pool() -> None:
    """A mutated title contains its base's title; that must not force a merge."""
    scenarios = list(SCENARIOS)
    simulator = ErrorSimulator(RecordingSink(), SimulatorConfig(seed=2), scenarios)
    base = SCENARIOS_BY_KEY["datasource_none"]
    mutation = simulator.mutate(base)
    client = DemoDevinClient(latency=INSTANT, scenarios=scenarios, seed=1)
    prompt = (
        "You are triaging a production error for the repo michelleteo/superset\n"
        f"{mutation.match_token()}\n"
        "Existing categories:\n"
        f"- pool_id=abc123 occurrences=4\n  title: {base.title}\n"
    )

    output = (await client.run_session(prompt)).structured_output

    assert output["action"] == "new_category"


@pytest.mark.asyncio
async def test_a_simulated_session_does_not_pretend_to_have_a_page() -> None:
    devin = DemoDevinClient(latency=INSTANT, seed=1)

    result = await devin.run_session("triage this")

    assert not result.url.startswith("http")


# -------------------------------------------------------------- webhook sink


def _sink(handler: Callable[[httpx.Request], httpx.Response]) -> WebhookSink:
    sink = WebhookSink("http://orchestrator/webhook/errors", token=TOKEN)
    sink._client = httpx.AsyncClient(  # noqa: SLF001 - swapping the transport
        transport=httpx.MockTransport(handler)
    )
    return sink


@pytest.mark.asyncio
async def test_the_sink_posts_the_batch_with_its_token() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(202, json={"accepted": 2, "received": 2})

    accepted = await _sink(handle)([{"message": "a"}, {"message": "b"}])

    assert accepted == 2
    assert seen[0].headers["X-Webhook-Token"] == TOKEN


@pytest.mark.asyncio
async def test_an_empty_batch_is_never_posted() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise AssertionError("nothing should have been posted")

    assert await _sink(handle)([]) == 0


@pytest.mark.asyncio
async def test_an_unreachable_orchestrator_accepts_nothing_rather_than_raising() -> (
    None
):
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    assert await _sink(handle)([{"message": "a"}]) == 0


@pytest.mark.asyncio
async def test_a_rejected_batch_counts_as_nothing_accepted() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "unauthorized"})

    assert await _sink(handle)([{"message": "a"}]) == 0


# --------------------------------------------------------- the session budget


class _CountingClient:
    def __init__(self, label: str) -> None:
        self.label = label
        self.calls = 0

    async def run_session(self, prompt: str, **kwargs: object) -> str:
        self.calls += 1
        return self.label


TRIAGE_PROMPT = "You are triaging a production error for the repo x/y"
REMEDIATE_PROMPT = "Reproduce and fix a production error in x/y"


@pytest.mark.asyncio
async def test_only_the_selected_stages_may_spend_a_real_session() -> None:
    live, fallback = _CountingClient("live"), _CountingClient("simulated")
    client = BudgetedDevinClient(live, fallback, budget=0, stages=("remediate",))

    assert await client.run_session(TRIAGE_PROMPT) == "simulated"
    assert await client.run_session(REMEDIATE_PROMPT) == "live"
    assert (live.calls, fallback.calls) == (1, 1)


@pytest.mark.asyncio
async def test_the_run_falls_back_once_the_budget_is_spent() -> None:
    live, fallback = _CountingClient("live"), _CountingClient("simulated")
    client = BudgetedDevinClient(live, fallback, budget=1)

    assert await client.run_session(REMEDIATE_PROMPT) == "live"
    assert await client.run_session(REMEDIATE_PROMPT) == "simulated"
    assert client.live_status()["spent"] == 1


@pytest.mark.asyncio
async def test_an_unlimited_budget_never_falls_back() -> None:
    live, fallback = _CountingClient("live"), _CountingClient("simulated")
    client = BudgetedDevinClient(live, fallback, budget=0)

    for _ in range(5):
        assert await client.run_session(REMEDIATE_PROMPT) == "live"

    status = client.live_status()
    assert (status["unlimited"], status["spent"], status["stages"]) == (True, 5, [])
    assert fallback.calls == 0


@pytest.mark.asyncio
async def test_a_session_in_flight_is_visible_while_it_runs() -> None:
    release = asyncio.Event()

    class _Slow:
        async def run_session(self, prompt: str, **kwargs: object) -> str:
            await release.wait()
            return "live"

    client = BudgetedDevinClient(_Slow(), _CountingClient("simulated"), budget=1)
    task = asyncio.create_task(client.run_session(REMEDIATE_PROMPT))
    while not client.in_flight:
        await asyncio.sleep(0)

    in_flight = client.live_status()["in_flight"]
    assert [item["stage"] for item in in_flight] == ["remediate"]

    release.set()
    await task
    assert client.live_status()["in_flight"] == []


@pytest.mark.asyncio
async def test_a_failing_real_session_still_releases_its_slot() -> None:
    class _Broken:
        async def run_session(self, prompt: str, **kwargs: object) -> str:
            raise RuntimeError("api down")

    client = BudgetedDevinClient(_Broken(), _CountingClient("simulated"), budget=1)

    with pytest.raises(RuntimeError, match="api down"):
        await client.run_session(REMEDIATE_PROMPT)

    assert client.live_status()["in_flight"] == []
