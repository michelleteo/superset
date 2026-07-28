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

from typing import Sequence

import pytest

from error_orchestrator.fingerprint import canonicalize
from error_orchestrator.models import ErrorEvent
from error_orchestrator.simulator import (
    DemoDevinClient,
    ErrorSimulator,
    LatencyProfile,
    SCENARIOS,
    SCENARIOS_BY_KEY,
    SimulatorConfig,
)

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
