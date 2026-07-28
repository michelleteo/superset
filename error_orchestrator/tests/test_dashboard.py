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

from typing import Iterator, NamedTuple, Sequence

import pytest
from starlette.testclient import TestClient

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.dashboard import Dashboard
from error_orchestrator.ingest import create_app
from error_orchestrator.models import ErrorPool, PoolState
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.review import ReviewQueue
from error_orchestrator.simulator import (
    DemoDevinClient,
    ErrorSimulator,
    LatencyProfile,
    SimulatorConfig,
)

INSTANT = LatencyProfile(triage=(0, 0), remediate=(0, 0), risk_check=(0, 0))


class Harness(NamedTuple):
    client: TestClient
    orchestrator: Orchestrator
    simulator: ErrorSimulator


def _pool(
    harness: Harness, fingerprint: str, title: str, state: PoolState | None = None
) -> ErrorPool:
    """Put a pool straight into the store, skipping the lanes."""
    pool = ErrorPool(fingerprint=fingerprint, title=title)
    if state is not None:
        pool.state = state
    store = harness.orchestrator.store
    store.pools[pool.pool_id] = pool
    store.index[fingerprint] = pool.pool_id
    return pool


class RecordingSink:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    async def __call__(self, payloads: Sequence[dict[str, object]]) -> int:
        self.payloads.extend(payloads)
        return len(payloads)


@pytest.fixture(name="harness")
def harness_fixture() -> Iterator[Harness]:
    orchestrator = Orchestrator(
        config=OrchestratorConfig(
            triage_workers=2, remediation_workers=2, risk_check_workers=1
        ),
        devin=DemoDevinClient(latency=INSTANT, seed=1),
        review_queue=ReviewQueue(["ana", "bo"]),
    )
    simulator = ErrorSimulator(RecordingSink(), SimulatorConfig(seed=3))
    dashboard = Dashboard(orchestrator, simulator)
    app = create_app(orchestrator, extra_routes=dashboard.routes())
    with TestClient(app) as client:
        yield Harness(client, orchestrator, simulator)


def test_dashboard_page_is_served(harness: Harness) -> None:
    response = harness.client.get("/")
    assert response.status_code == 200
    assert "Error Orchestrator" in response.text


def test_live_snapshot_has_everything_the_ui_renders(harness: Harness) -> None:
    body = harness.client.get("/api/live").json()

    assert set(body) >= {
        "stats",
        "pools",
        "review",
        "activity",
        "last_seq",
        "simulator",
    }
    assert set(body["stats"]) >= {"queues", "lanes", "pools", "review", "throughput"}
    assert set(body["stats"]["lanes"]) == {"triage", "remediate", "risk_check"}
    assert body["stats"]["lanes"]["triage"]["capacity"] == 2


def test_activity_polling_is_incremental(harness: Harness) -> None:
    harness.orchestrator.activity.publish("triage", "first")
    first = harness.client.get("/api/live").json()
    harness.orchestrator.activity.publish("triage", "second")

    second = harness.client.get(f"/api/live?since={first['last_seq']}").json()

    assert [event["message"] for event in second["activity"]] == ["second"]


def test_a_bad_since_cursor_falls_back_to_the_whole_feed(harness: Harness) -> None:
    harness.orchestrator.activity.publish("triage", "hello")
    body = harness.client.get("/api/live?since=not-a-number").json()
    assert body["activity"]


def test_pausing_and_rate_changes_reach_the_simulator(harness: Harness) -> None:
    assert harness.client.post("/api/simulator", json={"running": False}).json()[
        "running"
    ] is (False)
    assert harness.simulator.running is False

    body = harness.client.post("/api/simulator", json={"rate": 4.5}).json()

    assert body["rate"] == 4.5


def test_the_rate_cannot_be_driven_to_zero(harness: Harness) -> None:
    body = harness.client.post("/api/simulator", json={"rate": 0}).json()
    assert body["rate"] > 0


def test_an_invalid_rate_is_rejected(harness: Harness) -> None:
    response = harness.client.post("/api/simulator", json={"rate": "fast"})
    assert response.status_code == 400


def test_injecting_a_scenario_emits_events(harness: Harness) -> None:
    response = harness.client.post(
        "/api/inject", json={"scenario": "redis_timeout", "count": 2}
    )

    assert response.status_code == 202
    assert response.json()["accepted"] == 2


def test_injecting_an_unknown_scenario_is_rejected(harness: Harness) -> None:
    response = harness.client.post("/api/inject", json={"scenario": "nope"})
    assert response.status_code == 400


def test_a_human_can_clear_a_terminal_pool(harness: Harness) -> None:
    orchestrator = harness.orchestrator
    pool = _pool(harness, "fp-1", "AttributeError: boom", PoolState.AWAITING_REVIEW)
    orchestrator.review_queue.assign(pool)

    body = harness.client.post(
        f"/api/pools/{pool.pool_id}/clear", json={"by": "ana"}
    ).json()

    assert body["open"] is False
    assert body["cleared_by"] == "ana"
    assert orchestrator.review_queue.stats()["open"] == 0


def test_clearing_a_pool_twice_conflicts(harness: Harness) -> None:
    orchestrator = harness.orchestrator
    pool = _pool(harness, "fp-2", "KeyError: boom", PoolState.AUTO_MERGED)
    orchestrator.review_queue.assign(pool)
    harness.client.post(f"/api/pools/{pool.pool_id}/clear", json={})

    assert (
        harness.client.post(f"/api/pools/{pool.pool_id}/clear", json={}).status_code
        == 409
    )


def test_a_cleared_pool_is_still_listed_but_marked_done(harness: Harness) -> None:
    orchestrator = harness.orchestrator
    pool = _pool(harness, "fp-3", "ValueError: boom", PoolState.COULD_NOT_REPRODUCE)
    orchestrator.review_queue.assign(pool)
    harness.client.post(f"/api/pools/{pool.pool_id}/clear", json={})

    row = next(
        row
        for row in harness.client.get("/api/live").json()["pools"]
        if row["pool_id"] == pool.pool_id
    )

    assert row["cleared"] is True
    assert row["resolution"]


def test_reassigning_moves_the_item_to_another_human(harness: Harness) -> None:
    orchestrator = harness.orchestrator
    pool = _pool(harness, "fp-4", "TimeoutError: boom", PoolState.AWAITING_REVIEW)
    orchestrator.review_queue.assign(pool, to="ana")

    body = harness.client.post(
        f"/api/pools/{pool.pool_id}/assign", json={"to": "bo"}
    ).json()

    assert body["assignee"] == "bo"


def test_assigning_a_non_terminal_pool_conflicts(harness: Harness) -> None:
    pool = _pool(harness, "fp-5", "RuntimeError: boom")

    response = harness.client.post(
        f"/api/pools/{pool.pool_id}/assign", json={"to": "bo"}
    )

    assert response.status_code == 409


def test_assigning_an_unknown_pool_is_a_404(harness: Harness) -> None:
    assert harness.client.post("/api/pools/missing/assign", json={}).status_code == 404
