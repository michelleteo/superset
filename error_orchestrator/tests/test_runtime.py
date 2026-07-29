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

"""The demo's settings, and starting/resetting a run from the dashboard."""

from __future__ import annotations

from typing import Iterator

import pytest
from starlette.testclient import TestClient

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.demo import build_app
from error_orchestrator.models import ErrorPool
from error_orchestrator.runtime import DemoRuntime
from error_orchestrator.settings import DemoSettings, SettingsError


def _config(**overrides: object) -> OrchestratorConfig:
    return OrchestratorConfig(port=8099, **overrides)  # type: ignore[arg-type]


@pytest.fixture()
def client() -> Iterator[TestClient]:
    runtime = DemoRuntime(_config(), DemoSettings(rate=0.1), autostart=False)
    with TestClient(build_app(runtime)) as test_client:
        yield test_client


# ---------------------------------------------------------------- settings


def test_settings_from_the_panel_override_only_what_was_sent() -> None:
    updated = DemoSettings(rate=1.5, speed=2.0).merged({"rate": 4})

    assert updated.rate == 4
    assert updated.speed == 2.0


def test_reviewers_accept_a_comma_separated_string() -> None:
    updated = DemoSettings().merged({"reviewers": "ana, bo"})

    assert updated.reviewers == ("ana", "bo")


@pytest.mark.parametrize(
    "payload",
    [
        {"rate": 0},
        {"speed": -1},
        {"duplicate_rate": 1.4},
        {"remediation_workers": 0},
        {"risk_check_workers": 9, "remediation_workers": 2},
        {"live_devin_stages": ["deploy"]},
        {"rate": "fast"},
    ],
)
def test_settings_that_would_break_a_run_are_rejected(
    payload: dict[str, object],
) -> None:
    with pytest.raises(SettingsError):
        DemoSettings().merged(payload)


def test_settings_never_carry_the_devin_api_key() -> None:
    assert "devin_api_key" not in DemoSettings().as_dict()


# ----------------------------------------------------------------- runtime


def test_the_panel_reports_whether_real_devin_sessions_are_possible() -> None:
    without = DemoRuntime(_config(), autostart=False)
    with_key = DemoRuntime(_config(devin_api_key="secret"), autostart=False)

    assert without.live_devin_available is False
    assert with_key.live_devin_available is True
    assert "secret" not in str(with_key.status())


@pytest.mark.asyncio
async def test_applying_settings_rebuilds_the_run_with_new_worker_counts() -> None:
    runtime = DemoRuntime(_config(), DemoSettings(rate=0.1), autostart=False)
    first = runtime.orchestrator

    await runtime.apply(
        runtime.settings.merged({"remediation_workers": 1, "risk_check_workers": 1})
    )

    assert runtime.orchestrator is not first
    assert runtime.orchestrator.remediation_lane.concurrency == 1
    assert runtime.running is True
    await runtime.stop_tasks()


@pytest.mark.asyncio
async def test_live_devin_needs_a_key_in_the_environment() -> None:
    runtime = DemoRuntime(_config(), DemoSettings(rate=0.1), autostart=False)

    with pytest.raises(RuntimeError, match="DEVIN_API_KEY"):
        await runtime.apply(runtime.settings.merged({"live_devin": True}))


@pytest.mark.asyncio
async def test_reset_returns_to_a_clean_board_and_the_starting_settings() -> None:
    runtime = DemoRuntime(_config(), DemoSettings(rate=0.1), autostart=False)
    await runtime.apply(runtime.settings.merged({"rate": 5}))
    runtime.orchestrator.store.pools["p"] = ErrorPool(fingerprint="fp", title="t")

    await runtime.reset()

    assert runtime.settings.rate == 0.1
    assert list(runtime.orchestrator.store.all_pools()) == []
    assert runtime.running is False
    await runtime.stop_tasks()


# -------------------------------------------------------------- HTTP layer


def test_the_dashboard_serves_the_current_settings(client: TestClient) -> None:
    body = client.get("/api/settings").json()

    assert body["running"] is False
    assert body["settings"]["rate"] == 0.1
    assert body["live_devin_available"] is False


def test_start_applies_the_panel_settings(client: TestClient) -> None:
    body = client.post("/api/settings", json={"rate": 2, "triage_workers": 2}).json()

    assert body["running"] is True
    assert body["settings"]["rate"] == 2
    assert body["settings"]["triage_workers"] == 2


def test_start_rejects_settings_that_would_break_the_run(client: TestClient) -> None:
    response = client.post("/api/settings", json={"rate": -1})

    assert response.status_code == 400
    assert "rate" in response.json()["error"]


def test_start_refuses_live_devin_without_a_key(client: TestClient) -> None:
    response = client.post("/api/settings", json={"live_devin": True})

    assert response.status_code == 409
    assert "DEVIN_API_KEY" in response.json()["error"]


def test_reset_stops_the_run_and_restores_the_starting_settings(
    client: TestClient,
) -> None:
    client.post("/api/settings", json={"rate": 6})

    body = client.post("/api/reset").json()

    assert body["running"] is False
    assert body["settings"]["rate"] == 0.1


def test_the_ingest_surface_follows_the_rebuilt_run(client: TestClient) -> None:
    """A swapped orchestrator must still be the one the webhook writes to."""
    client.post("/api/settings", json={"rate": 1})
    payload = {
        "message": "ValueError: boom",
        "level": "ERROR",
        "module": "superset.views.core",
        "func": "get",
        "line": 10,
    }

    assert client.post("/webhook/errors", json=payload).json()["accepted"] == 1
    assert client.get("/stats").status_code == 200
