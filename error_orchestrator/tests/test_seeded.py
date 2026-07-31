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

"""The seeded defects have to be real, or live mode has nothing to fix."""

from __future__ import annotations

import time

import pytest

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.runtime import DemoRuntime
from error_orchestrator.seeded.bugs import SEEDED_BUGS, seeded_scenarios, SeededBug
from error_orchestrator.seeded.reproduce import main
from error_orchestrator.settings import DemoSettings
from error_orchestrator.simulator import (
    BudgetedDevinClient,
    ErrorSimulator,
    SimulatorConfig,
)


@pytest.mark.parametrize("bug", SEEDED_BUGS, ids=lambda bug: bug.key)
def test_every_seeded_bug_still_fails_for_real(bug: SeededBug) -> None:
    assert main([bug.key]) == 1


def test_scenarios_carry_the_frames_the_code_actually_produced() -> None:
    scenario = next(s for s in seeded_scenarios() if s.key == "seeded_datasource_none")

    assert scenario.frames[-1].file == "error_orchestrator/seeded/app.py"
    assert scenario.exception.startswith("AttributeError")
    assert "python -m error_orchestrator.seeded.reproduce" in scenario.message


def test_superset_defects_report_frames_inside_superset_itself() -> None:
    scenario = next(
        s for s in seeded_scenarios() if s.key == "superset_country_symbol_none"
    )

    assert scenario.frames[0].file == "superset/examples/countries.py"
    assert scenario.frames[0].func == "get"
    assert scenario.exception.startswith("AttributeError")


def test_no_scenario_is_filed_against_python_internals() -> None:
    for scenario in seeded_scenarios():
        assert all(
            frame.file.startswith(("superset/", "error_orchestrator/"))
            for frame in scenario.frames
        ), scenario.key
        assert scenario.frames, scenario.key


def test_seeded_mode_swaps_the_synthetic_catalog_for_the_real_one() -> None:
    runtime = DemoRuntime(
        OrchestratorConfig(),
        DemoSettings(rate=0.1, seeded_bugs=True),
        autostart=False,
    )

    keys = {scenario.key for scenario in runtime.simulator.scenarios}

    assert keys == {bug.key for bug in SEEDED_BUGS}


def test_a_seeded_run_never_drifts_back_into_synthetic_categories() -> None:
    async def sink(payloads: object) -> int:
        return 0

    simulator = ErrorSimulator(
        sink,
        SimulatorConfig(rate=1, duplicate_rate=0.0, variant_rate=0.0, seed=1),
        scenarios=seeded_scenarios(),
    )

    for _ in range(50):
        simulator.next_payload()

    seeds = {bug.key for bug in SEEDED_BUGS}
    assert all(s.key.split("~")[0] in seeds for s in simulator.scenarios)


def test_a_real_session_in_flight_is_visible_to_the_dashboard() -> None:
    runtime = DemoRuntime(
        OrchestratorConfig(devin_api_key="k"),
        DemoSettings(rate=0.1, live_devin=True, live_devin_budget=2),
        autostart=False,
    )
    devin = runtime.orchestrator.devin
    assert isinstance(devin, BudgetedDevinClient)
    devin.in_flight["remediate:1"] = time.time()

    live = runtime.status()["live_sessions"]

    assert live == {
        "spent": 0,
        "budget": 2,
        "unlimited": False,
        "stages": ["remediate"],
        "in_flight": [{"stage": "remediate", "elapsed": pytest.approx(0, abs=1)}],
    }


def test_an_unlimited_budget_still_honours_the_selected_stages() -> None:
    runtime = DemoRuntime(
        OrchestratorConfig(devin_api_key="k"),
        DemoSettings(
            rate=0.1,
            live_devin=True,
            live_devin_budget=0,
            live_devin_stages=("remediate",),
        ),
        autostart=False,
    )
    devin = runtime.orchestrator.devin

    assert isinstance(devin, BudgetedDevinClient)
    assert devin._use_live("remediate") is True
    assert devin._use_live("triage") is False
