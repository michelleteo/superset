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

import pytest

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.runtime import DemoRuntime
from error_orchestrator.seeded.bugs import SEEDED_BUGS, seeded_scenarios, SeededBug
from error_orchestrator.seeded.reproduce import main
from error_orchestrator.settings import DemoSettings


@pytest.mark.parametrize("bug", SEEDED_BUGS, ids=lambda bug: bug.key)
def test_every_seeded_bug_still_fails_for_real(bug: SeededBug) -> None:
    assert main([bug.key]) == 1


def test_scenarios_carry_the_frames_the_code_actually_produced() -> None:
    scenario = next(s for s in seeded_scenarios() if s.key == "seeded_datasource_none")

    assert scenario.frames[-1].file == "error_orchestrator/seeded/app.py"
    assert scenario.exception.startswith("AttributeError")
    assert "python -m error_orchestrator.seeded.reproduce" in scenario.message


def test_seeded_mode_swaps_the_synthetic_catalog_for_the_real_one() -> None:
    runtime = DemoRuntime(
        OrchestratorConfig(),
        DemoSettings(rate=0.1, seeded_bugs=True),
        autostart=False,
    )

    keys = {scenario.key for scenario in runtime.simulator.scenarios}

    assert keys == {bug.key for bug in SEEDED_BUGS}
