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

"""The dry-run double: plausible output for every lane, no credentials."""

from __future__ import annotations

import pytest

from error_orchestrator.simulation import (
    classify_prompt,
    make_dry_run_client,
    RISKY_DIFF,
    SAFE_DIFF,
)

TRIAGE_PROMPT = "You are triaging a production error for the repo x/y"
REMEDIATE_PROMPT = "Reproduce and fix a production error in x/y"
RISK_PROMPT = "Review this diff before it merges"


@pytest.mark.parametrize(
    ("prompt", "stage"),
    [
        (TRIAGE_PROMPT, "triage"),
        (REMEDIATE_PROMPT, "remediate"),
        (RISK_PROMPT, "risk_check"),
    ],
)
def test_a_prompt_says_which_lane_it_came_from(prompt: str, stage: str) -> None:
    assert classify_prompt(prompt) == stage


@pytest.mark.asyncio
async def test_triage_always_opens_a_category() -> None:
    client = make_dry_run_client()

    output = (await client.run_session(TRIAGE_PROMPT)).structured_output

    assert output["action"] == "new_category"
    assert client.prompts == [TRIAGE_PROMPT]


@pytest.mark.asyncio
async def test_review_approves_so_the_board_keeps_moving() -> None:
    output = (await make_dry_run_client().run_session(RISK_PROMPT)).structured_output

    assert output == {"approved": True, "confidence": 0.9, "concerns": []}


@pytest.mark.asyncio
async def test_a_fix_that_reproduces_carries_a_diff_and_a_branch() -> None:
    client = make_dry_run_client(reproduce_rate=1.0, risky_rate=0.0)

    output = (await client.run_session(REMEDIATE_PROMPT)).structured_output

    assert output["reproduced"] is True
    assert output["diff"] == SAFE_DIFF
    assert output["branch"] == "devin/dry-run-fix"


@pytest.mark.asyncio
async def test_the_risky_rate_decides_which_diff_comes_back() -> None:
    client = make_dry_run_client(reproduce_rate=1.0, risky_rate=1.0)

    output = (await client.run_session(REMEDIATE_PROMPT)).structured_output

    assert output["diff"] == RISKY_DIFF


@pytest.mark.asyncio
async def test_a_run_that_never_reproduces_says_so_instead_of_faking_a_fix() -> None:
    client = make_dry_run_client(reproduce_rate=0.0)

    output = (await client.run_session(REMEDIATE_PROMPT)).structured_output

    assert output["reproduced"] is False
    assert "diff" not in output
