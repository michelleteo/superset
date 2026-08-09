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

"""How a container's environment turns into an :class:`OrchestratorConfig`."""

from __future__ import annotations

import pytest

from error_orchestrator.config import ENV_PREFIX, OrchestratorConfig

TOKEN = "s3cret"  # noqa: S105


def _env(**values: str) -> dict[str, str]:
    return {f"{ENV_PREFIX}{name}": value for name, value in values.items()}


def test_an_empty_environment_gives_the_documented_defaults() -> None:
    config = OrchestratorConfig.from_env({})

    assert config.repo == "michelleteo/superset"
    assert (config.triage_workers, config.remediation_workers) == (4, 3)
    assert config.risk_check_workers == 2
    assert config.port == 8088
    assert config.auto_merge_enabled is False
    assert config.webhook_token is None


def test_every_knob_is_read_from_the_prefixed_environment() -> None:
    config = OrchestratorConfig.from_env(
        _env(
            REPO="acme/app",
            BASE_BRANCH="main",
            TRIAGE_WORKERS="7",
            REMEDIATION_WORKERS="5",
            RISK_CHECK_WORKERS="5",
            INGEST_QUEUE_SIZE="12",
            MERGE_CANDIDATE_LIMIT="3",
            AUTO_MERGE_ENABLED="yes",
            DEVIN_API_BASE="https://example.test/v2",
            DEVIN_POLL_INTERVAL="0.5",
            DEVIN_SESSION_TIMEOUT="30",
            DEVIN_NUDGE_INTERVAL="1.5",
            DEVIN_MAX_NUDGES="4",
            HOST="127.0.0.1",
            PORT="9001",
            WEBHOOK_TOKEN=TOKEN,
        )
    )

    assert config.repo == "acme/app"
    assert config.base_branch == "main"
    assert (config.triage_workers, config.remediation_workers) == (7, 5)
    assert config.risk_check_workers == 5
    assert (config.ingest_queue_size, config.merge_candidate_limit) == (12, 3)
    assert config.auto_merge_enabled is True
    assert config.devin_api_base == "https://example.test/v2"
    assert (config.devin_poll_interval, config.devin_session_timeout) == (0.5, 30.0)
    assert (config.devin_nudge_interval, config.devin_max_nudges) == (1.5, 4)
    assert (config.host, config.port) == ("127.0.0.1", 9001)
    assert config.webhook_token == TOKEN


@pytest.mark.parametrize("raw", ["", "four", "3.5"])
def test_an_unparseable_number_falls_back_to_the_default(raw: str) -> None:
    config = OrchestratorConfig.from_env(_env(TRIAGE_WORKERS=raw, PORT=raw))

    assert config.triage_workers == 4
    assert config.port == 8088


@pytest.mark.parametrize("raw", ["", "later"])
def test_an_unparseable_float_falls_back_to_the_default(raw: str) -> None:
    config = OrchestratorConfig.from_env(_env(DEVIN_POLL_INTERVAL=raw))

    assert config.devin_poll_interval == 10.0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1", True), ("TRUE", True), (" on ", True), ("0", False), ("nope", False)],
)
def test_flags_accept_the_usual_spellings(raw: str, expected: bool) -> None:
    config = OrchestratorConfig.from_env(_env(AUTO_MERGE_ENABLED=raw))

    assert config.auto_merge_enabled is expected


def test_the_api_key_is_read_from_the_unprefixed_name_too() -> None:
    assert (
        OrchestratorConfig.from_env({"DEVIN_API_KEY": "bare"}).devin_api_key == "bare"
    )
    assert (
        OrchestratorConfig.from_env(_env(DEVIN_API_KEY="prefixed")).devin_api_key
        == "prefixed"
    )


def test_a_risk_lane_wider_than_remediation_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="risk_check_workers"):
        OrchestratorConfig(remediation_workers=1, risk_check_workers=2)

    with pytest.raises(ValueError, match="risk_check_workers"):
        OrchestratorConfig.from_env(
            _env(REMEDIATION_WORKERS="1", RISK_CHECK_WORKERS="2")
        )
