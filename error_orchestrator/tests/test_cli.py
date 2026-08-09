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

"""The two entry points: ``python -m error_orchestrator[.demo]``.

Both are covered without ever binding a socket: ``uvicorn.run`` is replaced by
a recorder, so a test sees exactly the app and the address the process would
have served.
"""

from __future__ import annotations

from typing import Any

import pytest
from starlette.routing import Route

from error_orchestrator import __main__ as cli, demo
from error_orchestrator.devin_client import ScriptedDevinClient
from error_orchestrator.runtime import DemoRuntime


@pytest.fixture(autouse=True)
def no_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neither entry point may pick up the developer's own Devin key."""
    monkeypatch.delenv("DEVIN_API_KEY", raising=False)
    for name in ("ERROR_ORCHESTRATOR_DEVIN_API_KEY", "ERROR_ORCHESTRATOR_PORT"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture()
def served(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Records what each module would have handed to uvicorn."""
    calls: list[dict[str, Any]] = []

    def run(app: Any, **kwargs: Any) -> None:
        calls.append({"app": app, **kwargs})

    monkeypatch.setattr(cli.uvicorn, "run", run)
    monkeypatch.setattr(demo.uvicorn, "run", run)
    return calls


# ------------------------------------------------------- the bare service


def test_the_service_refuses_to_start_without_a_key_or_dry_run(
    served: list[dict[str, Any]],
) -> None:
    assert cli.main([]) == 2
    assert served == []


def test_a_dry_run_needs_no_credentials_and_serves_the_webhook(
    served: list[dict[str, Any]],
) -> None:
    assert cli.main(["--dry-run", "--host", "127.0.0.1", "--port", "9099"]) == 0

    assert len(served) == 1
    assert (served[0]["host"], served[0]["port"]) == ("127.0.0.1", 9099)
    assert served[0]["log_level"] == "info"
    assert {route.path for route in served[0]["app"].routes} >= {
        "/webhook/errors",
        "/healthz",
        "/stats",
    }


def test_a_real_key_builds_the_real_client(
    monkeypatch: pytest.MonkeyPatch, served: list[dict[str, Any]]
) -> None:
    monkeypatch.setenv("DEVIN_API_KEY", "k")

    assert cli.main([]) == 0
    assert served[0]["port"] == 8088


def test_an_impossible_lane_configuration_stops_the_process(
    monkeypatch: pytest.MonkeyPatch, served: list[dict[str, Any]]
) -> None:
    monkeypatch.setenv("ERROR_ORCHESTRATOR_REMEDIATION_WORKERS", "1")
    monkeypatch.setenv("ERROR_ORCHESTRATOR_RISK_CHECK_WORKERS", "2")

    assert cli.main(["--dry-run"]) == 2
    assert served == []


def test_the_dry_run_flag_swaps_in_the_scripted_client() -> None:
    args = cli.parse_args(["--dry-run"])

    assert args.dry_run is True
    assert isinstance(cli.make_dry_run_client(), ScriptedDevinClient)


# ---------------------------------------------------------------- the demo


def test_the_demo_streams_by_default_and_idles_on_request(
    served: list[dict[str, Any]],
) -> None:
    assert demo.main(["--port", "8099", "--rate", "3", "--idle"]) == 0

    assert served[0]["port"] == 8099
    assert served[0]["access_log"] is False


def test_command_line_settings_reach_the_run(served: list[dict[str, Any]]) -> None:
    settings = demo.settings_from_args(
        demo.parse_args(
            [
                "--rate",
                "4",
                "--speed",
                "3",
                "--reviewers",
                "ana",
                "bo",
                "--no-auto-review",
                "--seeded-bugs",
                "--triage-workers",
                "4",
                "--remediation-workers",
                "3",
                "--risk-check-workers",
                "2",
            ]
        )
    )

    assert (settings.rate, settings.speed) == (4.0, 3.0)
    assert settings.reviewers == ("ana", "bo")
    assert settings.auto_review is False
    assert settings.seeded_bugs is True


def test_lane_widths_come_from_the_flag_then_the_environment(
    monkeypatch: pytest.MonkeyPatch, served: list[dict[str, Any]]
) -> None:
    monkeypatch.setenv("ERROR_ORCHESTRATOR_TRIAGE_WORKERS", "6")
    built: list[DemoRuntime] = []

    def record(*args: Any, **kwargs: Any) -> DemoRuntime:
        built.append(DemoRuntime(*args, **kwargs))
        return built[-1]

    monkeypatch.setattr(demo, "DemoRuntime", record)

    assert demo.parse_args([]).triage_workers is None
    assert demo.main(["--idle", "--remediation-workers", "5"]) == 0

    runtime = built[0]
    assert runtime.config.triage_workers == 6
    assert runtime.settings.triage_workers == 6
    assert runtime.settings.remediation_workers == 5
    assert runtime.running is False


def test_live_devin_without_a_key_stops_the_demo(served: list[dict[str, Any]]) -> None:
    assert demo.main(["--live-devin"]) == 2
    assert served == []


def test_settings_the_run_could_not_honour_stop_the_demo(
    served: list[dict[str, Any]],
) -> None:
    assert demo.main(["--rate", "0"]) == 2
    assert served == []


def test_an_impossible_lane_configuration_stops_the_demo(
    monkeypatch: pytest.MonkeyPatch, served: list[dict[str, Any]]
) -> None:
    monkeypatch.setenv("ERROR_ORCHESTRATOR_REMEDIATION_WORKERS", "1")
    monkeypatch.setenv("ERROR_ORCHESTRATOR_RISK_CHECK_WORKERS", "2")

    assert demo.main([]) == 2
    assert served == []


def test_the_dashboard_is_served_next_to_the_webhook() -> None:
    runtime = DemoRuntime(
        demo.OrchestratorConfig(port=8099), demo.DemoSettings(rate=0.1), autostart=False
    )
    routes = demo.build_app(runtime).routes
    paths = {route.path for route in routes if isinstance(route, Route)}

    assert "/webhook/errors" in paths
    assert "/api/settings" in paths
