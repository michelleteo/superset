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

"""One-command demo: ``python -m error_orchestrator.demo``.

Starts the real orchestrator, a synthetic Superset error source that keeps
pushing errors at it over the real webhook, and a dashboard that reads live
orchestrator state. Nothing is mocked except the two external systems a laptop
does not have: Devin sessions and Superset itself.
"""

from __future__ import annotations

import argparse
import logging
import sys

import uvicorn
from starlette.applications import Starlette

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.dashboard import Dashboard
from error_orchestrator.ingest import create_app, Endpoints
from error_orchestrator.review import DEFAULT_REVIEWERS
from error_orchestrator.runtime import DemoRuntime
from error_orchestrator.settings import DemoSettings, SettingsError

logger = logging.getLogger("error_orchestrator.demo")


def settings_from_args(args: argparse.Namespace) -> DemoSettings:
    """The command line and the dashboard's setup panel produce the same thing."""
    return DemoSettings(
        rate=args.rate,
        duplicate_rate=args.duplicate_rate,
        variant_rate=args.variant_rate,
        seed=args.seed,
        speed=args.speed,
        triage_workers=args.triage_workers,
        remediation_workers=args.remediation_workers,
        risk_check_workers=args.risk_check_workers,
        reviewers=tuple(args.reviewers or DEFAULT_REVIEWERS),
        auto_review=not args.no_auto_review,
        review_interval=args.review_interval,
        live_devin=args.live_devin,
        live_devin_budget=args.live_devin_budget,
        live_devin_stages=tuple(args.live_devin_stages or ()),
        seeded_bugs=args.seeded_bugs,
    )


def build_app(runtime: DemoRuntime) -> Starlette:
    """Assemble the run plus its dashboard into one app.

    The endpoints and the dashboard are bound to the runtime rather than to one
    orchestrator, so Start and Reset can swap the run underneath them.
    """
    endpoints = Endpoints(runtime.orchestrator)
    dashboard = Dashboard(runtime.orchestrator, runtime.simulator, runtime=runtime)
    runtime.bind(endpoints, dashboard)

    def start() -> None:
        runtime.start_tasks()
        logger.info(
            "demo ready: dashboard on http://localhost:%s/ (%s)",
            runtime.config.port,
            "streaming" if runtime.running else "idle — press Start in the UI",
        )

    return create_app(
        runtime.orchestrator,
        extra_routes=dashboard.routes(),
        on_start=[start],
        on_stop=[runtime.stop_tasks],
        endpoints=endpoints,
        autostart=False,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="error_orchestrator.demo",
        description="Run the error orchestrator with a live error simulator "
        "and observability dashboard.",
    )
    parser.add_argument("--host", default=None, help="bind address")
    parser.add_argument("--port", type=int, default=None, help="bind port")
    parser.add_argument(
        "--rate", type=float, default=1.5, help="simulated errors per second"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="session speed multiplier; >1 makes simulated Devin sessions finish "
        "faster, so states change more visibly",
    )
    parser.add_argument("--duplicate-rate", type=float, default=0.55)
    parser.add_argument("--variant-rate", type=float, default=0.25)
    parser.add_argument("--triage-workers", type=int, default=None)
    parser.add_argument("--remediation-workers", type=int, default=None)
    parser.add_argument("--risk-check-workers", type=int, default=None)
    parser.add_argument(
        "--idle",
        action="store_true",
        help="boot without streaming, so the demo starts from the UI's Start button",
    )
    parser.add_argument(
        "--review-interval",
        type=float,
        default=12.0,
        help="seconds between simulated human clears of the terminal backlog",
    )
    parser.add_argument(
        "--no-auto-review",
        action="store_true",
        help="leave every terminal item for a real human to clear in the UI",
    )
    parser.add_argument(
        "--reviewers",
        nargs="*",
        default=None,
        help="names the terminal backlog is assigned to",
    )
    parser.add_argument(
        "--live-devin",
        action="store_true",
        help="use the real Devin API (real sessions, real diffs) instead of the "
        "simulated session client",
    )
    parser.add_argument(
        "--live-devin-budget",
        type=int,
        default=3,
        help="with --live-devin: how many real sessions to spend before falling "
        "back to simulated ones; 0 means no limit",
    )
    parser.add_argument(
        "--live-devin-stages",
        nargs="*",
        default=["remediate"],
        choices=["triage", "remediate", "risk_check"],
        help="with --live-devin: which lanes may spend a real session",
    )
    parser.add_argument(
        "--seeded-bugs",
        action="store_true",
        help="emit the repository's own seeded defects "
        "(error_orchestrator/seeded) instead of synthetic Superset errors, so a "
        "real Devin session can reproduce and patch them",
    )
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # The simulator posts every event over HTTP; one log line each is noise.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        config = OrchestratorConfig.from_env()
    except ValueError as error:
        logger.error("invalid configuration: %s", error)
        return 2
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port
    if args.live_devin and not config.devin_api_key:
        logger.error("--live-devin needs DEVIN_API_KEY")
        return 2
    for name in ("triage_workers", "remediation_workers", "risk_check_workers"):
        override = getattr(args, name)
        if override is not None:
            setattr(config, name, override)
        else:
            setattr(args, name, getattr(config, name))

    try:
        settings = settings_from_args(args)
    except SettingsError as error:
        logger.error("invalid settings: %s", error)
        return 2

    app = build_app(DemoRuntime(config, settings, autostart=not args.idle))
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=args.log_level.lower(),
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
