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
from error_orchestrator.devin_client import DevinClient, HttpDevinClient
from error_orchestrator.ingest import create_app
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.review import AutoReviewer, DEFAULT_REVIEWERS, ReviewQueue
from error_orchestrator.simulator import (
    BudgetedDevinClient,
    DemoDevinClient,
    ErrorScenario,
    ErrorSimulator,
    LatencyProfile,
    SCENARIOS,
    SimulatorConfig,
    WebhookSink,
)

logger = logging.getLogger("error_orchestrator.demo")


def build_app(args: argparse.Namespace, config: OrchestratorConfig) -> Starlette:
    """Assemble orchestrator + simulator + reviewers + dashboard into one app."""
    # One list, shared by the simulator and the session double: when the
    # simulator invents a new category, the double still recognises it.
    scenarios: list[ErrorScenario] = list(SCENARIOS)
    simulated = DemoDevinClient(
        latency=LatencyProfile().scaled(1.0 / args.speed), scenarios=scenarios
    )

    devin: DevinClient = simulated
    if args.live_devin:
        live = HttpDevinClient(
            api_key=config.devin_api_key or "",
            api_base=config.devin_api_base,
            poll_interval=config.devin_poll_interval,
            timeout=config.devin_session_timeout,
        )
        devin = (
            live
            if args.live_devin_budget <= 0
            else BudgetedDevinClient(
                live,
                simulated,
                budget=args.live_devin_budget,
                stages=tuple(args.live_devin_stages or ()),
            )
        )

    review_queue = ReviewQueue(args.reviewers or DEFAULT_REVIEWERS)
    orchestrator = Orchestrator(config=config, devin=devin, review_queue=review_queue)

    sink = WebhookSink(
        f"http://127.0.0.1:{config.port}/webhook/errors", token=config.webhook_token
    )
    simulator = ErrorSimulator(
        sink,
        SimulatorConfig(
            rate=args.rate,
            duplicate_rate=args.duplicate_rate,
            variant_rate=args.variant_rate,
            seed=args.seed,
        ),
        scenarios=scenarios,
    )
    reviewer = AutoReviewer(
        review_queue,
        interval=args.review_interval,
        enabled=not args.no_auto_review,
        clear=orchestrator.clear_review,
        seed=args.seed,
    )
    dashboard = Dashboard(orchestrator, simulator)

    def start() -> None:
        simulator.start()
        reviewer.start()
        logger.info(
            "demo ready: dashboard on http://localhost:%s/ (%s errors/s, "
            "%s remediation workers)",
            config.port,
            args.rate,
            config.remediation_workers,
        )

    return create_app(
        orchestrator,
        extra_routes=dashboard.routes(),
        on_start=[start],
        on_stop=[simulator.stop, reviewer.stop],
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
    if args.speed <= 0:
        logger.error("--speed must be positive")
        return 2
    if args.live_devin and not config.devin_api_key:
        logger.error("--live-devin needs DEVIN_API_KEY")
        return 2

    app = build_app(args, config)
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
