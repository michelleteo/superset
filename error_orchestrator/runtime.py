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

"""A restartable demo run: orchestrator + simulator + stand-in reviewers.

Worker counts, session speed and the Devin client cannot be changed on a
running orchestrator, so applying settings means building a new one. The
runtime owns that swap and re-points the HTTP surface at the new instance, so
Start and Reset in the browser do exactly what different Docker arguments
would have done, without restarting the container.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Protocol

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.devin_client import DevinClient, HttpDevinClient
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.review import AutoReviewer, ReviewQueue
from error_orchestrator.seeded.bugs import seeded_scenarios
from error_orchestrator.settings import DemoSettings
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

logger = logging.getLogger(__name__)


class Bindable(Protocol):
    """Anything holding a reference the runtime has to re-point on a swap."""

    orchestrator: Orchestrator


class DemoRuntime:
    """Builds, starts, stops and rebuilds one demo run."""

    def __init__(
        self,
        config: OrchestratorConfig,
        settings: DemoSettings | None = None,
        autostart: bool = True,
    ) -> None:
        self.base_config = config
        self.defaults = settings or DemoSettings()
        self.settings = self.defaults
        self.autostart = autostart
        self._bound: list[Bindable] = []
        self._build()

    # ------------------------------------------------------------ assembling

    def _config_for(self, settings: DemoSettings) -> OrchestratorConfig:
        return dataclasses.replace(
            self.base_config,
            triage_workers=settings.triage_workers,
            remediation_workers=settings.remediation_workers,
            risk_check_workers=settings.risk_check_workers,
        )

    def _devin_for(
        self, settings: DemoSettings, scenarios: list[ErrorScenario]
    ) -> DevinClient:
        simulated = DemoDevinClient(
            latency=LatencyProfile().scaled(1.0 / settings.speed), scenarios=scenarios
        )
        if not settings.live_devin:
            return simulated
        live = HttpDevinClient(
            api_key=self.config.devin_api_key or "",
            api_base=self.config.devin_api_base,
            poll_interval=self.config.devin_poll_interval,
            timeout=self.config.devin_session_timeout,
        )
        if settings.live_devin_budget <= 0:
            return live
        # Real sessions take minutes; the budget keeps the board moving while
        # still putting genuinely Devin-authored diffs on screen.
        return BudgetedDevinClient(
            live,
            simulated,
            budget=settings.live_devin_budget,
            stages=tuple(settings.live_devin_stages),
        )

    def _build(self) -> None:
        settings = self.settings
        self.config = self._config_for(settings)
        # One list, shared by the simulator and the session double: when the
        # simulator invents a category, the double still recognises it.
        scenarios: list[ErrorScenario] = (
            seeded_scenarios() if settings.seeded_bugs else list(SCENARIOS)
        )
        self.review_queue = ReviewQueue(settings.reviewers)
        self.orchestrator = Orchestrator(
            config=self.config,
            devin=self._devin_for(settings, scenarios),
            review_queue=self.review_queue,
        )
        self.simulator = ErrorSimulator(
            WebhookSink(
                f"http://127.0.0.1:{self.config.port}/webhook/errors",
                token=self.config.webhook_token,
            ),
            SimulatorConfig(
                rate=settings.rate,
                duplicate_rate=settings.duplicate_rate,
                variant_rate=settings.variant_rate,
                seed=settings.seed,
            ),
            scenarios=scenarios,
        )
        self.simulator.running = self.autostart
        self.reviewer = AutoReviewer(
            self.review_queue,
            interval=settings.review_interval,
            enabled=settings.auto_review,
            clear=self.orchestrator.clear_review,
            seed=settings.seed,
        )
        for target in self._bound:
            self._point(target)

    def _point(self, target: Bindable) -> None:
        target.orchestrator = self.orchestrator
        if hasattr(target, "simulator"):
            target.simulator = self.simulator

    def bind(self, *targets: Bindable) -> None:
        """Keep HTTP handlers pointing at the current run across rebuilds."""
        for target in targets:
            self._bound.append(target)
            self._point(target)

    # ------------------------------------------------------------- lifecycle

    @property
    def running(self) -> bool:
        return self.simulator.running

    @property
    def live_devin_available(self) -> bool:
        """Whether a real Devin key is present — the key itself never leaves."""
        return bool(self.base_config.devin_api_key)

    def start_tasks(self) -> None:
        """Start the long-lived tasks; safe to call again after a rebuild."""
        self.orchestrator.start()
        self.simulator.start()
        self.reviewer.start()

    async def stop_tasks(self) -> None:
        await self.simulator.stop()
        await self.reviewer.stop()
        await self.orchestrator.stop()

    async def apply(self, settings: DemoSettings, running: bool = True) -> None:
        """Rebuild the run under new settings and (optionally) start it."""
        if settings.live_devin and not self.live_devin_available:
            raise RuntimeError(
                "live Devin sessions need DEVIN_API_KEY in the server's environment"
            )
        await self.stop_tasks()
        self.settings = settings
        self.autostart = running
        self._build()
        self.start_tasks()
        logger.info(
            "demo run rebuilt: %s errors/s, %s remediation workers, live_devin=%s",
            settings.rate,
            settings.remediation_workers,
            settings.live_devin,
        )

    async def reset(self) -> None:
        """Back to a clean board with the settings the process started with."""
        await self.apply(self.defaults, running=False)

    def status(self) -> dict[str, object]:
        return {
            "running": self.running,
            "settings": self.settings.as_dict(),
            "defaults": self.defaults.as_dict(),
            "live_devin_available": self.live_devin_available,
            "repo": self.base_config.repo,
        }


__all__ = ["DemoRuntime"]
