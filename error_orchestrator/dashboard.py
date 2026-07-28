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

"""Observability surface: one JSON snapshot endpoint plus a static page.

The dashboard is a poll-driven read model over live orchestrator state — it
holds no state of its own. ``/api/live`` returns everything a refresh needs in
one round trip, including only the activity entries newer than the sequence
number the caller already has.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Route

from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.review import ReviewError
from error_orchestrator.simulator import ErrorSimulator

STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML = STATIC_DIR / "dashboard.html"

#: Pools shown in the live table (highest priority first).
POOL_LIMIT = 60


@dataclass
class Dashboard:
    """Read model plus the few controls a demo operator needs."""

    orchestrator: Orchestrator
    simulator: ErrorSimulator | None = None

    # ------------------------------------------------------------- read side

    def snapshot(self, since: int = 0) -> dict[str, Any]:
        orchestrator = self.orchestrator
        pools = [
            self._pool_row(orchestrator.pool_view(pool))
            for pool in orchestrator.store.all_pools()
        ]
        pools.sort(key=lambda row: (row["state"] == "cleared", -row["priority"]))
        return {
            "now": time.time(),
            "stats": orchestrator.stats(),
            "pools": pools[:POOL_LIMIT],
            "review": {
                "stats": orchestrator.review_queue.stats(),
                "open": [
                    item.as_dict() for item in orchestrator.review_queue.open_items()
                ],
                "reviewers": orchestrator.review_queue.reviewers,
            },
            "activity": [
                event.as_dict() for event in orchestrator.activity.since(since)
            ],
            "last_seq": orchestrator.activity.last_seq,
            "simulator": self.simulator.status() if self.simulator else None,
        }

    @staticmethod
    def _pool_row(view: dict[str, Any]) -> dict[str, Any]:
        risk = view.get("risk") or {}
        human = view.get("human") or {}
        return {
            **view,
            "risk_tier": risk.get("tier"),
            "risk_reasons": risk.get("reasons", []),
            "assignee": human.get("assignee"),
            "cleared": bool(human) and not human.get("open", True),
            "resolution": human.get("resolution"),
        }

    # ------------------------------------------------------------ HTTP layer

    async def index(self, _: Request) -> Response:
        return FileResponse(INDEX_HTML, media_type="text/html")

    async def live(self, request: Request) -> Response:
        try:
            since = int(request.query_params.get("since", 0))
        except ValueError:
            since = 0
        return JSONResponse(self.snapshot(since))

    async def control_simulator(self, request: Request) -> Response:
        if self.simulator is None:
            return JSONResponse({"error": "no simulator"}, status_code=404)
        body = await _json_body(request)
        if "running" in body:
            self.simulator.running = bool(body["running"])
        if "rate" in body:
            try:
                self.simulator.config.rate = max(float(body["rate"]), 0.05)
            except (TypeError, ValueError):
                return JSONResponse({"error": "invalid rate"}, status_code=400)
        return JSONResponse(self.simulator.status())

    async def inject(self, request: Request) -> Response:
        if self.simulator is None:
            return JSONResponse({"error": "no simulator"}, status_code=404)
        body = await _json_body(request)
        count = int(body.get("count", 1) or 1)
        scenario = body.get("scenario")
        try:
            accepted = (
                await self.simulator.inject(str(scenario), count)
                if scenario
                else await self.simulator.emit(count)
            )
        except KeyError:
            return JSONResponse({"error": "unknown scenario"}, status_code=400)
        return JSONResponse({"accepted": accepted}, status_code=202)

    async def clear_pool(self, request: Request) -> Response:
        body = await _json_body(request)
        try:
            item = self.orchestrator.clear_review(
                request.path_params["pool_id"],
                by=body.get("by"),
                resolution=body.get("resolution"),
            )
        except ReviewError as error:
            return JSONResponse({"error": str(error)}, status_code=409)
        return JSONResponse(item.as_dict())

    async def assign_pool(self, request: Request) -> Response:
        body = await _json_body(request)
        pool = self.orchestrator.store.get(request.path_params["pool_id"])
        if pool is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        try:
            item = self.orchestrator.review_queue.assign(pool, to=body.get("to"))
        except ReviewError as error:
            return JSONResponse({"error": str(error)}, status_code=409)
        return JSONResponse(item.as_dict())

    def routes(self) -> list[Route]:
        return [
            Route("/", self.index, methods=["GET"]),
            Route("/api/live", self.live, methods=["GET"]),
            Route("/api/simulator", self.control_simulator, methods=["POST"]),
            Route("/api/inject", self.inject, methods=["POST"]),
            Route("/api/pools/{pool_id}/clear", self.clear_pool, methods=["POST"]),
            Route("/api/pools/{pool_id}/assign", self.assign_pool, methods=["POST"]),
        ]


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


__all__ = ["Dashboard"]
