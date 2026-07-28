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

"""HTTP front door: receives error payloads and hands them to the orchestrator.

The payload schema is the one emitted by
``superset.mcp_service.webhook_logging.WebhookLogHandler``, so pointing
``MCP_ERROR_WEBHOOK_URL`` at ``/webhook/errors`` is all the wiring required.
Ingest never blocks the caller: events go onto a bounded queue and the
response returns immediately.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from error_orchestrator.models import ErrorEvent
from error_orchestrator.orchestrator import Orchestrator

logger = logging.getLogger(__name__)

_IGNORED_LEVELS = frozenset({"DEBUG", "INFO"})


def payload_to_events(payload: Any) -> list[ErrorEvent]:
    """Accept a single webhook payload or a batch, and skip non-error noise."""
    records = payload if isinstance(payload, list) else [payload]
    events: list[ErrorEvent] = []
    for record in records:
        if not isinstance(record, dict) or not record.get("message"):
            continue
        if str(record.get("level", "ERROR")).upper() in _IGNORED_LEVELS:
            continue
        try:
            events.append(ErrorEvent.from_webhook_payload(record))
        except Exception:  # noqa: BLE001 - one bad record must not drop a batch
            logger.warning("dropping unparseable webhook record", exc_info=True)
    return events


@dataclass
class Endpoints:
    """HTTP surface over an orchestrator; also usable without Starlette."""

    orchestrator: Orchestrator

    def _unauthorized(self, request: Request) -> Response | None:
        expected = self.orchestrator.config.webhook_token
        if not expected or request.headers.get("X-Webhook-Token") == expected:
            return None
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    async def receive_errors(self, request: Request) -> Response:
        denied = self._unauthorized(request)
        if denied is not None:
            return denied
        try:
            payload = await request.json()
        except ValueError:
            return JSONResponse({"error": "invalid json"}, status_code=400)
        events = payload_to_events(payload)
        accepted = sum(1 for event in events if self.orchestrator.submit(event))
        return JSONResponse(
            {"accepted": accepted, "received": len(events)}, status_code=202
        )

    async def healthz(self, _: Request) -> Response:
        return JSONResponse({"status": "ok"})

    async def stats(self, _: Request) -> Response:
        return JSONResponse(self.orchestrator.stats())

    async def list_pools(self, request: Request) -> Response:
        state = request.query_params.get("state")
        pools = [
            self.orchestrator.pool_view(pool)
            for pool in self.orchestrator.store.all_pools()
            if state is None or pool.state.value == state
        ]
        pools.sort(key=lambda view: view["priority"], reverse=True)
        return JSONResponse({"pools": pools})

    async def get_pool(self, request: Request) -> Response:
        pool = self.orchestrator.store.get(request.path_params["pool_id"])
        if pool is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        view = self.orchestrator.pool_view(pool)
        view["history"] = [
            {
                "from": item.from_state.value if item.from_state else None,
                "to": item.to_state.value,
                "at": item.at,
                "reason": item.reason,
            }
            for item in pool.history
        ]
        if pool.proposed_fix is not None:
            view["diff"] = pool.proposed_fix.diff
        return JSONResponse(view)


def create_app(orchestrator: Orchestrator) -> Starlette:
    """Build the Starlette app wired to a (not yet started) orchestrator."""
    endpoints = Endpoints(orchestrator)

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.stop()

    return Starlette(
        routes=[
            Route("/webhook/errors", endpoints.receive_errors, methods=["POST"]),
            Route("/healthz", endpoints.healthz, methods=["GET"]),
            Route("/stats", endpoints.stats, methods=["GET"]),
            Route("/pools", endpoints.list_pools, methods=["GET"]),
            Route("/pools/{pool_id}", endpoints.get_pool, methods=["GET"]),
        ],
        lifespan=lifespan,
    )
