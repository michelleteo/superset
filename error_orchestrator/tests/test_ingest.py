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

from __future__ import annotations

import math
import time

from starlette.testclient import TestClient

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.ingest import create_app, payload_to_events
from error_orchestrator.models import ErrorEvent
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.simulation import make_dry_run_client

WEBHOOK_TOKEN = "s3cret"  # noqa: S105

#: Exactly what ``WebhookLogHandler.build_payload`` emits.
MCP_PAYLOAD = {
    "timestamp": time.time(),
    "level": "ERROR",
    "logger": "superset.mcp_service.tools",
    "message": "Tool call failed: dataset not found",
    "module": "tools",
    "func": "get_dataset",
    "line": 128,
    "traceback": 'Traceback (most recent call last):\n  File "/app/superset/'
    'mcp_service/tools.py", line 128, in get_dataset\n    raise SupersetError'
    "(msg)\nsuperset.exceptions.SupersetError: dataset not found",
}


def test_webhook_payload_maps_onto_an_error_event() -> None:
    event = ErrorEvent.from_webhook_payload(MCP_PAYLOAD)
    assert event.logger == "superset.mcp_service.tools"
    assert event.line == 128
    assert event.traceback is not None


def test_batches_are_accepted_and_info_records_dropped() -> None:
    events = payload_to_events(
        [MCP_PAYLOAD, {**MCP_PAYLOAD, "level": "INFO"}, {"level": "ERROR"}, "junk"]
    )
    assert len(events) == 1


def test_malformed_payloads_never_raise() -> None:
    assert payload_to_events(None) == []
    assert payload_to_events({}) == []


def test_http_endpoints_accept_events_and_expose_state() -> None:
    orchestrator = Orchestrator(
        config=OrchestratorConfig(
            triage_workers=1,
            remediation_workers=1,
            risk_check_workers=1,
            webhook_token=WEBHOOK_TOKEN,
        ),
        devin=make_dry_run_client(),
    )
    with TestClient(create_app(orchestrator)) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.post("/webhook/errors", json=MCP_PAYLOAD).status_code == 401

        response = client.post(
            "/webhook/errors",
            json=[MCP_PAYLOAD, MCP_PAYLOAD],
            headers={"X-Webhook-Token": WEBHOOK_TOKEN},
        )
        assert response.status_code == 202
        assert response.json() == {"accepted": 2, "received": 2}

        deadline = time.time() + 5
        while time.time() < deadline and not orchestrator.store.pools:
            time.sleep(0.05)

        pools = client.get("/pools").json()["pools"]
        assert len(pools) == 1
        assert pools[0]["occurrences"] == 2
        detail = client.get(f"/pools/{pools[0]['pool_id']}").json()
        assert detail["history"][0]["to"] == "triaged"
        assert client.get("/pools/nope").status_code == 404
        assert client.get("/stats").json()["pools"]


def test_one_poisoned_record_does_not_drop_the_batch() -> None:
    events = payload_to_events(
        [
            MCP_PAYLOAD,
            {**MCP_PAYLOAD, "line": 1e400},
            {**MCP_PAYLOAD, "timestamp": "nan"},
            MCP_PAYLOAD,
        ]
    )
    assert len(events) == 4
    assert all(math.isfinite(event.timestamp) for event in events)


def test_pools_stay_serializable_after_a_non_finite_payload() -> None:
    orchestrator = Orchestrator(
        config=OrchestratorConfig(
            triage_workers=1, remediation_workers=1, risk_check_workers=1
        ),
        devin=make_dry_run_client(),
    )
    with TestClient(create_app(orchestrator)) as client:
        # A literal 1e400 is valid JSON text that decodes to float("inf").
        posted = client.post(
            "/webhook/errors",
            content=(
                '{"message": "boom", "level": "ERROR", "logger": "superset.tasks",'
                ' "module": "tasks", "func": "run", "line": 1e400,'
                ' "timestamp": 1e400}'
            ),
            headers={"Content-Type": "application/json"},
        )
        assert posted.json() == {"accepted": 1, "received": 1}

        deadline = time.time() + 5
        while time.time() < deadline and not orchestrator.store.pools:
            time.sleep(0.05)

        assert client.get("/pools").status_code == 200
        assert client.get("/stats").status_code == 200
        pool = client.get("/pools").json()["pools"][0]
        assert client.get(f"/pools/{pool['pool_id']}").status_code == 200
