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

"""Fixtures for the orchestrator test suite (no Superset app required)."""

from __future__ import annotations

import time
from typing import Any, Callable, Mapping

import pytest

from error_orchestrator.devin_client import ScriptedDevinClient
from error_orchestrator.models import ErrorEvent

TRACEBACK_TEMPLATE = """Traceback (most recent call last):
  File "/app/superset/views/core.py", line 412, in explore
    payload = self.get_payload(form_data)
  File "/app/superset/common/query_context.py", line 88, in get_payload
    return self.processor.get_df_payload(query_obj)
superset.exceptions.SupersetException: Dataset {dataset} does not exist \
(request {request_id} at {timestamp}, object at {address})
"""


def make_event(
    dataset: str = "sales",
    request_id: str = "4f2a1c3e-1b2d-4a5e-9c8f-0a1b2c3d4e5f",
    timestamp: str = "2026-07-27T10:11:12+00:00",
    address: str = "0x7f3c1a2b3c4d",
    user_id: str | None = "u1",
    traceback: str | None = None,
) -> ErrorEvent:
    """A realistic MCP webhook payload with instance-specific noise in it."""
    rendered = TRACEBACK_TEMPLATE.format(
        dataset=dataset, request_id=request_id, timestamp=timestamp, address=address
    )
    return ErrorEvent.from_webhook_payload(
        {
            "timestamp": time.time(),
            "level": "ERROR",
            "logger": "superset.views.core",
            "message": f"Dataset {dataset} does not exist (request {request_id})",
            "module": "core",
            "func": "explore",
            "line": 412,
            "traceback": rendered if traceback is None else traceback,
            "user_id": user_id,
        }
    )


@pytest.fixture
def event_factory() -> Callable[..., ErrorEvent]:
    return make_event


@pytest.fixture
def scripted_devin() -> Callable[[Callable[[str], Mapping[str, Any]]], Any]:
    def _build(responder: Callable[[str], Mapping[str, Any]]) -> ScriptedDevinClient:
        return ScriptedDevinClient(responder=responder)

    return _build
