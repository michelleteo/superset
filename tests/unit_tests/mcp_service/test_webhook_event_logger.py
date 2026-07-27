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

"""Unit tests for WebhookEventLogger."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from superset.mcp_service.webhook_event_logger import WebhookEventLogger

WEBHOOK_URL = "https://hooks.example.com/mcp"


@pytest.fixture(autouse=True)
def _webhook_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.delenv("MCP_ERROR_WEBHOOK_MIN_LEVEL", raising=False)
    monkeypatch.delenv("MCP_ERROR_WEBHOOK_TIMEOUT", raising=False)
    monkeypatch.delenv("MCP_ERROR_WEBHOOK_HEADERS", raising=False)


def _log(**kwargs: Any) -> tuple[MagicMock, MagicMock]:
    payload: dict[str, Any] = {
        "user_id": 1,
        "action": "mcp_tool",
        "dashboard_id": None,
        "duration_ms": 12,
        "slice_id": None,
        "referrer": None,
    }
    payload.update(kwargs)
    curated_payload = payload.pop("curated_payload", None)
    with (
        patch(
            "superset.mcp_service.webhook_event_logger.DBEventLogger.log"
        ) as super_log,
        patch("superset.mcp_service.webhook_event_logger.send_webhook_async") as send,
    ):
        WebhookEventLogger().log(**payload, curated_payload=curated_payload)
    return super_log, send


def test_log_calls_super_and_skips_non_error_events() -> None:
    super_log, send = _log(curated_payload={"success": True})

    super_log.assert_called_once()
    send.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"action": "mcp_tool_error"},
        {"curated_payload": {"severity": "error"}},
        {"curated_payload": {"success": False}},
    ],
)
def test_log_posts_for_error_events(kwargs: dict[str, Any]) -> None:
    super_log, send = _log(**kwargs)

    super_log.assert_called_once()
    send.assert_called_once()
    url, body = send.call_args.args
    assert url == WEBHOOK_URL
    assert body["action"] == kwargs.get("action", "mcp_tool")
    assert body["user_id"] == 1
    assert body["duration_ms"] == 12
    assert body["curated_payload"] == kwargs.get("curated_payload")


def test_log_never_raises_when_webhook_fails() -> None:
    with (
        patch("superset.mcp_service.webhook_event_logger.DBEventLogger.log"),
        patch(
            "superset.mcp_service.webhook_event_logger.send_webhook_async",
            side_effect=RuntimeError("network down"),
        ),
    ):
        WebhookEventLogger().log(
            user_id=1,
            action="mcp_tool_error",
            dashboard_id=None,
            duration_ms=1,
            slice_id=None,
            referrer=None,
        )
