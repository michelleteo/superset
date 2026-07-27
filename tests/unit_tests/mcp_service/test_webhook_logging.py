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

"""Unit tests for the MCP error webhook log handler."""

import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from superset.mcp_service import webhook_logging
from superset.mcp_service.webhook_logging import (
    attach_webhook_handler,
    build_webhook_handler,
    get_webhook_config,
    WebhookLogHandler,
)

WEBHOOK_URL = "https://hooks.example.com/mcp"


def _record(
    level: int = logging.ERROR,
    msg: str = "boom",
    args: Any = None,
    exc_info: Any = None,
) -> logging.LogRecord:
    return logging.LogRecord(
        name="superset.mcp_service.middleware",
        level=level,
        pathname="/app/superset/mcp_service/middleware.py",
        lineno=42,
        msg=msg,
        args=args,
        exc_info=exc_info,
        func="on_call_tool",
    )


@pytest.fixture
def posted() -> Any:
    """Capture payloads submitted to the async poster."""
    calls: list[dict[str, Any]] = []

    def fake_submit(url, payload, timeout, headers=None, on_error=None):  # noqa: ANN001
        calls.append(
            {"url": url, "payload": payload, "timeout": timeout, "headers": headers}
        )
        return True

    with patch.object(webhook_logging._POSTER, "submit", side_effect=fake_submit):
        yield calls


def test_emit_posts_payload_for_warning_and_error(posted: list[dict[str, Any]]) -> None:
    handler = WebhookLogHandler(WEBHOOK_URL, extra_headers={"X-Token": "abc"})

    handler.emit(_record(level=logging.WARNING, msg="user error %s", args=("x",)))
    handler.emit(_record(level=logging.ERROR, msg="system error"))

    assert [c["payload"]["level"] for c in posted] == ["WARNING", "ERROR"]
    payload = posted[0]["payload"]
    assert posted[0]["url"] == WEBHOOK_URL
    assert posted[0]["headers"] == {"X-Token": "abc"}
    assert payload["message"] == "user error x"
    assert payload["logger"] == "superset.mcp_service.middleware"
    assert payload["module"] == "middleware"
    assert payload["func"] == "on_call_tool"
    assert payload["line"] == 42
    assert isinstance(payload["timestamp"], float)
    assert "traceback" not in payload


def test_emit_includes_traceback_when_exc_info_set(
    posted: list[dict[str, Any]],
) -> None:
    handler = WebhookLogHandler(WEBHOOK_URL)
    try:
        raise ValueError("kaboom")
    except ValueError:
        import sys

        handler.emit(_record(exc_info=sys.exc_info()))

    assert "ValueError: kaboom" in posted[0]["payload"]["traceback"]


def test_emit_redacts_secrets(posted: list[dict[str, Any]]) -> None:
    handler = WebhookLogHandler(WEBHOOK_URL)
    handler.emit(
        _record(
            msg="failed for admin@example.com using "
            "postgresql://user:pw@db.internal/superset password=hunter2"
        )
    )

    message = posted[0]["payload"]["message"]
    assert "admin@example.com" not in message
    assert "hunter2" not in message
    assert "user:pw" not in message


def test_emit_never_raises_when_post_fails() -> None:
    handler = WebhookLogHandler(WEBHOOK_URL)
    handler.handleError = MagicMock()  # type: ignore[method-assign]

    with patch.object(
        webhook_logging._POSTER, "submit", side_effect=RuntimeError("network down")
    ):
        handler.emit(_record())

    # send_webhook_async swallows the failure, so emit is a silent no-op
    assert handler.handleError.call_count == 0


def test_emit_calls_handle_error_when_payload_build_fails() -> None:
    handler = WebhookLogHandler(WEBHOOK_URL)
    handler.handleError = MagicMock()  # type: ignore[method-assign]

    with patch.object(handler, "build_payload", side_effect=RuntimeError("bad record")):
        handler.emit(_record())

    handler.handleError.assert_called_once()


def test_poster_swallows_http_errors() -> None:
    poster = webhook_logging._AsyncPoster()
    on_error = MagicMock()
    with patch.object(
        webhook_logging._AsyncPoster, "_post", side_effect=RuntimeError("boom")
    ):
        poster.submit(WEBHOOK_URL, {"a": 1}, 1, None, on_error)
        poster.flush(timeout=5)
    on_error.assert_called_once()


def test_no_url_configured_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MCP_ERROR_WEBHOOK_URL", raising=False)
    flask_app = MagicMock()
    flask_app.config.get.return_value = None

    assert get_webhook_config(flask_app) is None
    assert build_webhook_handler(flask_app) is None
    assert attach_webhook_handler(flask_app) is None


def test_config_read_from_flask_then_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_MIN_LEVEL", "error")
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_TIMEOUT", "7")
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_HEADERS", '{"X-Token": "abc"}')

    config = get_webhook_config()

    assert config == {
        "url": WEBHOOK_URL,
        "min_level": logging.ERROR,
        "timeout": 7.0,
        "headers": {"X-Token": "abc"},
    }


def test_attach_webhook_handler_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MCP_ERROR_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.delenv("MCP_ERROR_WEBHOOK_MIN_LEVEL", raising=False)
    names = ("test_webhook_logger_a", "test_webhook_logger_b")
    try:
        handler = attach_webhook_handler(logger_names=names)
        assert handler is not None
        attach_webhook_handler(logger_names=names)

        for name in names:
            target = logging.getLogger(name)
            handlers = [h for h in target.handlers if isinstance(h, WebhookLogHandler)]
            assert len(handlers) == 1
            assert handlers[0].level == logging.WARNING
    finally:
        for name in names:
            logging.getLogger(name).handlers = []
