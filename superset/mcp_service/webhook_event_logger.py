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

"""Event logger that forwards structured MCP error events to a webhook.

Enable in ``superset_config.py``::

    from superset.mcp_service.webhook_event_logger import WebhookEventLogger

    EVENT_LOGGER = WebhookEventLogger()

It extends :class:`~superset.utils.log.DBEventLogger` so DB logging and the
Action Log UI keep working.
"""

from __future__ import annotations

import logging
from typing import Any

from flask import current_app, has_app_context

from superset.mcp_service.webhook_logging import get_webhook_config, send_webhook_async
from superset.utils.log import DBEventLogger

logger = logging.getLogger(__name__)

#: Actions always treated as errors regardless of payload contents.
ERROR_ACTIONS = frozenset({"mcp_tool_error"})


def _get_flask_app() -> Any:
    """Return the active Flask app, or ``None`` outside an app context."""
    return current_app if has_app_context() else None


def _is_error_event(action: str, curated_payload: Any) -> bool:
    if action in ERROR_ACTIONS:
        return True
    if isinstance(curated_payload, dict):
        if curated_payload.get("severity") == "error":
            return True
        if curated_payload.get("success") is False:
            return True
    return False


class WebhookEventLogger(DBEventLogger):
    """DB event logger that additionally POSTs error events to a webhook."""

    def log(  # pylint: disable=too-many-arguments
        self,
        user_id: int | None,
        action: str,
        dashboard_id: int | None,
        duration_ms: int | None,
        slice_id: int | None,
        referrer: str | None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().log(
            user_id,
            action,
            dashboard_id,
            duration_ms,
            slice_id,
            referrer,
            *args,
            **kwargs,
        )
        try:
            curated_payload = kwargs.get("curated_payload")
            if not _is_error_event(action, curated_payload):
                return
            config = get_webhook_config(_get_flask_app())
            if config is None:
                return
            send_webhook_async(
                config["url"],
                {
                    "action": action,
                    "user_id": user_id,
                    "duration_ms": duration_ms,
                    "curated_payload": curated_payload,
                },
                timeout=config["timeout"],
                headers=config["headers"],
            )
        except Exception:  # pylint: disable=broad-except
            # Logging must never break tool execution.
            logger.debug("WebhookEventLogger failed to forward event", exc_info=True)
