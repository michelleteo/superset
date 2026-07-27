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

"""The webhook handler is attached by both MCP entry points."""

import logging
from collections.abc import Iterator
from unittest.mock import patch

import pytest

from superset.mcp_service import __main__ as mcp_main
from superset.mcp_service.server import configure_logging

#: Loggers ``main()`` silences in stdio mode; restored so other tests are unaffected.
_MUTATED_LOGGERS = (
    "superset",
    "flask",
    "werkzeug",
    "sqlalchemy",
    "flask_appbuilder",
    "celery",
    "alembic",
)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    saved = [
        (
            logging.getLogger(name),
            logging.getLogger(name).level,
            list(logging.getLogger(name).handlers),
        )
        for name in _MUTATED_LOGGERS
    ]
    root = logging.getLogger()
    root_level, root_handlers = root.level, list(root.handlers)
    try:
        yield
    finally:
        for target, level, handlers in saved:
            target.setLevel(level)
            target.handlers = handlers
        root.setLevel(root_level)
        root.handlers = root_handlers


def test_configure_logging_attaches_webhook_handler() -> None:
    with patch("superset.mcp_service.server.attach_webhook_handler") as attach:
        configure_logging()

    attach.assert_called_once()


@pytest.mark.usefixtures("restore_logging")
def test_stdio_main_attaches_webhook_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FASTMCP_TRANSPORT", "stdio")
    with (
        patch("superset.mcp_service.__main__.attach_webhook_handler") as attach,
        patch("superset.mcp_service.flask_singleton.get_flask_app"),
        patch("superset.mcp_service.__main__.init_fastmcp_server"),
        patch("superset.mcp_service.__main__._add_default_middlewares"),
        patch("superset.mcp_service.__main__.mcp"),
    ):
        mcp_main.main()

    attach.assert_called_once()
