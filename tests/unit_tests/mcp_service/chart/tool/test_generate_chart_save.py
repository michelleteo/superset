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

"""
Unit tests for the ``save_chart=True`` branch of the MCP ``generate_chart`` tool.

The tool is exercised end-to-end (through the ``@tool`` wrapper) with the
external seams mocked: the dataset/chart DAOs, ``CreateChartCommand``,
``_compile_chart``, ``validate_chart_dataset`` and the form-data cache command.
No metadata database or example data is required.
"""

from contextlib import ExitStack
from typing import Any, Iterator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from superset.commands.exceptions import CommandException
from superset.mcp_service.chart.compile import CompileResult
from superset.mcp_service.chart.schemas import (
    ColumnRef,
    GenerateChartRequest,
    TableChartConfig,
)
from superset.mcp_service.chart.tool.generate_chart import generate_chart
from superset.mcp_service.common.error_schemas import ChartGenerationError
from superset.utils import json

VIZ_TYPE = "table"
DATASET_UUID = "a1b2c3d4-5678-90ab-cdef-1234567890ab"


def _make_request(
    dataset_id: str | int = "1", chart_name: str | None = "My Chart"
) -> GenerateChartRequest:
    return GenerateChartRequest(
        dataset_id=dataset_id,
        config=TableChartConfig(
            chart_type="table",
            columns=[ColumnRef(name="region")],
        ),
        chart_name=chart_name,
        save_chart=True,
        generate_preview=False,
    )


def _make_ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.info = AsyncMock()
    ctx.debug = AsyncMock()
    ctx.warning = AsyncMock()
    ctx.error = AsyncMock()
    ctx.report_progress = AsyncMock()
    return ctx


def _make_dataset(dataset_id: int = 7) -> Mock:
    dataset = Mock()
    dataset.id = dataset_id
    dataset.table_name = "cleaned_sales_data"
    dataset.datasource_name = "cleaned_sales_data"
    return dataset


def _make_chart(chart_id: int | None = 42) -> Mock:
    """Build a chart mock with every attribute ``serialize_chart_object`` reads."""
    chart = Mock()
    chart.id = chart_id
    chart.slice_name = "My Chart"
    chart.viz_type = VIZ_TYPE
    chart.uuid = "chart-uuid"
    chart.datasource_name = "cleaned_sales_data"
    chart.datasource_type = "table"
    chart.description = None
    chart.certified_by = None
    chart.certification_details = None
    chart.cache_timeout = None
    chart.changed_by = None
    chart.changed_by_name = "admin"
    chart.changed_on = None
    chart.changed_on_humanized = "1 day ago"
    chart.created_by = None
    chart.created_by_name = "admin"
    chart.created_on = None
    chart.created_on_humanized = "2 days ago"
    chart.deleted_at = None
    chart.params = json.dumps(_form_data())
    chart.tags = []
    chart.editors = []
    return chart


def _form_data() -> dict[str, Any]:
    return {"viz_type": VIZ_TYPE, "groupby": ["region"], "row_limit": 100}


class _Seams:
    """Handles for the mocked seams used by the save_chart branch."""

    def __init__(self) -> None:
        self.dataset_dao_find = MagicMock(return_value=_make_dataset())
        self.has_dataset_access = MagicMock(return_value=True)
        self.command_cls = MagicMock()
        self.command_cls.return_value.run.return_value = _make_chart()
        self.compile_chart = MagicMock(return_value=CompileResult(success=True))
        self.validate_chart_dataset = MagicMock(
            return_value=Mock(is_valid=True, error=None, warnings=[])
        )
        self.chart_dao_find = MagicMock(side_effect=lambda chart_id, **_: None)
        self.chart_dao_delete = MagicMock()
        self.session = MagicMock()
        self.form_data_command = MagicMock()
        self.form_data_command.return_value.run.return_value = "form-data-key"
        self.generate_chart_name = MagicMock(return_value="Generated Name")


@pytest.fixture(name="seams")
def seams_fixture() -> Iterator[_Seams]:
    """Patch every external seam of the ``save_chart=True`` code path."""
    seams = _Seams()
    user = Mock(id=1, username="admin", roles=[], groups=[])
    module = "superset.mcp_service.chart.tool.generate_chart"

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "superset.mcp_service.auth.get_user_from_request",
                return_value=user,
            )
        )
        stack.enter_context(
            patch(
                "superset.daos.dataset.DatasetDAO.find_by_id",
                seams.dataset_dao_find,
            )
        )
        stack.enter_context(
            patch(f"{module}.has_dataset_access", seams.has_dataset_access)
        )
        stack.enter_context(
            patch(f"{module}.map_config_to_form_data", return_value=_form_data())
        )
        stack.enter_context(
            patch(f"{module}.generate_chart_name", seams.generate_chart_name)
        )
        stack.enter_context(patch(f"{module}._compile_chart", seams.compile_chart))
        stack.enter_context(
            patch(f"{module}.validate_chart_dataset", seams.validate_chart_dataset)
        )
        stack.enter_context(
            patch(
                "superset.commands.chart.create.CreateChartCommand",
                seams.command_cls,
            )
        )
        stack.enter_context(
            patch("superset.daos.chart.ChartDAO.find_by_id", seams.chart_dao_find)
        )
        stack.enter_context(
            patch("superset.daos.chart.ChartDAO.delete", seams.chart_dao_delete)
        )
        stack.enter_context(patch("superset.db.session", seams.session))
        stack.enter_context(
            patch(
                "superset.mcp_service.commands.create_form_data."
                "MCPCreateFormDataCommand",
                seams.form_data_command,
            )
        )
        stack.enter_context(
            patch(
                "superset.mcp_service.chart.validation.ValidationPipeline."
                "validate_request_with_warnings",
                # request=None keeps the caller's request object in play
                return_value=Mock(is_valid=True, request=None, warnings={}, error=None),
            )
        )
        yield seams


@pytest.mark.asyncio
async def test_dataset_lookup_by_numeric_id(seams: _Seams) -> None:
    """A numeric dataset_id is resolved through ``DatasetDAO.find_by_id(id)``."""
    result = await generate_chart(_make_request(dataset_id="1"), ctx=_make_ctx())

    assert result.success is True
    seams.dataset_dao_find.assert_any_call(1)


@pytest.mark.asyncio
async def test_dataset_lookup_by_uuid(seams: _Seams) -> None:
    """A non-numeric dataset_id is resolved through the ``uuid`` id column."""
    result = await generate_chart(
        _make_request(dataset_id=DATASET_UUID), ctx=_make_ctx()
    )

    assert result.success is True
    seams.dataset_dao_find.assert_any_call(DATASET_UUID, id_column="uuid")


@pytest.mark.parametrize("dataset_id", ["1", DATASET_UUID])
@pytest.mark.asyncio
async def test_dataset_access_denied_returns_dataset_not_found(
    seams: _Seams, dataset_id: str
) -> None:
    """A dataset the user cannot access is reported as ``dataset_not_found``."""
    seams.has_dataset_access.return_value = False

    result = await generate_chart(_make_request(dataset_id=dataset_id), ctx=_make_ctx())

    assert result.success is False
    assert result.chart is None
    assert result.error is not None
    assert result.error.error_type == "dataset_not_found"
    assert result.error.error_code == "DATASET_NOT_FOUND"
    seams.command_cls.assert_not_called()


@pytest.mark.asyncio
async def test_happy_path_creates_chart_with_expected_payload(seams: _Seams) -> None:
    """``CreateChartCommand`` receives the mapped payload and the id is returned."""
    result = await generate_chart(_make_request(), ctx=_make_ctx())

    seams.command_cls.assert_called_once_with(
        {
            "slice_name": "My Chart",
            "viz_type": VIZ_TYPE,
            "datasource_id": 7,
            "datasource_type": "table",
            "params": json.dumps(_form_data()),
        }
    )
    assert result.success is True
    assert result.chart is not None
    assert result.chart.id == 42
    assert result.explore_url is not None
    assert result.explore_url.endswith("?slice_id=42")


@pytest.mark.asyncio
async def test_missing_chart_id_raises_runtime_error(seams: _Seams) -> None:
    """A command result without an id trips the chart-creation guard.

    ``RuntimeError`` is not among the exceptions ``generate_chart`` handles, so
    it propagates out of the tool.
    """
    seams.command_cls.return_value.run.return_value = _make_chart(chart_id=None)

    with pytest.raises(RuntimeError, match="no chart ID returned"):
        await generate_chart(_make_request(), ctx=_make_ctx())


@pytest.mark.asyncio
async def test_refresh_failure_is_logged_and_flow_continues(seams: _Seams) -> None:
    """A failing ``db.session.refresh`` warns but does not fail the request."""
    seams.session.refresh.side_effect = SQLAlchemyError("stale session")

    with patch(
        "superset.mcp_service.chart.tool.generate_chart.logger.warning"
    ) as mock_warning:
        result = await generate_chart(_make_request(), ctx=_make_ctx())

    assert result.success is True
    assert result.chart is not None
    assert result.chart.id == 42
    assert any(
        "refresh failed" in str(call.args[0]) for call in mock_warning.call_args_list
    )


@pytest.mark.asyncio
async def test_post_create_dataset_validation_failure_surfaces_warning(
    seams: _Seams,
) -> None:
    """Post-create dataset validation failures warn but keep the chart."""
    seams.validate_chart_dataset.return_value = Mock(
        is_valid=False,
        error="Chart's dataset is not accessible",
        warnings=["virtual dataset"],
    )
    ctx = _make_ctx()

    result = await generate_chart(_make_request(), ctx=ctx)

    seams.validate_chart_dataset.assert_called_once()
    assert seams.validate_chart_dataset.call_args.kwargs["check_access"] is True
    assert result.success is True
    assert result.chart is not None
    assert result.chart.id == 42
    assert "Chart's dataset is not accessible" in result.warnings
    assert "virtual dataset" in result.warnings


@pytest.mark.asyncio
async def test_compile_failure_deletes_chart_and_returns_error(seams: _Seams) -> None:
    """A failed compile check deletes the chart and reports the compile error."""
    chart = seams.command_cls.return_value.run.return_value
    seams.compile_chart.return_value = CompileResult(
        success=False, error="column 'bad_col' does not exist"
    )

    result = await generate_chart(_make_request(), ctx=_make_ctx())

    seams.chart_dao_delete.assert_called_once_with([chart])
    assert result.success is False
    assert result.chart is None
    assert result.error is not None
    assert result.error.error_code == "CHART_COMPILE_FAILED"
    assert "bad_col" in (result.error.details or "")


@pytest.mark.asyncio
async def test_compile_failure_prefers_structured_error_object(seams: _Seams) -> None:
    """``CompileResult.error_obj`` is returned verbatim when present."""
    seams.compile_chart.return_value = CompileResult(
        success=False,
        error="column 'bad_col' does not exist",
        error_obj=ChartGenerationError(
            error_type="compile_error",
            message="Did you mean sum_boys?",
            details="column 'bad_col' does not exist",
            suggestions=["Use sum_boys"],
            error_code="CHART_COMPILE_FAILED",
        ),
    )

    result = await generate_chart(_make_request(), ctx=_make_ctx())

    assert result.success is False
    assert result.error is not None
    assert result.error.message == "Did you mean sum_boys?"
    assert result.error.suggestions == ["Use sum_boys"]


@pytest.mark.asyncio
async def test_command_exception_is_logged_reported_and_reraised(
    seams: _Seams,
) -> None:
    """A ``CommandException`` is logged, reported via ``ctx.error`` and re-raised.

    The tool's outer handler converts it into a ``CHART_GENERATION_FAILED``
    response, so the exception is re-raised from the inner block only.
    """
    seams.command_cls.return_value.run.side_effect = CommandException("boom")
    ctx = _make_ctx()

    result = await generate_chart(_make_request(), ctx=ctx)

    assert any(
        "Chart creation failed" in call.args[0] for call in ctx.error.call_args_list
    )
    assert result.success is False
    assert result.error is not None
    assert result.error.error_code == "CHART_GENERATION_FAILED"


@pytest.mark.asyncio
async def test_chart_name_falls_back_to_generated_name(seams: _Seams) -> None:
    """Without ``chart_name``, the name is generated from config and dataset."""
    request = _make_request(chart_name=None)

    await generate_chart(request, ctx=_make_ctx())

    seams.generate_chart_name.assert_called_once_with(
        request.config, dataset_name="cleaned_sales_data"
    )
    assert seams.command_cls.call_args.args[0]["slice_name"] == "Generated Name"
