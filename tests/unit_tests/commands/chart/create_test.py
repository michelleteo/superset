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
"""Unit tests for ``CreateChartCommand``: payload normalization, validation
branches and the ``run()`` side effects."""

from datetime import datetime
from typing import Any

import pytest
from pytest_mock import MockerFixture

from superset.commands.chart.create import CreateChartCommand
from superset.commands.chart.exceptions import (
    ChartForbiddenError,
    ChartInvalidError,
    DashboardsForbiddenError,
)
from superset.commands.exceptions import DatasourceNotFoundValidationError
from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
from superset.exceptions import SupersetSecurityException
from superset.subjects.exceptions import SubjectsNotFoundValidationError
from superset.utils import json


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "slice_name": "test",
        "datasource_id": 1,
        "datasource_type": "table",
    }
    payload.update(overrides)
    return payload


def _security_exception() -> SupersetSecurityException:
    return SupersetSecurityException(
        SupersetError(
            error_type=SupersetErrorType.DATASOURCE_SECURITY_ACCESS_ERROR,
            message="Access denied",
            level=ErrorLevel.ERROR,
        )
    )


# ---------------------------------------------------------------------------
# __init__: params / viz_type handling
# ---------------------------------------------------------------------------


def test_viz_type_is_adopted_from_params() -> None:
    """``viz_type`` supplied only inside ``params`` is promoted to the top level."""
    command = CreateChartCommand(_payload(params=json.dumps({"viz_type": "pie"})))

    assert command._properties["viz_type"] == "pie"


def test_explicit_viz_type_wins_over_params() -> None:
    """An explicit top-level ``viz_type`` is not overwritten by ``params``."""
    command = CreateChartCommand(
        _payload(viz_type="table", params=json.dumps({"viz_type": "pie"}))
    )

    assert command._properties["viz_type"] == "table"


@pytest.mark.parametrize(
    "params",
    [
        "[1, 2]",  # valid JSON, but not a dict
        '"pie"',  # valid JSON string
        "42",  # valid JSON number
        json.dumps({"granularity": "ds"}),  # dict without a viz_type key
    ],
)
def test_no_viz_type_inferred_from_non_dict_params(params: str) -> None:
    """Valid JSON payloads that carry no ``viz_type`` mapping are left alone."""
    command = CreateChartCommand(_payload(params=params))

    assert "viz_type" not in command._properties


@pytest.mark.parametrize("params", [None, "", 0])
def test_falsy_params_are_skipped(params: Any) -> None:
    """Absent/empty ``params`` short-circuits the walrus guard without crashing."""
    command = CreateChartCommand(_payload(params=params))

    assert "viz_type" not in command._properties
    assert command._properties["params"] == params


def test_missing_params_key_is_skipped() -> None:
    """A payload with no ``params`` key at all does not crash."""
    command = CreateChartCommand(_payload())

    assert "viz_type" not in command._properties


@pytest.mark.parametrize("params", ["not-json", "{unclosed", "{'single': 'quotes'}"])
def test_malformed_params_raise_json_decode_error(params: str) -> None:
    """``json.loads`` in ``__init__`` is unguarded, so a malformed ``params``
    string surfaces as a ``JSONDecodeError`` at construction time.

    The REST API never reaches this path because ``ChartPostSchema`` rejects
    invalid JSON first, but non-API callers (e.g. MCP tools) that instantiate
    the command directly do, and get a raw decoding error rather than a
    ``ChartInvalidError``.
    """
    with pytest.raises(json.JSONDecodeError):
        CreateChartCommand(_payload(params=params))


def test_input_payload_is_not_mutated() -> None:
    """The command copies its input, so the caller's dict is left untouched."""
    data = _payload(params=json.dumps({"viz_type": "pie"}))

    CreateChartCommand(data)

    assert "viz_type" not in data


# ---------------------------------------------------------------------------
# validate()
# ---------------------------------------------------------------------------


def test_validate_collects_datasource_not_found(mocker: MockerFixture) -> None:
    """A missing datasource is collected and re-raised inside ChartInvalidError."""
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id",
        side_effect=DatasourceNotFoundValidationError(),
    )
    mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids", return_value=[]
    )
    mocker.patch("superset.commands.chart.create.populate_subjects")

    with pytest.raises(ChartInvalidError) as excinfo:
        CreateChartCommand(_payload()).validate()

    assert excinfo.value.normalized_messages() == {
        "datasource_id": ["Datasource does not exist"]
    }


def test_validate_collects_missing_dashboards(mocker: MockerFixture) -> None:
    """Fewer dashboards found than requested yields DashboardsNotFound."""
    datasource = mocker.MagicMock()
    datasource.name = "test_table"
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id", return_value=datasource
    )
    mocker.patch("superset.commands.chart.create.security_manager.raise_for_access")
    mocker.patch(
        "superset.commands.chart.create.security_manager.is_editor", return_value=True
    )
    mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids",
        return_value=[mocker.MagicMock()],
    )
    mocker.patch("superset.commands.chart.create.populate_subjects")

    with pytest.raises(ChartInvalidError) as excinfo:
        CreateChartCommand(_payload(dashboards=[1, 2])).validate()

    assert excinfo.value.normalized_messages() == {
        "dashboards": ["Dashboards do not exist"]
    }


def test_validate_raises_dashboards_forbidden_immediately(
    mocker: MockerFixture,
) -> None:
    """A dashboard the user cannot edit raises immediately, short-circuiting
    ``populate_subjects`` and the aggregate ``ChartInvalidError``."""
    datasource = mocker.MagicMock()
    datasource.name = "test_table"
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id", return_value=datasource
    )
    mocker.patch("superset.commands.chart.create.security_manager.raise_for_access")
    mocker.patch(
        "superset.commands.chart.create.security_manager.is_editor", return_value=False
    )
    mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids",
        return_value=[mocker.MagicMock()],
    )
    populate_subjects = mocker.patch("superset.commands.chart.create.populate_subjects")

    with pytest.raises(DashboardsForbiddenError):
        CreateChartCommand(_payload(dashboards=[1])).validate()

    populate_subjects.assert_not_called()


def test_validate_forbidden_datasource_skips_dashboard_lookup(
    mocker: MockerFixture,
) -> None:
    """``ChartForbiddenError`` is raised before dashboards are even resolved."""
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "superset.commands.chart.create.security_manager.raise_for_access",
        side_effect=_security_exception(),
    )
    find_by_ids = mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids", return_value=[]
    )

    with pytest.raises(ChartForbiddenError):
        CreateChartCommand(_payload(dashboards=[1])).validate()

    find_by_ids.assert_not_called()


def test_validate_aggregates_multiple_errors(mocker: MockerFixture) -> None:
    """Datasource, dashboard and subject errors are aggregated into one error."""
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id",
        side_effect=DatasourceNotFoundValidationError(),
    )
    mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids", return_value=[]
    )

    def _populate(properties: dict[str, Any], exceptions: list[Exception]) -> None:
        exceptions.append(SubjectsNotFoundValidationError("editors"))

    mocker.patch(
        "superset.commands.chart.create.populate_subjects", side_effect=_populate
    )

    with pytest.raises(ChartInvalidError) as excinfo:
        CreateChartCommand(_payload(dashboards=[1], editors=[7])).validate()

    assert set(excinfo.value.normalized_messages()) == {
        "datasource_id",
        "dashboards",
        "editors",
    }


def test_validate_populates_datasource_name_and_dashboards(
    mocker: MockerFixture,
) -> None:
    """A successful validation resolves the datasource name and dashboard models."""
    datasource = mocker.MagicMock()
    datasource.name = "test_table"
    dashboard = mocker.MagicMock()
    mocker.patch(
        "superset.commands.chart.create.get_datasource_by_id", return_value=datasource
    )
    mocker.patch("superset.commands.chart.create.security_manager.raise_for_access")
    mocker.patch(
        "superset.commands.chart.create.security_manager.is_editor", return_value=True
    )
    mocker.patch(
        "superset.commands.chart.create.DashboardDAO.find_by_ids",
        return_value=[dashboard],
    )
    mocker.patch("superset.commands.chart.create.populate_subjects")

    command = CreateChartCommand(_payload(dashboards=[1]))
    command.validate()

    assert command._properties["datasource_name"] == "test_table"
    assert command._properties["dashboards"] == [dashboard]


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


@pytest.fixture
def created_chart(mocker: MockerFixture) -> Any:
    """Patch out ``validate()``, the DAO and the transaction's session."""
    mocker.patch.object(CreateChartCommand, "validate")
    mocker.patch("superset.utils.decorators.g", in_transaction=True)
    return mocker.patch("superset.commands.chart.create.ChartDAO.create")


def test_run_sets_last_saved_fields(mocker: MockerFixture, created_chart: Any) -> None:
    """``run()`` stamps ``last_saved_at``/``last_saved_by`` before the insert."""
    user = mocker.MagicMock()
    mocker.patch("superset.commands.chart.create.g", user=user)

    chart = CreateChartCommand(_payload()).run()

    assert chart is created_chart.return_value
    attributes = created_chart.call_args.kwargs["attributes"]
    assert isinstance(attributes["last_saved_at"], datetime)
    assert attributes["last_saved_by"] is user
    assert attributes["slice_name"] == "test"


def test_run_calls_after_asset_create_hook(
    mocker: MockerFixture, created_chart: Any
) -> None:
    """The ``AFTER_ASSET_CREATE`` hook is invoked with the chart and its type."""
    mocker.patch("superset.commands.chart.create.g", user=mocker.MagicMock())
    after_create = mocker.MagicMock()
    mocker.patch.dict(
        "superset.commands.chart.create.current_app.config",
        {"AFTER_ASSET_CREATE": after_create},
    )

    chart = CreateChartCommand(_payload()).run()

    after_create.assert_called_once_with(chart, "chart")


def test_run_skips_after_asset_create_when_unset(
    mocker: MockerFixture, created_chart: Any
) -> None:
    """No hook is called when ``AFTER_ASSET_CREATE`` is not configured."""
    mocker.patch("superset.commands.chart.create.g", user=mocker.MagicMock())
    mocker.patch.dict(
        "superset.commands.chart.create.current_app.config",
        {"AFTER_ASSET_CREATE": None},
    )

    # No hook to assert against; the test guards against a ``None`` call attempt.
    assert CreateChartCommand(_payload()).run() is created_chart.return_value
