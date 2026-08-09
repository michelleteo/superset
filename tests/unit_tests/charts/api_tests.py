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

from typing import Any
from unittest.mock import patch

from superset.commands.chart.exceptions import ChartForbiddenError


def test_create_chart_datasource_forbidden_returns_403(
    client: Any,
    full_api_access: None,
) -> None:
    """
    ``ChartForbiddenError`` from ``CreateChartCommand`` maps to a 403, like it
    does for the update and delete endpoints.
    """
    with patch(
        "superset.charts.api.CreateChartCommand.run",
        side_effect=ChartForbiddenError(),
    ):
        response = client.post(
            "/api/v1/chart/",
            json={
                "slice_name": "forbidden_datasource_chart",
                "datasource_id": 1,
                "datasource_type": "table",
            },
        )

    assert response.status_code == 403
