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

"""A miniature, deliberately buggy slice of Superset's chart-data path.

The demo's default traffic is synthetic, which is fine for exercising the
state machine but useless for a *real* Devin session: cloning the repo and
looking for the traceback correctly concludes it does not exist. These
functions exist so live mode has somewhere honest to point — each one carries
a genuine defect, raises a genuine traceback through this file, and can be
reproduced with a one-line command (see :mod:`error_orchestrator.seeded.bugs`).

The bugs are all of the "missing guard" family that these code paths really do
suffer from: an optional attribute assumed present, a form key assumed set, an
empty string handed to a parser, and bytes assumed to be UTF-8.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence


@dataclass
class Datasource:
    """The columns/metrics metadata a chart query is described against."""

    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryContext:
    """A chart data request. ``datasource`` is unset for ad-hoc queries."""

    datasource: Datasource | None = None
    form_data: dict[str, Any] = field(default_factory=dict)


def get_column_names(query_context: QueryContext) -> list[str]:
    """The columns a chart's payload should carry."""
    return list(query_context.datasource.data.get("columns", []))  # type: ignore[union-attr]


def get_time_filter_status(form_data: Mapping[str, Any]) -> dict[str, Any]:
    """Describe which temporal column a chart is filtered on."""
    temporal_column = form_data["granularity_sqla"]
    return {"column": temporal_column, "applied": True}


def parse_human_datetime(human_readable: str) -> datetime:
    """Parse the handful of date shapes the explore UI can produce."""
    return datetime.strptime(human_readable, "%Y-%m-%d")


def rows_to_csv(rows: Sequence[Sequence[bytes]]) -> str:
    """Render query results, which arrive as raw bytes from the DB driver."""
    return "\n".join(",".join(cell.decode("utf-8") for cell in row) for row in rows)
