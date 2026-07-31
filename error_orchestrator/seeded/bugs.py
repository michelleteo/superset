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

"""Turns the seeded defects into simulator scenarios by *running* them.

Nothing here is hand-written traceback text: each bug is triggered for real
and the frames are read back off the exception, so what the orchestrator
ingests is exactly what the repository produces. That is what makes a live
Devin session able to reproduce and fix it.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from error_orchestrator.seeded import app, superset_app
from error_orchestrator.simulator import ErrorScenario, Frame

#: How a human (or a Devin session) makes the bug happen on a clean clone.
REPRO = "python -m error_orchestrator.seeded.reproduce {key}"
#: Frames are reported relative to this, the way a real logger would.
REPO_ROOT = Path(__file__).resolve().parents[2]
#: Files that only call the defect; their frames are not part of the report.
_HARNESS_FILES = frozenset({Path(__file__).name, "superset_app.py"})


@dataclass(frozen=True)
class SeededBug:
    """A real defect, in Superset itself or in :mod:`.seeded.app`."""

    key: str
    title: str
    message: str
    logger: str
    #: Calling this raises the defect's exception.
    trigger: Callable[[], object]
    #: The guard the defect is missing, as a diff against the file it lives in.
    #: Simulated remediation proposes this instead of a generic patch, so a
    #: seeded run shows a fix for the bug actually on the ticket.
    fix_diff: str = ""

    @property
    def repro_command(self) -> str:
        return REPRO.format(key=self.key)


SEEDED_BUGS: tuple[SeededBug, ...] = (
    SeededBug(
        key="seeded_datasource_none",
        title="AttributeError: 'NoneType' object has no attribute 'data'",
        message="Chart data query failed: ad-hoc query context has no datasource",
        logger="error_orchestrator.seeded.app.get_column_names",
        trigger=lambda: app.get_column_names(app.QueryContext()),
        fix_diff="""--- a/error_orchestrator/seeded/app.py
+++ b/error_orchestrator/seeded/app.py
@@ def get_column_names(query_context: QueryContext) -> list[str]:
-    return list(query_context.datasource.data.get("columns", []))
+    datasource = query_context.datasource
+    return list(datasource.data.get("columns", [])) if datasource else []
""",
    ),
    SeededBug(
        key="seeded_granularity_keyerror",
        title="KeyError: 'granularity_sqla'",
        message="Time filter status failed for a chart with no temporal column",
        logger="error_orchestrator.seeded.app.get_time_filter_status",
        trigger=lambda: app.get_time_filter_status({"viz_type": "table"}),
        fix_diff="""--- a/error_orchestrator/seeded/app.py
+++ b/error_orchestrator/seeded/app.py
@@ def get_time_filter_status(form_data: Mapping[str, Any]) -> dict[str, Any]:
-    temporal_column = form_data["granularity_sqla"]
-    return {"column": temporal_column, "applied": True}
+    temporal_column = form_data.get("granularity_sqla")
+    if temporal_column is None:
+        return {"column": None, "applied": False}
+    return {"column": temporal_column, "applied": True}
""",
    ),
    SeededBug(
        key="seeded_date_parser",
        title="ValueError: unparseable human readable date",
        message="Could not parse the date range the explore UI submitted",
        logger="error_orchestrator.seeded.app.parse_human_datetime",
        trigger=lambda: app.parse_human_datetime(""),
        fix_diff="""--- a/error_orchestrator/seeded/app.py
+++ b/error_orchestrator/seeded/app.py
@@ def parse_human_datetime(human_readable: str) -> datetime:
-    return datetime.strptime(human_readable, "%Y-%m-%d")
+    if not human_readable.strip():
+        raise ValueError("no date range was submitted")
+    return datetime.strptime(human_readable.strip(), "%Y-%m-%d")
""",
    ),
    SeededBug(
        key="seeded_csv_encoding",
        title="UnicodeDecodeError while rendering results as CSV",
        message="CSV export failed on a latin-1 encoded result cell",
        logger="error_orchestrator.seeded.app.rows_to_csv",
        trigger=lambda: app.rows_to_csv([[b"ok", b"caf\xe9"]]),
        fix_diff="""--- a/error_orchestrator/seeded/app.py
+++ b/error_orchestrator/seeded/app.py
@@ def rows_to_csv(rows: Sequence[Sequence[bytes]]) -> str:
-    return "\\n".join(",".join(cell.decode("utf-8") for cell in row) for row in rows)
+    return "\\n".join(
+        ",".join(cell.decode("utf-8", errors="replace") for cell in row)
+        for row in rows
+    )
""",
    ),
    SeededBug(
        key="superset_country_symbol_none",
        title="AttributeError: 'NoneType' object has no attribute 'lower'",
        message="Country lookup failed for a chart whose country column is unset",
        logger="superset.examples.countries.get",
        trigger=superset_app.country_lookup_without_symbol,
        fix_diff="""--- a/superset/examples/countries.py
+++ b/superset/examples/countries.py
@@ def get(field: str, symbol: str) -> Optional[dict[str, Any]]:
-    return all_lookups[field].get(symbol.lower())
+    if not symbol:
+        return None
+    return all_lookups[field].get(symbol.lower())
""",
    ),
    SeededBug(
        key="superset_country_unknown_field",
        title="KeyError: 'iso3'",
        message="Country lookup failed for a code standard that is not indexed",
        logger="superset.examples.countries.get",
        trigger=superset_app.country_lookup_unknown_field,
        fix_diff="""--- a/superset/examples/countries.py
+++ b/superset/examples/countries.py
@@ def get(field: str, symbol: str) -> Optional[dict[str, Any]]:
-    return all_lookups[field].get(symbol.lower())
+    lookup = all_lookups.get(field)
+    if lookup is None:
+        raise ValueError(f"unknown country field {field}; try one of {lookups}")
+    return lookup.get(symbol.lower())
""",
    ),
    SeededBug(
        key="superset_class_name_no_module",
        title="ValueError: Empty module name",
        message="Config loading failed on a class name with no module path",
        logger="superset.utils.class_utils.load_class_from_name",
        trigger=superset_app.class_name_without_module,
        fix_diff="""--- a/superset/utils/class_utils.py
+++ b/superset/utils/class_utils.py
@@ def load_class_from_name(fq_class_name: str) -> Any:
-    if not fq_class_name:
+    if not fq_class_name or "." not in fq_class_name:
         raise ValueError(f"Invalid class name {fq_class_name}")
""",
    ),
)

SEEDED_BUGS_BY_KEY = {bug.key: bug for bug in SEEDED_BUGS}


def capture(bug: SeededBug) -> BaseException:
    """Run the defect and hand back the exception it really raised."""
    try:
        bug.trigger()
    except Exception as error:  # pylint: disable=broad-except
        return error
    raise RuntimeError(f"seeded bug {bug.key} no longer fails — it has been fixed")


def _relative(filename: str) -> str:
    return str(Path(filename).resolve().relative_to(REPO_ROOT))


def _is_repository_code(filename: str) -> bool:
    """Frames from the standard library, site-packages or ``<frozen ...>`` are noise.

    An exception can surface anywhere below the defect — ``import_module`` raises
    ``ValueError`` from inside CPython — but the report has to name a file in this
    repository, since that is where a reader (or a session) has to go.
    """
    path = Path(filename)
    return (
        path.is_absolute()
        and path.is_file()
        and path.resolve().is_relative_to(REPO_ROOT)
        and path.name not in _HARNESS_FILES
    )


def _frames(error: BaseException) -> tuple[Frame, ...]:
    """The defect's own frames — the harness that called it is not the bug."""
    return tuple(
        Frame(
            file=_relative(frame.filename),
            line=frame.lineno or 0,
            func=frame.name,
            code=frame.line or "",
        )
        for frame in traceback.extract_tb(error.__traceback__)
        if _is_repository_code(frame.filename)
    )


def scenario_for(bug: SeededBug) -> ErrorScenario:
    """A simulator scenario carrying the bug's real frames and exception."""
    error = capture(bug)
    frames = _frames(error)
    if not frames:
        raise RuntimeError(
            f"seeded bug {bug.key} failed outside the repository "
            f"({error!r}) — the source files it needs are missing"
        )
    last = frames[-1]
    exception = "".join(traceback.format_exception_only(type(error), error)).strip()
    return ErrorScenario(
        key=bug.key,
        title=bug.title,
        logger=bug.logger,
        module=last.file.removesuffix(".py").replace("/", "."),
        func=last.func,
        line=last.line,
        exception=exception,
        message=f"{bug.message} (reproduce: {bug.repro_command})",
        frames=frames,
        fix="safe",
        diff=bug.fix_diff or None,
        match_hint=bug.key,
    )


def seeded_scenarios() -> list[ErrorScenario]:
    """Every seeded defect, as scenarios the simulator can emit."""
    return [scenario_for(bug) for bug in SEEDED_BUGS]
