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

"""A synthetic Superset error source, for demonstrating the orchestrator.

The scenarios below are modelled on real Superset failure modes and carry
tracebacks through real modules, so the fingerprinter, the triage merge path
and the risk registry all see input shaped like production traffic:

* an **exact repeat** re-uses the same frames, so it hashes to a known pool in
  O(1) and never spends a session;
* a **variant** keeps the exception but reaches it through a different entry
  point, producing a fingerprint miss that only triage can resolve;
* a **new** scenario is a category nobody has seen before.

The simulator only produces webhook payloads. It posts them to
``/webhook/errors`` like Superset's ``WebhookLogHandler`` would, so the demo
exercises the same ingest path as a real deployment.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

import httpx

from error_orchestrator.devin_client import ScriptedDevinClient

logger = logging.getLogger(__name__)

Payload = dict[str, Any]
Sink = Callable[[Sequence[Payload]], Awaitable[int]]

USERS = [f"u{index:03d}" for index in range(1, 41)]


@dataclass(frozen=True)
class Frame:
    file: str
    line: int
    func: str
    code: str

    def render(self) -> str:
        return (
            f'  File "{self.file}", line {self.line}, in {self.func}\n    {self.code}'
        )


@dataclass(frozen=True)
class ErrorScenario:
    """One Superset failure mode the simulator can emit."""

    key: str
    title: str
    logger: str
    module: str
    func: str
    line: int
    exception: str
    message: str
    frames: tuple[Frame, ...]
    #: Alternate entry points: same bug, different call path, new fingerprint.
    variants: tuple[Frame, ...] = ()
    #: Which fix the demo Devin double proposes; drives the risk tier.
    fix: str = "safe"
    #: Fraction of remediation attempts that fail to reproduce.
    flaky: float = 0.0
    weight: float = 1.0
    #: Distinctive substring used to recognise this scenario in a Devin prompt.
    match_hint: str = ""
    #: Derived from another scenario, so its title shares that scenario's
    #: prefix and must never be used to match a merge candidate.
    is_mutation: bool = False

    def match_token(self) -> str:
        return self.match_hint or self.exception

    def render_traceback(self, entry: Frame | None) -> str:
        frames = ((entry,) if entry else ()) + self.frames
        body = "\n".join(frame.render() for frame in frames)
        return f"Traceback (most recent call last):\n{body}\n{self.exception}"


def _frame(file: str, line: int, func: str, code: str) -> Frame:
    return Frame(file=file, line=line, func=func, code=code)


SCENARIOS: tuple[ErrorScenario, ...] = (
    ErrorScenario(
        key="datasource_none",
        title="AttributeError: 'NoneType' object has no attribute 'data'",
        logger="superset.commands.chart.data.get_data_command",
        module="superset.commands.chart.data.get_data_command",
        func="run",
        line=68,
        exception="AttributeError: 'NoneType' object has no attribute 'data'",
        message="Error running chart data query for slice 4821",
        match_hint="query_context.datasource.data",
        frames=(
            _frame(
                "superset/commands/chart/data/get_data_command.py",
                68,
                "run",
                "payload = self._query_context.get_payload(",
            ),
            _frame(
                "superset/common/query_context.py",
                72,
                "get_payload",
                "return self._processor.get_payload(cache_query_context, force_cached)",
            ),
            _frame(
                "superset/common/query_context_processor.py",
                181,
                "get_df_payload",
                "columns = query_context.datasource.data.get('columns', [])",
            ),
        ),
        variants=(
            _frame(
                "superset/charts/data/api.py",
                412,
                "_get_data_response",
                "result = command.run(force_cached=force_cached)",
            ),
            _frame(
                "superset/tasks/async_queries.py",
                118,
                "load_chart_data_into_cache",
                "result = command.run(force_cached=False)",
            ),
            _frame(
                "superset/reports/commands/execute.py",
                389,
                "_get_screenshots",
                "payload = command.run()",
            ),
        ),
        fix="safe",
        weight=3.0,
    ),
    ErrorScenario(
        key="granularity_keyerror",
        title="KeyError: 'granularity_sqla'",
        logger="superset.explore.utils",
        module="superset.utils.core",
        func="get_time_filter_status",
        line=1284,
        exception="KeyError: 'granularity_sqla'",
        message="Failed to resolve time grain for datasource 318",
        match_hint="granularity_sqla",
        frames=(
            _frame(
                "superset/utils/core.py",
                1284,
                "get_time_filter_status",
                "temporal_column = form_data['granularity_sqla']",
            ),
        ),
        variants=(
            _frame(
                "superset/explore/api.py",
                144,
                "get",
                "return self.response(200, result=command.run())",
            ),
            _frame(
                "superset/charts/api.py",
                907,
                "get_data",
                "status = get_time_filter_status(datasource, form_data)",
            ),
        ),
        fix="safe",
        weight=2.0,
    ),
    ErrorScenario(
        key="date_parser",
        title="ValueError: Unknown string format for human readable date",
        logger="superset.utils.date_parser",
        module="superset.utils.date_parser",
        func="parse_human_datetime",
        line=120,
        exception="ValueError: Unknown string format: ''",
        message="Couldn't parse date string ''",
        match_hint="parse_human_datetime",
        frames=(
            _frame(
                "superset/utils/date_parser.py",
                120,
                "parse_human_datetime",
                "parsed = parse(human_readable)",
            ),
        ),
        variants=(
            _frame(
                "superset/models/helpers.py",
                1622,
                "get_time_range",
                "since = parse_human_datetime(since)",
            ),
        ),
        fix="safe",
        weight=2.0,
    ),
    ErrorScenario(
        key="redis_timeout",
        title="Redis cache backend timed out",
        logger="superset.utils.cache_manager",
        module="superset.utils.cache",
        func="get",
        line=94,
        exception="redis.exceptions.TimeoutError: Timeout reading from socket",
        message="Cache lookup failed for key slice_data_4821",
        match_hint="redis.exceptions.TimeoutError",
        frames=(
            _frame(
                "superset/utils/cache.py",
                94,
                "get",
                "return cache.get(cache_key)",
            ),
            _frame(
                "redis/connection.py",
                624,
                "read_response",
                "raise TimeoutError('Timeout reading from socket')",
            ),
        ),
        variants=(
            _frame(
                "superset/tasks/cache.py",
                211,
                "cache_warmup",
                "value = cache_manager.data_cache.get(key)",
            ),
        ),
        fix="deps",
        flaky=0.55,
        weight=1.5,
    ),
    ErrorScenario(
        key="rls_guest_token",
        title="SupersetSecurityException: guest token failed RLS check",
        logger="superset.security.manager",
        module="superset.security.manager",
        func="raise_for_access",
        line=2411,
        exception="superset.exceptions.SupersetSecurityException: Guest user "
        "cannot access dashboard 92",
        message="Guest token rejected for dashboard 92",
        match_hint="raise_for_access",
        frames=(
            _frame(
                "superset/security/manager.py",
                2411,
                "raise_for_access",
                "raise SupersetSecurityException("
                "self.get_dashboard_access_error_object())",
            ),
        ),
        variants=(
            _frame(
                "superset/embedded/view.py",
                88,
                "embedded",
                "security_manager.raise_for_access(dashboard=dashboard)",
            ),
        ),
        fix="security",
        weight=1.0,
    ),
    ErrorScenario(
        key="migration_lock",
        title="OperationalError: could not obtain lock on relation dashboards",
        logger="superset.migrations.shared.utils",
        module="superset.migrations.shared.utils",
        func="add_columns",
        line=214,
        exception="sqlalchemy.exc.OperationalError: (psycopg2.errors.LockNotAvailable) "
        "could not obtain lock on relation 'dashboards'",
        message="Migration step failed while altering dashboards",
        match_hint="LockNotAvailable",
        frames=(
            _frame(
                "superset/migrations/shared/utils.py",
                214,
                "add_columns",
                "op.add_column(table_name, column)",
            ),
        ),
        fix="migration",
        weight=0.8,
    ),
    ErrorScenario(
        key="csv_encoding",
        title="UnicodeDecodeError while exporting results to CSV",
        logger="superset.utils.csv",
        module="superset.utils.csv",
        func="df_to_escaped_csv",
        line=57,
        exception="UnicodeDecodeError: 'utf-8' codec can't decode byte 0xa0 in "
        "position 12: invalid start byte",
        message="CSV export failed for query 77213",
        match_hint="df_to_escaped_csv",
        frames=(
            _frame(
                "superset/utils/csv.py",
                57,
                "df_to_escaped_csv",
                "return df.to_csv(**kwargs)",
            ),
        ),
        variants=(
            _frame(
                "superset/views/core.py",
                1183,
                "csv",
                "csv_data = csv.df_to_escaped_csv("
                "df, index=False, **config['CSV_EXPORT'])",
            ),
        ),
        fix="safe",
        weight=1.2,
    ),
    ErrorScenario(
        key="presto_syntax",
        title="DatabaseError: line 1:8 mismatched input 'FROM'",
        logger="superset.db_engine_specs.presto",
        module="superset.db_engine_specs.presto",
        func="execute",
        line=1102,
        exception="superset.exceptions.SupersetErrorException: "
        "pyhive.exc.DatabaseError: line 1:8 mismatched input 'FROM'",
        message="Ad-hoc query failed on database 12",
        match_hint="mismatched input",
        frames=(
            _frame(
                "superset/db_engine_specs/presto.py",
                1102,
                "execute",
                "cursor.execute(statement)",
            ),
            _frame(
                "superset/sql_lab.py",
                412,
                "execute_sql_statement",
                "db_engine_spec.execute(cursor, sql, database)",
            ),
        ),
        fix="safe",
        flaky=0.7,
        weight=1.5,
    ),
    ErrorScenario(
        key="report_screenshot",
        title="ReportScheduleScreenshotFailedError: screenshot timed out",
        logger="superset.commands.report.execute",
        module="superset.commands.report.execute",
        func="_get_screenshots",
        line=402,
        exception="superset.commands.report.exceptions."
        "ReportScheduleScreenshotFailedError",
        message="Screenshot generation timed out for report 145",
        match_hint="raise ReportScheduleScreenshotFailedError()",
        frames=(
            _frame(
                "superset/commands/report/execute.py",
                402,
                "_get_screenshots",
                "raise ReportScheduleScreenshotFailedError()",
            ),
        ),
        fix="large",
        weight=1.0,
    ),
    ErrorScenario(
        key="async_worker_oom",
        title="MemoryError in async query worker",
        logger="superset.tasks.async_queries",
        module="superset.tasks.async_queries",
        func="load_chart_data_into_cache",
        line=131,
        exception="MemoryError",
        message="Async chart data worker exhausted memory on query 90214",
        match_hint="cache.set(cache_key, payload)",
        frames=(
            _frame(
                "superset/tasks/async_queries.py",
                131,
                "load_chart_data_into_cache",
                "cache.set(cache_key, payload)",
            ),
        ),
        fix="large",
        flaky=0.4,
        weight=0.8,
    ),
)

SCENARIOS_BY_KEY = {scenario.key: scenario for scenario in SCENARIOS}


# --------------------------------------------------------------------- sinks


class WebhookSink:
    """Posts payloads to a running orchestrator's ``/webhook/errors``."""

    def __init__(self, url: str, token: str | None = None, timeout: float = 5.0):
        self.url = url
        self._headers = {"X-Webhook-Token": token} if token else {}
        self._client = httpx.AsyncClient(timeout=timeout)

    async def __call__(self, payloads: Sequence[Payload]) -> int:
        if not payloads:
            return 0
        try:
            response = await self._client.post(
                self.url, json=list(payloads), headers=self._headers
            )
        except httpx.HTTPError:
            logger.warning("simulator could not reach %s", self.url, exc_info=True)
            return 0
        if response.status_code >= 400:
            logger.warning("webhook rejected batch: %s", response.status_code)
            return 0
        return int(response.json().get("accepted", len(payloads)))

    async def aclose(self) -> None:
        await self._client.aclose()


# ----------------------------------------------------------------- simulator


@dataclass
class SimulatorConfig:
    #: Errors emitted per second while running.
    rate: float = 1.5
    #: Probability an event repeats a fingerprint already seen (O(1) dedup).
    duplicate_rate: float = 0.55
    #: Probability an event is a known bug down a new code path (triage merge).
    variant_rate: float = 0.25
    #: Probability an occurrence carries a user id (drives affected-user score).
    user_rate: float = 0.7
    seed: int | None = None


class ErrorSimulator:
    """Emits a continuous, controllable stream of Superset-shaped errors."""

    def __init__(
        self,
        sink: Sink,
        config: SimulatorConfig | None = None,
        scenarios: list[ErrorScenario] | None = None,
    ) -> None:
        self.sink = sink
        self.config = config or SimulatorConfig()
        #: Mutable on purpose: mutated scenarios are appended here, and a
        #: :class:`DemoDevinClient` sharing this list keeps recognising them.
        self.scenarios = list(SCENARIOS) if scenarios is None else scenarios
        if not self.scenarios:
            raise ValueError("at least one scenario is required")
        #: Mutations derive from the catalog the run started with, so a seeded
        #: run never drifts back into synthetic categories.
        self._catalog = list(self.scenarios)
        self.running = True
        self.emitted = 0
        self._rng = random.Random(self.config.seed)  # noqa: S311 - simulation only
        self._seen: set[str] = set()
        self._mutations = 0
        self._task: asyncio.Task[None] | None = None

    # ---------------------------------------------------------- construction

    def _pick_scenario(self) -> ErrorScenario:
        return self._rng.choices(
            self.scenarios, weights=[s.weight for s in self.scenarios], k=1
        )[0]

    def mutate(self, base: ErrorScenario) -> ErrorScenario:
        """Derive a never-seen-before category from an existing scenario.

        A finite catalog would go quiet once every scenario had a pool, and the
        interesting states would stop changing. Mutation keeps genuinely new
        bugs arriving: a distinct call path, message and title, so it hashes
        to a miss and triage has to rule out every existing category.
        """
        self._mutations += 1
        index = self._mutations
        func = f"{base.func}_path{index}"
        line = base.line + index * 13
        entry = _frame(
            base.frames[0].file if base.frames else "superset/views/core.py",
            line,
            func,
            f"result = {func}(payload)",
        )
        scenario = ErrorScenario(
            key=f"{base.key}~{index}",
            title=f"{base.title} [{func}]",
            logger=base.logger,
            module=base.module,
            func=func,
            line=line,
            exception=base.exception,
            message=f"{base.message} (path {index})",
            frames=(entry, *base.frames),
            # No variants: a mutation's alternate paths would reuse the base's
            # frames and hash back into the base pool.
            variants=(),
            fix=base.fix,
            flaky=base.flaky,
            weight=base.weight * 0.5,
            # Include the call parentheses: a bare "..._path4" would also be a
            # substring of "..._path44", and mutations would match each other.
            match_hint=f"{func}(payload)",
            is_mutation=True,
        )
        self.scenarios.append(scenario)
        return scenario

    def build_payload(
        self, scenario: ErrorScenario, entry: Frame | None = None
    ) -> Payload:
        return {
            "timestamp": time.time(),
            "level": "ERROR",
            "logger": scenario.logger,
            "message": scenario.message,
            "module": scenario.module,
            "func": entry.func if entry else scenario.func,
            "line": entry.line if entry else scenario.line,
            "traceback": scenario.render_traceback(entry),
            "user_id": (
                self._rng.choice(USERS)
                if self._rng.random() < self.config.user_rate
                else None
            ),
            "service": "superset",
            "event_id": uuid.uuid4().hex,
            "scenario": scenario.key,
        }

    def next_payload(self) -> Payload:
        """One event, weighted between repeat / variant / brand-new."""
        roll = self._rng.random()
        seen = [s for s in self.scenarios if s.key in self._seen]
        unseen = [s for s in self.scenarios if s.key not in self._seen]

        if seen and roll < self.config.duplicate_rate:
            scenario = self._rng.choice(seen)
            return self.build_payload(scenario)
        if seen and roll < self.config.duplicate_rate + self.config.variant_rate:
            candidates = [s for s in seen if s.variants]
            if candidates:
                scenario = self._rng.choice(candidates)
                return self.build_payload(scenario, self._rng.choice(scenario.variants))
        scenario = (
            self._rng.choice(unseen)
            if unseen
            else self.mutate(self._rng.choice(self._catalog))
        )
        self._seen.add(scenario.key)
        return self.build_payload(scenario)

    # -------------------------------------------------------------- emission

    async def emit(self, count: int = 1) -> int:
        payloads = [self.next_payload() for _ in range(max(count, 0))]
        accepted = await self.sink(payloads)
        self.emitted += accepted
        return accepted

    async def inject(self, key: str, count: int = 1) -> int:
        """Emit a specific scenario on demand (dashboard "trigger" button)."""
        scenario = next((s for s in self.scenarios if s.key == key), None)
        if scenario is None:
            raise KeyError(key)
        self._seen.add(scenario.key)
        payloads = [self.build_payload(scenario) for _ in range(max(count, 1))]
        accepted = await self.sink(payloads)
        self.emitted += accepted
        return accepted

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="simulator")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        aclose = getattr(self.sink, "aclose", None)
        if aclose is not None:
            await aclose()

    async def _loop(self) -> None:
        while True:
            rate = max(self.config.rate, 0.01)
            await asyncio.sleep(1.0 / rate)
            if not self.running:
                continue
            try:
                await self.emit()
            except Exception:  # pylint: disable=broad-except
                logger.exception("simulator emit failed")

    def status(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "rate": self.config.rate,
            "emitted": self.emitted,
            "mutations": self._mutations,
            "scenarios": [
                {"key": scenario.key, "title": scenario.title, "fix": scenario.fix}
                for scenario in SCENARIOS
            ],
        }


# ------------------------------------------------------- scripted Devin side

SAFE_DIFF = """--- a/superset/common/query_context_processor.py
+++ b/superset/common/query_context_processor.py
@@ -178,7 +178,9 @@ class QueryContextProcessor:
-        columns = query_context.datasource.data.get("columns", [])
+        datasource = query_context.datasource
+        columns = datasource.data.get("columns", []) if datasource else []
--- a/tests/unit_tests/common/query_context_processor_test.py
+++ b/tests/unit_tests/common/query_context_processor_test.py
@@ -40,3 +40,8 @@ def test_get_df_payload() -> None:
+def test_get_df_payload_without_datasource() -> None:
+    processor = QueryContextProcessor(query_context_without_datasource())
+    assert processor.get_df_payload()["columns"] == []
"""

MIGRATION_DIFF = """--- a/superset/migrations/versions/2024_05_02_ab12_lock.py
+++ b/superset/migrations/versions/2024_05_02_ab12_lock.py
@@ -1,3 +1,8 @@
+def upgrade() -> None:
+    op.execute("SET lock_timeout = '5s'")
+    op.add_column("dashboards", sa.Column("last_locked_at", sa.DateTime()))
"""

SECURITY_DIFF = """--- a/superset/security/manager.py
+++ b/superset/security/manager.py
@@ -2405,7 +2405,7 @@ class SupersetSecurityManager:
-        if self.is_guest_user() and dashboard.id not in guest_dashboard_ids:
+        if self.is_guest_user() and not self.has_guest_access(dashboard):
             raise SupersetSecurityException(...)
"""

DEPS_DIFF = """--- a/requirements/base.txt
+++ b/requirements/base.txt
@@ -120,7 +120,7 @@
-redis==4.6.0
+redis==5.0.8
--- a/superset/utils/cache.py
+++ b/superset/utils/cache.py
@@ -90,7 +90,10 @@ def get(cache_key: str) -> Any:
-    return cache.get(cache_key)
+    try:
+        return cache.get(cache_key)
+    except RedisTimeoutError:
+        return None
"""

LARGE_DIFF = "\n".join(
    [
        f"""--- a/superset/commands/report/execute_part{index}.py
+++ b/superset/commands/report/execute_part{index}.py
@@ -1,4 +1,9 @@
+    def _wait_for_screenshot(self, timeout: int) -> bytes:
+        deadline = time.monotonic() + timeout
+        while time.monotonic() < deadline:
+            if (shot := self._poll()) is not None:
+                return shot
"""
        for index in range(9)
    ]
)

DIFFS = {
    "safe": SAFE_DIFF,
    "migration": MIGRATION_DIFF,
    "security": SECURITY_DIFF,
    "deps": DEPS_DIFF,
    "large": LARGE_DIFF,
}


@dataclass
class LatencyProfile:
    """How long each simulated Devin session takes, in seconds."""

    triage: tuple[float, float] = (2.0, 5.0)
    remediate: tuple[float, float] = (10.0, 24.0)
    risk_check: tuple[float, float] = (3.0, 8.0)

    def scaled(self, factor: float) -> LatencyProfile:
        def scale(span: tuple[float, float]) -> tuple[float, float]:
            return (span[0] * factor, span[1] * factor)

        return LatencyProfile(
            triage=scale(self.triage),
            remediate=scale(self.remediate),
            risk_check=scale(self.risk_check),
        )


class DemoDevinClient:
    """Scenario-aware Devin double: realistic latency, per-scenario outcomes.

    Recognises which scenario a prompt is about from its distinctive token, so
    the same error always yields the same kind of fix — and therefore the same
    risk tier — which makes a demo reproducible.
    """

    def __init__(
        self,
        latency: LatencyProfile | None = None,
        scenarios: list[ErrorScenario] | None = None,
        seed: int | None = 11,
        review_approval_rate: float = 0.75,
    ) -> None:
        self.latency = latency or LatencyProfile()
        #: Shared with the simulator so mutated scenarios stay recognisable.
        self.scenarios = list(SCENARIOS) if scenarios is None else scenarios
        self.review_approval_rate = review_approval_rate
        self._rng = random.Random(seed)  # noqa: S311 - simulation only
        self._counter = 0

    def _scenario_for(self, prompt: str) -> ErrorScenario | None:
        # Longest token first: a mutated scenario's token contains its base's.
        for scenario in sorted(
            self.scenarios, key=lambda s: len(s.match_token()), reverse=True
        ):
            if scenario.match_token() in prompt or scenario.message in prompt:
                return scenario
        return None

    @staticmethod
    def _stage(prompt: str) -> str:
        if "You are triaging a production error" in prompt:
            return "triage"
        if "Reproduce and fix a production error" in prompt:
            return "remediate"
        return "risk_check"

    def _respond(self, stage: str, prompt: str) -> Mapping[str, Any]:
        scenario = self._scenario_for(prompt)
        if stage == "triage":
            # A variant reaches triage with an existing pool for the same bug;
            # merging is the interesting path, so prefer it when offered.
            candidate = _first_candidate_pool_id(prompt, scenario)
            if candidate:
                return {
                    "action": "merge",
                    "pool_id": candidate,
                    "summary": "same root cause, different entry point",
                    "confidence": 0.86,
                }
            return {
                "action": "new_category",
                "title": scenario.title if scenario else "Unclassified error",
                "summary": "no existing category shares this root cause",
                "confidence": 0.9,
            }
        if stage == "remediate":
            flaky = scenario.flaky if scenario else 0.15
            if self._rng.random() < flaky:
                return {
                    "reproduced": False,
                    "reason": "could not reproduce locally against master",
                }
            fix = scenario.fix if scenario else "safe"
            return {
                "reproduced": True,
                "diff": DIFFS.get(fix, SAFE_DIFF),
                "summary": f"fix for {scenario.title if scenario else 'error'}",
                "test_added": fix in ("safe", "deps"),
                "branch": f"devin/fix-{scenario.key if scenario else 'error'}",
            }
        approved = self._rng.random() < self.review_approval_rate
        return {
            "approved": approved,
            "confidence": 0.9 if approved else 0.5,
            "concerns": [] if approved else ["test does not fail without the fix"],
            "summary": "independent review complete",
        }

    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> Any:
        stage = self._stage(prompt)
        low, high = getattr(self.latency, stage)
        await asyncio.sleep(self._rng.uniform(low, high))
        self._counter += 1
        session_id = f"demo-{self._counter}"
        from error_orchestrator.devin_client import DevinSessionResult

        return DevinSessionResult(
            session_id=session_id,
            url=f"https://app.devin.ai/sessions/{session_id}",
            status="finished",
            structured_output=dict(self._respond(stage, prompt)),
        )


class BudgetedDevinClient:
    """Spend up to ``budget`` real Devin sessions, then fall back to the double.

    Real sessions produce real diffs but take minutes and cost money, which is
    at odds with a dashboard that has to keep moving. This runs the first N
    pools through the real API — enough to show genuine Devin-authored diffs —
    and simulates the rest so throughput stays visible.
    """

    def __init__(
        self, live: Any, fallback: Any, budget: int = 3, stages: Sequence[str] = ()
    ) -> None:
        self.live = live
        self.fallback = fallback
        self.budget = budget
        #: Which lanes may use the real API; empty means all of them.
        self.stages = tuple(stages)
        self.spent = 0
        #: Real sessions in flight, by stage, with when they started. A real
        #: session holds its worker for minutes, so the dashboard needs to be
        #: able to say that a slot is waiting on Devin rather than wedged.
        self.in_flight: dict[str, float] = {}

    def _use_live(self, stage: str) -> bool:
        if self.stages and stage not in self.stages:
            return False
        return self.spent < self.budget

    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> Any:
        stage = DemoDevinClient._stage(prompt)  # noqa: SLF001 - same module
        if not self._use_live(stage):
            return await self.fallback.run_session(
                prompt, title=title, tags=tags, idempotent=idempotent
            )
        self.spent += 1
        logger.info(
            "spending real Devin session %s/%s on %s", self.spent, self.budget, stage
        )
        key = f"{stage}:{self.spent}"
        self.in_flight[key] = time.time()
        try:
            return await self.live.run_session(
                prompt, title=title, tags=tags, idempotent=idempotent
            )
        finally:
            self.in_flight.pop(key, None)

    def live_status(self) -> dict[str, Any]:
        """What the dashboard says about real session spend."""
        now = time.time()
        return {
            "spent": self.spent,
            "budget": self.budget,
            "in_flight": [
                {"stage": key.split(":")[0], "elapsed": now - started}
                for key, started in sorted(self.in_flight.items())
            ],
        }


def _first_candidate_pool_id(prompt: str, scenario: ErrorScenario | None) -> str | None:
    """Pull a merge candidate out of a triage prompt, if one matches the scenario."""
    if scenario is None:
        return None
    for line in prompt.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- pool_id="):
            continue
        pool_id = stripped.removeprefix("- pool_id=").split()[0]
        block_start = prompt.index(stripped)
        block = prompt[block_start : block_start + 400]
        # A mutation is a new bug that merely reads like its base scenario, so
        # only its unique token may match; matching on the shared title prefix
        # would collapse every mutation back into the pool it was derived from.
        if scenario.match_token() in block:
            return pool_id
        if not scenario.is_mutation and scenario.title in block:
            return pool_id
    return None


def make_scripted_client(latency: float = 0.0) -> ScriptedDevinClient:
    """Zero-latency variant used by tests that only need plausible output."""
    demo = DemoDevinClient(latency=LatencyProfile((0, 0), (0, 0), (0, 0)))
    return ScriptedDevinClient(
        responder=lambda prompt: demo._respond(  # noqa: SLF001 - same module
            DemoDevinClient._stage(prompt), prompt
        ),
        latency=latency,
    )


__all__ = [
    "DemoDevinClient",
    "ErrorScenario",
    "ErrorSimulator",
    "Frame",
    "LatencyProfile",
    "SCENARIOS",
    "SCENARIOS_BY_KEY",
    "SimulatorConfig",
    "WebhookSink",
    "make_scripted_client",
]
