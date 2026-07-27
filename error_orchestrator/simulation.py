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

"""Dry-run helpers: a scripted Devin client and a synthetic error stream.

Nothing here talks to Devin or to a real repo; it exists so the state machine,
the lanes and the risk registry can be exercised end to end locally and in
tests.
"""

from __future__ import annotations

import random
from typing import Any, Mapping

from error_orchestrator.devin_client import ScriptedDevinClient

SAFE_DIFF = """--- a/superset/utils/date_parser.py
+++ b/superset/utils/date_parser.py
@@ -120,7 +120,7 @@ def parse_human_datetime(human_readable: str) -> datetime:
-    parsed = parse(human_readable)
+    parsed = parse(human_readable) if human_readable else None
--- a/tests/unit_tests/utils/date_parser_test.py
+++ b/tests/unit_tests/utils/date_parser_test.py
@@ -10,3 +10,7 @@ def test_parse_human_datetime() -> None:
+def test_parse_human_datetime_empty() -> None:
+    assert parse_human_datetime("") is None
"""

RISKY_DIFF = """--- a/superset/migrations/versions/2024_01_01_abc_add_column.py
+++ b/superset/migrations/versions/2024_01_01_abc_add_column.py
@@ -1,3 +1,6 @@
+def upgrade() -> None:
+    op.drop_column("dashboards", "json_metadata")
"""


def classify_prompt(prompt: str) -> str:
    """Which lane produced this prompt (dry-run only)."""
    if "You are triaging a production error" in prompt:
        return "triage"
    if "Reproduce and fix a production error" in prompt:
        return "remediate"
    return "risk_check"


def make_dry_run_client(
    seed: int = 7,
    reproduce_rate: float = 0.75,
    risky_rate: float = 0.3,
    latency: float = 0.0,
) -> ScriptedDevinClient:
    """A Devin double that produces plausible outputs for every lane."""
    rng = random.Random(seed)  # noqa: S311 - simulation only

    def responder(prompt: str) -> Mapping[str, Any]:
        stage = classify_prompt(prompt)
        if stage == "triage":
            return {
                "action": "new_category",
                "title": "Simulated category",
                "summary": "dry-run triage",
                "confidence": 0.9,
            }
        if stage == "remediate":
            if rng.random() > reproduce_rate:
                return {
                    "reproduced": False,
                    "reason": "dry-run: could not reproduce locally",
                }
            return {
                "reproduced": True,
                "diff": RISKY_DIFF if rng.random() < risky_rate else SAFE_DIFF,
                "summary": "dry-run fix with regression test",
                "test_added": True,
                "branch": "devin/dry-run-fix",
            }
        return {"approved": True, "confidence": 0.9, "concerns": []}

    return ScriptedDevinClient(responder=responder, latency=latency)
