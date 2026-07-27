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

from __future__ import annotations

from error_orchestrator.models import RiskTier
from error_orchestrator.risk_registry import Diff, RiskRegistry
from error_orchestrator.simulation import RISKY_DIFF, SAFE_DIFF


def test_safe_fix_with_a_test_is_low_risk() -> None:
    assessment = RiskRegistry().assess(SAFE_DIFF)
    assert assessment.tier is RiskTier.LOW
    assert assessment.triggered_rules == []


def test_migration_change_is_a_hard_veto() -> None:
    assessment = RiskRegistry().assess(RISKY_DIFF)
    assert assessment.tier is RiskTier.HIGH
    assert "db_migration" in assessment.triggered_rules


def test_fix_without_a_test_is_high_risk() -> None:
    untested = SAFE_DIFF.split("--- a/tests")[0]
    assessment = RiskRegistry().assess(untested)
    assert assessment.tier is RiskTier.HIGH
    assert "no_test_coverage" in assessment.triggered_rules


def test_empty_diff_is_high_risk() -> None:
    assert RiskRegistry().assess("   ").tier is RiskTier.HIGH


def test_security_paths_and_secret_material_are_flagged() -> None:
    diff = """--- a/superset/security/manager.py
+++ b/superset/security/manager.py
@@ -1 +1,2 @@
+    api_key = "abc123"
--- a/tests/unit_tests/security_test.py
+++ b/tests/unit_tests/security_test.py
@@ -1 +1,2 @@
+def test_x() -> None: ...
"""
    assessment = RiskRegistry().assess(diff)
    assert assessment.tier is RiskTier.HIGH
    assert {"security_surface", "secret_material"} <= set(assessment.triggered_rules)


def test_diff_parsing_counts_files_and_lines() -> None:
    diff = Diff(SAFE_DIFF)
    assert diff.files == [
        "superset/utils/date_parser.py",
        "tests/unit_tests/utils/date_parser_test.py",
    ]
    assert diff.touches_tests is True
    assert diff.added_lines == 3
    assert diff.removed_lines == 1
