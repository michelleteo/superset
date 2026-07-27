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

"""Programmatic rules deciding whether a diff may bypass human review.

Everything here is deterministic and cheap: it runs before the (expensive)
Devin review session and can veto auto-merge on its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Sequence

from error_orchestrator.models import RiskAssessment, RiskTier

#: Score at/above which a diff is High risk and always needs a human.
HIGH_RISK_THRESHOLD = 100
MEDIUM_RISK_THRESHOLD = 30

_FILE_RE = re.compile(r"^\+\+\+ b/(?P<path>\S+)", re.MULTILINE)
_ADDED_RE = re.compile(r"^\+(?!\+\+ )", re.MULTILINE)
_REMOVED_RE = re.compile(r"^-(?!-- )", re.MULTILINE)
_SECRETY_RE = re.compile(
    r"(?i)^\+.*\b(?:secret|password|api[_-]?key|token|private[_-]key)\b\s*[:=]",
    re.MULTILINE,
)


@dataclass(frozen=True)
class Diff:
    """A unified diff, parsed just enough for the rules below."""

    text: str

    @property
    def files(self) -> list[str]:
        return _FILE_RE.findall(self.text)

    @property
    def added_lines(self) -> int:
        return len(_ADDED_RE.findall(self.text))

    @property
    def removed_lines(self) -> int:
        return len(_REMOVED_RE.findall(self.text))

    @property
    def changed_lines(self) -> int:
        return self.added_lines + self.removed_lines

    @property
    def touches_tests(self) -> bool:
        return any(
            "tests/" in path or path.rsplit("/", 1)[-1].startswith("test_")
            for path in self.files
        )


@dataclass(frozen=True)
class Rule:
    """A named, deterministic check over a diff."""

    name: str
    weight: int
    description: str
    predicate: Callable[[Diff], bool]

    def evaluate(self, diff: Diff) -> bool:
        return self.predicate(diff)


def _path_rule(
    name: str, weight: int, description: str, patterns: Sequence[str]
) -> Rule:
    compiled = [re.compile(pattern) for pattern in patterns]
    return Rule(
        name=name,
        weight=weight,
        description=description,
        predicate=lambda diff: any(
            regex.search(path) for path in diff.files for regex in compiled
        ),
    )


#: Superset-specific defaults. Weights >= HIGH_RISK_THRESHOLD are hard vetoes.
DEFAULT_RULES: tuple[Rule, ...] = (
    _path_rule(
        "db_migration",
        HIGH_RISK_THRESHOLD,
        "touches an Alembic migration",
        [r"^superset/migrations/"],
    ),
    _path_rule(
        "security_surface",
        HIGH_RISK_THRESHOLD,
        "touches authn/authz or row level security",
        [
            r"^superset/security/",
            r"^superset/.*guest_token",
            r"^superset/row_level_security/",
            r"^superset/extensions/.*jwt",
        ],
    ),
    _path_rule(
        "ci_or_release",
        HIGH_RISK_THRESHOLD,
        "touches CI, release or container build config",
        [r"^\.github/", r"^Dockerfile", r"^docker/", r"^RELEASING/", r"^helm/"],
    ),
    _path_rule(
        "dependencies",
        HIGH_RISK_THRESHOLD,
        "changes dependencies or packaging",
        [
            r"^requirements/",
            r"^setup\.py$",
            r"^pyproject\.toml$",
            r"package(-lock)?\.json$",
        ],
    ),
    _path_rule(
        "global_config",
        60,
        "changes global configuration or feature flags",
        [r"^superset/config\.py$", r"^superset/app\.py$", r"^superset/initialization/"],
    ),
    _path_rule(
        "public_api",
        40,
        "changes a REST API surface",
        [r"^superset/.*/api\.py$", r"^superset/views/"],
    ),
    Rule(
        name="secret_material",
        weight=HIGH_RISK_THRESHOLD,
        description="adds something that looks like a credential",
        predicate=lambda diff: bool(_SECRETY_RE.search(diff.text)),
    ),
    Rule(
        name="large_diff",
        weight=50,
        description="over 300 changed lines",
        predicate=lambda diff: diff.changed_lines > 300,
    ),
    Rule(
        name="medium_diff",
        weight=20,
        description="over 100 changed lines",
        predicate=lambda diff: 100 < diff.changed_lines <= 300,
    ),
    Rule(
        name="broad_blast_radius",
        weight=25,
        description="touches more than 5 files",
        predicate=lambda diff: len(diff.files) > 5,
    ),
    Rule(
        name="deletes_more_than_it_adds",
        weight=15,
        description="removes substantially more code than it adds",
        predicate=lambda diff: diff.removed_lines > 2 * max(diff.added_lines, 1),
    ),
    Rule(
        name="no_test_coverage",
        weight=HIGH_RISK_THRESHOLD,
        description="ships no test change alongside the fix",
        predicate=lambda diff: not diff.touches_tests,
    ),
)


@dataclass
class RiskRegistry:
    """Evaluates a diff against the rule set and returns a tier."""

    rules: Sequence[Rule] = field(default_factory=lambda: DEFAULT_RULES)
    high_threshold: int = HIGH_RISK_THRESHOLD
    medium_threshold: int = MEDIUM_RISK_THRESHOLD

    def assess(self, diff_text: str) -> RiskAssessment:
        diff = Diff(diff_text)
        if not diff.text.strip():
            return RiskAssessment(
                tier=RiskTier.HIGH,
                score=self.high_threshold,
                reasons=["empty diff"],
                triggered_rules=["empty_diff"],
            )
        score = 0
        reasons: list[str] = []
        triggered: list[str] = []
        for rule in self.rules:
            if rule.evaluate(diff):
                score += rule.weight
                triggered.append(rule.name)
                reasons.append(f"{rule.name}: {rule.description}")
        if score >= self.high_threshold:
            tier = RiskTier.HIGH
        elif score >= self.medium_threshold:
            tier = RiskTier.MEDIUM
        else:
            tier = RiskTier.LOW
        return RiskAssessment(
            tier=tier, score=score, reasons=reasons, triggered_rules=triggered
        )
