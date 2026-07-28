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

"""Risk-check lane: registry rules first, independent Devin review second.

A High tier from the registry is a hard veto — the diff goes straight to
``awaiting_review`` without spending a session. Anything else gets an
independent Devin review, and only an unambiguous approval auto-merges.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from error_orchestrator.devin_client import DevinClient, DevinError
from error_orchestrator.models import ProposedFix, RiskAssessment, RiskTier
from error_orchestrator.risk_registry import RiskRegistry

logger = logging.getLogger(__name__)


class ReviewOutcome(str, Enum):
    AUTO_MERGE = "auto_merge"
    AWAITING_REVIEW = "awaiting_review"


@dataclass(frozen=True)
class RiskCheckRequest:
    pool_id: str
    title: str
    fix: ProposedFix
    signature: str = ""
    repo: str = ""


@dataclass
class RiskDecision:
    pool_id: str
    outcome: ReviewOutcome
    assessment: RiskAssessment
    #: Which stage decided: "registry" or "devin_review".
    decided_by: str = "registry"
    review_session_url: str | None = None
    reasons: list[str] = field(default_factory=list)

    @property
    def tier(self) -> RiskTier:
        return self.assessment.tier


def build_review_prompt(request: RiskCheckRequest, assessment: RiskAssessment) -> str:
    return f"""Independently review an auto-generated fix for {request.repo}
before it is merged without a human.

Category: {request.title}
Canonical signature:
{request.signature}

Fix summary from the remediation session:
{request.fix.summary or "(none provided)"}

Static risk assessment: tier={assessment.tier.value} score={assessment.score}
{chr(10).join("- " + reason for reason in assessment.reasons) or "- no rules triggered"}

Diff under review:
```diff
{request.fix.diff}
```

Verify independently: does the fix actually address the root cause, is the
test meaningful (does it fail without the fix?), and does it introduce
regressions, behaviour changes or security issues? When in doubt, do not
approve.

Return structured output:
{{
  "approved": true | false,
  "confidence": <float 0-1>,
  "concerns": ["<blocking concern>"],
  "summary": "<verdict in 1-3 sentences>"
}}
"""


def parse_review_output(output: Mapping[str, Any]) -> tuple[bool, list[str]]:
    approved = bool(output.get("approved"))
    concerns = [str(concern) for concern in output.get("concerns", [])]
    if summary := str(output.get("summary") or ""):
        concerns.append(f"review summary: {summary}")
    return approved and not output.get("concerns"), concerns


async def risk_check_handler(
    request: RiskCheckRequest,
    devin: DevinClient,
    registry: RiskRegistry,
) -> RiskDecision:
    """Pure: decide auto-merge vs. human review for one proposed fix."""
    assessment = registry.assess(request.fix.diff)
    if assessment.tier is RiskTier.HIGH:
        return RiskDecision(
            pool_id=request.pool_id,
            outcome=ReviewOutcome.AWAITING_REVIEW,
            assessment=assessment,
            decided_by="registry",
            reasons=assessment.reasons,
        )
    try:
        result = await devin.run_session(
            build_review_prompt(request, assessment),
            title=f"Risk review: {request.title[:80]}",
            tags=["error-orchestrator", "risk-check"],
        )
    except DevinError as error:
        logger.exception("review session failed for %s", request.pool_id)
        return RiskDecision(
            pool_id=request.pool_id,
            outcome=ReviewOutcome.AWAITING_REVIEW,
            assessment=assessment,
            decided_by="devin_review",
            reasons=[*assessment.reasons, f"review session failed: {error}"],
        )
    approved, concerns = parse_review_output(result.structured_output)
    return RiskDecision(
        pool_id=request.pool_id,
        outcome=ReviewOutcome.AUTO_MERGE if approved else ReviewOutcome.AWAITING_REVIEW,
        assessment=assessment,
        decided_by="devin_review",
        review_session_url=result.url,
        reasons=[*assessment.reasons, *concerns],
    )
