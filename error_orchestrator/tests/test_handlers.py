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

from typing import Any, Mapping

from error_orchestrator.devin_client import DevinError, ScriptedDevinClient
from error_orchestrator.fingerprint import canonicalize
from error_orchestrator.handlers import (
    remediate_handler,
    RemediationRequest,
    ReviewOutcome,
    risk_check_handler,
    RiskCheckRequest,
    triage_handler,
    TriageAction,
    TriageRequest,
)
from error_orchestrator.handlers.triage import MergeCandidate
from error_orchestrator.models import ProposedFix, RiskTier
from error_orchestrator.risk_registry import RiskRegistry
from error_orchestrator.simulation import RISKY_DIFF, SAFE_DIFF
from error_orchestrator.tests.conftest import make_event

CANDIDATE = MergeCandidate(
    pool_id="pool-1", title="Dataset missing", signature="exc=X", occurrences=4
)


def _triage_request() -> TriageRequest:
    event = make_event()
    return TriageRequest(
        fingerprint="fp-new",
        canonical=canonicalize(event),
        sample=event,
        candidates=(CANDIDATE,),
        repo="michelleteo/superset",
    )


def _client(output: Mapping[str, Any]) -> ScriptedDevinClient:
    return ScriptedDevinClient(responder=lambda _: output)


class _FailingDevinClient:
    async def run_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        tags: list[str] | None = None,
        idempotent: bool = True,
    ) -> Any:
        raise DevinError("boom")


async def test_triage_merges_into_a_known_category() -> None:
    decision = await triage_handler(
        _triage_request(),
        _client({"action": "merge", "pool_id": "pool-1", "confidence": 0.95}),
    )
    assert decision.action is TriageAction.MERGE
    assert decision.pool_id == "pool-1"


async def test_triage_rejects_low_confidence_or_unknown_pool_merges() -> None:
    low_confidence = await triage_handler(
        _triage_request(),
        _client({"action": "merge", "pool_id": "pool-1", "confidence": 0.2}),
    )
    unknown_pool = await triage_handler(
        _triage_request(),
        _client({"action": "merge", "pool_id": "nope", "confidence": 0.99}),
    )
    assert low_confidence.action is TriageAction.NEW_CATEGORY
    assert unknown_pool.action is TriageAction.NEW_CATEGORY


async def test_triage_skips_the_session_when_there_is_nothing_to_merge_into() -> None:
    request = TriageRequest(
        fingerprint="fp",
        canonical=canonicalize(make_event()),
        sample=make_event(),
        candidates=(),
    )
    client = _client({"action": "merge", "pool_id": "pool-1", "confidence": 1.0})
    decision = await triage_handler(request, client)
    assert decision.action is TriageAction.NEW_CATEGORY
    assert client.prompts == []


async def test_triage_falls_back_to_new_category_when_devin_fails() -> None:
    decision = await triage_handler(_triage_request(), _FailingDevinClient())
    assert decision.action is TriageAction.NEW_CATEGORY


def _remediation_request() -> RemediationRequest:
    return RemediationRequest(
        pool_id="pool-1",
        title="Dataset missing",
        signature="exc=SupersetException",
        occurrences=12,
        affected_users=3,
        priority=4.2,
        samples=(make_event(),),
        repo="michelleteo/superset",
    )


async def test_remediation_returns_a_diff_when_reproduced() -> None:
    outcome = await remediate_handler(
        _remediation_request(),
        _client(
            {
                "reproduced": True,
                "diff": SAFE_DIFF,
                "summary": "guard empty input",
                "test_added": True,
            }
        ),
    )
    assert outcome.reproduced is True
    assert outcome.test_added is True
    assert outcome.diff == SAFE_DIFF


async def test_remediation_reports_could_not_reproduce() -> None:
    outcome = await remediate_handler(
        _remediation_request(),
        _client({"reproduced": False, "reason": "needs prod data"}),
    )
    assert outcome.reproduced is False
    assert outcome.reason == "needs prod data"


async def test_remediation_treats_an_empty_diff_as_not_reproduced() -> None:
    outcome = await remediate_handler(
        _remediation_request(), _client({"reproduced": True, "diff": "  "})
    )
    assert outcome.reproduced is False
    assert "empty diff" in outcome.reason


def _risk_request(diff: str) -> RiskCheckRequest:
    return RiskCheckRequest(
        pool_id="pool-1",
        title="Dataset missing",
        fix=ProposedFix(diff=diff, summary="fix", test_added=True),
        repo="michelleteo/superset",
    )


async def test_high_risk_diffs_never_reach_a_review_session() -> None:
    client = _client({"approved": True})
    decision = await risk_check_handler(
        _risk_request(RISKY_DIFF), client, RiskRegistry()
    )
    assert decision.outcome is ReviewOutcome.AWAITING_REVIEW
    assert decision.decided_by == "registry"
    assert decision.tier is RiskTier.HIGH
    assert client.prompts == []


async def test_low_risk_diff_auto_merges_when_the_review_approves() -> None:
    decision = await risk_check_handler(
        _risk_request(SAFE_DIFF),
        _client({"approved": True, "confidence": 0.9, "concerns": []}),
        RiskRegistry(),
    )
    assert decision.outcome is ReviewOutcome.AUTO_MERGE
    assert decision.decided_by == "devin_review"


async def test_review_concerns_send_the_fix_to_a_human() -> None:
    decision = await risk_check_handler(
        _risk_request(SAFE_DIFF),
        _client({"approved": True, "concerns": ["test does not fail without the fix"]}),
        RiskRegistry(),
    )
    assert decision.outcome is ReviewOutcome.AWAITING_REVIEW
    assert any("test does not fail" in reason for reason in decision.reasons)


async def test_review_failure_falls_back_to_human_review() -> None:
    decision = await risk_check_handler(
        _risk_request(SAFE_DIFF), _FailingDevinClient(), RiskRegistry()
    )
    assert decision.outcome is ReviewOutcome.AWAITING_REVIEW
