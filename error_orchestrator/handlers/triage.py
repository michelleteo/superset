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

"""Triage lane: is this unknown fingerprint a new bug, or an old one in disguise?

The fingerprint hash already handles identical traces in O(1); this handler is
only invoked for fingerprints that matched nothing, to decide whether the same
underlying bug is manifesting through a different code path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from error_orchestrator.devin_client import DevinClient, DevinError
from error_orchestrator.fingerprint import CanonicalError
from error_orchestrator.models import ErrorEvent

logger = logging.getLogger(__name__)

MIN_MERGE_CONFIDENCE = 0.7


class TriageAction(str, Enum):
    MERGE = "merge"
    NEW_CATEGORY = "new_category"


@dataclass(frozen=True)
class MergeCandidate:
    pool_id: str
    title: str
    signature: str
    occurrences: int


@dataclass(frozen=True)
class TriageRequest:
    fingerprint: str
    canonical: CanonicalError
    sample: ErrorEvent
    candidates: tuple[MergeCandidate, ...] = ()
    repo: str = ""


@dataclass
class TriageDecision:
    action: TriageAction
    fingerprint: str
    pool_id: str | None = None
    title: str = ""
    summary: str = ""
    confidence: float = 0.0
    session_url: str | None = None
    reasons: list[str] = field(default_factory=list)


def build_triage_prompt(request: TriageRequest) -> str:
    candidates = "\n".join(
        f"- pool_id={candidate.pool_id} occurrences={candidate.occurrences}\n"
        f"  title: {candidate.title}\n"
        f"  signature: {candidate.signature.replace(chr(10), ' / ')}"
        for candidate in request.candidates
    )
    sample = request.sample
    return f"""You are triaging a production error for the repo {request.repo}.

A new error fingerprint appeared that does not match any known category by
hash. Decide whether it is the SAME underlying bug as one of the existing
categories (the same root cause reaching us through a different code path) or
a genuinely NEW bug.

Canonical form of the new error:
{request.canonical.render()}

Latest raw occurrence:
  level: {sample.level}
  logger: {sample.logger}
  location: {sample.module}.{sample.func}:{sample.line}
  message: {sample.message}
  traceback:
{sample.traceback or "  (none)"}

Existing categories:
{candidates or "- (none)"}

Read the relevant code to check whether the failure originates from the same
root cause as a candidate. Do not guess from the message text alone.

Finish by replying with ONE fenced ```json block and nothing else:
{{
  "action": "merge" | "new_category",
  "pool_id": "<required when action is merge>",
  "title": "<short human title, required when action is new_category>",
  "summary": "<1-3 sentences on the root cause>",
  "confidence": <float 0-1>,
  "reasons": ["<why>"]
}}
"""


def parse_triage_output(
    output: Mapping[str, Any],
    request: TriageRequest,
    session_url: str | None = None,
) -> TriageDecision:
    """Turn Devin's structured output into a decision, defaulting to NEW."""
    action_raw = str(output.get("action", "")).strip().lower()
    confidence = _as_float(output.get("confidence"), 0.0)
    known_ids = {candidate.pool_id for candidate in request.candidates}
    pool_id = output.get("pool_id")
    reasons = [str(reason) for reason in output.get("reasons", [])]
    summary = str(output.get("summary", ""))

    merge_valid = (
        action_raw == TriageAction.MERGE.value
        and isinstance(pool_id, str)
        and pool_id in known_ids
        and confidence >= MIN_MERGE_CONFIDENCE
    )
    if merge_valid:
        return TriageDecision(
            action=TriageAction.MERGE,
            fingerprint=request.fingerprint,
            pool_id=str(pool_id),
            summary=summary,
            confidence=confidence,
            session_url=session_url,
            reasons=reasons,
        )
    if action_raw == TriageAction.MERGE.value:
        reasons.append(
            "merge rejected: unknown pool_id or confidence below "
            f"{MIN_MERGE_CONFIDENCE}"
        )
    return TriageDecision(
        action=TriageAction.NEW_CATEGORY,
        fingerprint=request.fingerprint,
        title=str(output.get("title") or request.canonical.title),
        summary=summary,
        confidence=confidence,
        session_url=session_url,
        reasons=reasons,
    )


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def triage_handler(request: TriageRequest, devin: DevinClient) -> TriageDecision:
    """Pure: classify an unknown fingerprint. Never raises."""
    if not request.candidates:
        # Nothing to merge into — no need to spend a session.
        return TriageDecision(
            action=TriageAction.NEW_CATEGORY,
            fingerprint=request.fingerprint,
            title=request.canonical.title,
            confidence=1.0,
            reasons=["no existing categories to compare against"],
        )
    try:
        result = await devin.run_session(
            build_triage_prompt(request),
            title=f"Triage: {request.canonical.title[:80]}",
            tags=["error-orchestrator", "triage"],
        )
    except DevinError:
        logger.exception("triage session failed for %s", request.fingerprint)
        return TriageDecision(
            action=TriageAction.NEW_CATEGORY,
            fingerprint=request.fingerprint,
            title=request.canonical.title,
            reasons=["triage session failed; treating as a new category"],
        )
    return parse_triage_output(result.structured_output, request, result.url)
