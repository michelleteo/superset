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

"""Remediation lane: reproduce the bug, then propose a fix plus a test."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

from error_orchestrator.devin_client import DevinClient, DevinError
from error_orchestrator.models import ErrorEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RemediationRequest:
    pool_id: str
    title: str
    signature: str
    occurrences: int
    affected_users: int
    priority: float
    samples: tuple[ErrorEvent, ...] = ()
    repo: str = ""
    base_branch: str = "master"


@dataclass
class RemediationOutcome:
    pool_id: str
    reproduced: bool
    diff: str = ""
    summary: str = ""
    test_added: bool = False
    branch: str | None = None
    session_url: str | None = None
    reason: str = ""
    reasons: list[str] = field(default_factory=list)


def build_remediation_prompt(request: RemediationRequest) -> str:
    samples = "\n\n".join(
        f"  occurrence {index + 1} @ {sample.timestamp}\n"
        f"  {sample.level} {sample.logger} "
        f"{sample.module}.{sample.func}:{sample.line}\n"
        f"  {sample.message}\n"
        f"{sample.traceback or '  (no traceback)'}"
        for index, sample in enumerate(request.samples)
    )
    return f"""Reproduce and fix a production error in {request.repo}
(base branch: {request.base_branch}).

Category: {request.title}
Seen {request.occurrences} time(s), affecting {request.affected_users} user(s).

Canonical signature:
{request.signature}

Sample occurrences:
{samples or "  (none captured)"}

Steps:
1. Trace the failure to the responsible code path.
2. Write a FAILING regression test that reproduces it. If you cannot make the
   error happen, stop and report could_not_reproduce with what you ruled out.
3. Fix the bug with the smallest correct change and make the test pass.
4. Do NOT open a pull request or merge anything — a separate risk-check stage
   owns that decision. Report the diff instead.

Return structured output:
{{
  "reproduced": true | false,
  "diff": "<unified diff of fix + test, empty when not reproduced>",
  "summary": "<what was wrong and how the fix addresses it>",
  "test_added": true | false,
  "branch": "<branch name if you pushed one, else null>",
  "reason": "<why it could not be reproduced, when applicable>"
}}
"""


def parse_remediation_output(
    output: Mapping[str, Any],
    request: RemediationRequest,
    session_url: str | None = None,
) -> RemediationOutcome:
    diff = str(output.get("diff") or "")
    reproduced = bool(output.get("reproduced")) and bool(diff.strip())
    reason = str(output.get("reason") or "")
    if bool(output.get("reproduced")) and not diff.strip():
        reason = reason or "session reported a fix but returned an empty diff"
    return RemediationOutcome(
        pool_id=request.pool_id,
        reproduced=reproduced,
        diff=diff,
        summary=str(output.get("summary") or ""),
        test_added=bool(output.get("test_added")),
        branch=(str(output["branch"]) if output.get("branch") else None),
        session_url=session_url,
        reason=reason,
    )


async def remediate_handler(
    request: RemediationRequest, devin: DevinClient
) -> RemediationOutcome:
    """Pure: attempt to reproduce and fix one category. Never raises."""
    try:
        result = await devin.run_session(
            build_remediation_prompt(request),
            title=f"Remediate: {request.title[:80]}",
            tags=["error-orchestrator", "remediate"],
        )
    except DevinError as error:
        logger.exception("remediation session failed for %s", request.pool_id)
        return RemediationOutcome(
            pool_id=request.pool_id,
            reproduced=False,
            reason=f"remediation session failed: {error}",
        )
    return parse_remediation_output(result.structured_output, request, result.url)
