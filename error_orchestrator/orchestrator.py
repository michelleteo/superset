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

"""The orchestrator: owns the state machine, the queues and the lanes.

Handlers are pure — they take a request and return a decision. Everything that
mutates state (pool creation, transitions, priority updates, capacity limits)
lives here, so there is a single place to reason about correctness.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.devin_client import DevinClient, HttpDevinClient
from error_orchestrator.handlers import (
    remediate_handler,
    RemediationOutcome,
    RemediationRequest,
    ReviewOutcome,
    risk_check_handler,
    RiskCheckRequest,
    RiskDecision,
    triage_handler,
    TriageAction,
    TriageDecision,
    TriageRequest,
)
from error_orchestrator.handlers.triage import MergeCandidate
from error_orchestrator.lanes import Lane, QueueSource
from error_orchestrator.models import ErrorEvent, ErrorPool, PoolState, ProposedFix
from error_orchestrator.priority import compute_priority
from error_orchestrator.risk_registry import RiskRegistry
from error_orchestrator.state_machine import transition
from error_orchestrator.store import IngestOutcome, IngestResult, PoolStore

logger = logging.getLogger(__name__)

MergeCallback = Callable[[ErrorPool, RiskDecision], Awaitable[None]]


@dataclass
class DecisionRecord:
    """Audit trail entry; the observability layer will read these."""

    at: float
    stage: str
    pool_id: str
    detail: dict[str, Any] = field(default_factory=dict)


class Orchestrator:
    """Wires ingest -> triage -> remediate -> risk check."""

    def __init__(
        self,
        config: OrchestratorConfig | None = None,
        devin: DevinClient | None = None,
        store: PoolStore | None = None,
        registry: RiskRegistry | None = None,
        merge_callback: MergeCallback | None = None,
    ) -> None:
        self.config = config or OrchestratorConfig()
        self.store = store or PoolStore(weights=self.config.weights)
        self.registry = registry or RiskRegistry()
        self.devin = devin or self._default_devin_client()
        self.decisions: list[DecisionRecord] = []
        self._merge_callback = merge_callback or self._default_merge_callback

        self.ingest_queue: asyncio.Queue[ErrorEvent] = asyncio.Queue(
            maxsize=self.config.ingest_queue_size
        )
        self.triage_source: QueueSource[IngestResult] = QueueSource()
        self.risk_source: QueueSource[str] = QueueSource()

        self.triage_lane: Lane[IngestResult] = Lane(
            "triage",
            self.config.triage_workers,
            self.triage_source,
            self._run_triage,
        )
        self.remediation_lane: Lane[ErrorPool] = Lane(
            "remediate",
            self.config.remediation_workers,
            self._next_remediation,
            self._run_remediation,
        )
        self.risk_lane: Lane[str] = Lane(
            "risk_check",
            self.config.risk_check_workers,
            self.risk_source,
            self._run_risk_check,
        )
        self._ingest_task: asyncio.Task[None] | None = None
        self._dropped_events = 0

    def _default_devin_client(self) -> DevinClient:
        return HttpDevinClient(
            api_key=self.config.devin_api_key or "",
            api_base=self.config.devin_api_base,
            poll_interval=self.config.devin_poll_interval,
            timeout=self.config.devin_session_timeout,
        )

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._ingest_task is None:
            self._ingest_task = asyncio.create_task(self._ingest_loop(), name="ingest")
        self.triage_lane.start()
        self.remediation_lane.start()
        self.risk_lane.start()
        logger.info(
            "orchestrator started (triage=%s remediate=%s risk=%s)",
            self.config.triage_workers,
            self.config.remediation_workers,
            self.config.risk_check_workers,
        )

    async def stop(self) -> None:
        if self._ingest_task is not None:
            self._ingest_task.cancel()
            try:
                await self._ingest_task
            except asyncio.CancelledError:
                pass
            self._ingest_task = None
        await asyncio.gather(
            self.triage_lane.stop(),
            self.remediation_lane.stop(),
            self.risk_lane.stop(),
        )

    # ---------------------------------------------------------------- ingest

    def submit(self, event: ErrorEvent) -> bool:
        """Non-blocking enqueue; drops (and counts) when the buffer is full."""
        try:
            self.ingest_queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped_events += 1
            logger.warning("ingest queue full; dropped event %s", event.event_id)
            return False
        return True

    async def _ingest_loop(self) -> None:
        while True:
            event = await self.ingest_queue.get()
            try:
                await self.handle_event(event)
            except Exception:  # pylint: disable=broad-except
                logger.exception("ingest failed for event %s", event.event_id)
            finally:
                self.ingest_queue.task_done()

    async def handle_event(self, event: ErrorEvent) -> IngestResult:
        """Normalize + hash the event and route it. Cheap and synchronous."""
        result = self.store.record_event(event)
        if result.outcome is IngestOutcome.EXISTING_POOL and result.pool is not None:
            # A hotter pool may now outrank whatever remediation is parked on.
            self.remediation_lane.notify()
        elif result.outcome is IngestOutcome.NEEDS_TRIAGE:
            await self.triage_source.put(result)
        return result

    # ---------------------------------------------------------------- triage

    def build_triage_request(self, result: IngestResult) -> TriageRequest:
        candidates = tuple(
            MergeCandidate(
                pool_id=pool.pool_id,
                title=pool.title,
                signature=pool.signature,
                occurrences=pool.occurrences,
            )
            for pool in self.store.candidates_for_merge(
                self.config.merge_candidate_limit
            )
        )
        return TriageRequest(
            fingerprint=result.fingerprint,
            canonical=result.canonical,
            sample=result.event,
            candidates=candidates,
            repo=self.config.repo,
        )

    async def _run_triage(self, result: IngestResult) -> None:
        request = self.build_triage_request(result)
        decision = await triage_handler(request, self.devin)
        self.apply_triage(result, decision)

    def apply_triage(self, result: IngestResult, decision: TriageDecision) -> ErrorPool:
        """Fold a triage decision into the store; a new category becomes triaged."""
        if decision.action is TriageAction.MERGE and decision.pool_id:
            pool = self.store.merge_fingerprint(
                decision.pool_id,
                result.fingerprint,
                reason=f"triage merge: {decision.summary}",
            )
            self._record("triage", pool, {"action": "merge", "merged": True})
        else:
            pool = self.store.create_pool(
                result.fingerprint,
                result.canonical,
                title=decision.title or result.canonical.title,
                session_url=decision.session_url,
            )
            self._record("triage", pool, {"action": "new_category"})
        self.remediation_lane.notify()
        return pool

    # ----------------------------------------------------------- remediation

    async def _next_remediation(self) -> ErrorPool | None:
        """Pull the highest-priority triaged pool; ``None`` parks the worker."""
        return self.store.next_pool_for_remediation()

    def build_remediation_request(self, pool: ErrorPool) -> RemediationRequest:
        return RemediationRequest(
            pool_id=pool.pool_id,
            title=pool.title,
            signature=pool.signature,
            occurrences=pool.occurrences,
            affected_users=len(pool.affected_users),
            priority=pool.priority,
            samples=tuple(pool.samples),
            repo=self.config.repo,
            base_branch=self.config.base_branch,
        )

    async def _run_remediation(self, pool: ErrorPool) -> None:
        outcome = await remediate_handler(
            self.build_remediation_request(pool), self.devin
        )
        await self.apply_remediation(pool, outcome)

    async def apply_remediation(
        self, pool: ErrorPool, outcome: RemediationOutcome
    ) -> None:
        pool.claimed = False
        if not outcome.reproduced:
            transition(
                pool,
                PoolState.COULD_NOT_REPRODUCE,
                reason=outcome.reason or "could not reproduce",
            )
            self.store.heap.remove(pool.pool_id)
            self._record(
                "remediate",
                pool,
                {
                    "reproduced": False,
                    "reason": outcome.reason,
                    "session_url": outcome.session_url,
                },
            )
            return
        pool.proposed_fix = ProposedFix(
            diff=outcome.diff,
            summary=outcome.summary,
            test_added=outcome.test_added,
            session_url=outcome.session_url,
            branch=outcome.branch,
        )
        transition(pool, PoolState.FIX_PROPOSED, reason=outcome.summary)
        self._record(
            "remediate",
            pool,
            {"reproduced": True, "session_url": outcome.session_url},
        )
        await self.risk_source.put(pool.pool_id)
        # A remediation slot just freed up: pull the next highest priority pool.
        self.remediation_lane.notify()

    # ------------------------------------------------------------ risk check

    def build_risk_request(self, pool: ErrorPool) -> RiskCheckRequest:
        if pool.proposed_fix is None:
            raise ValueError(f"pool {pool.pool_id} has no proposed fix")
        return RiskCheckRequest(
            pool_id=pool.pool_id,
            title=pool.title,
            fix=pool.proposed_fix,
            signature=pool.signature,
            repo=self.config.repo,
        )

    async def _run_risk_check(self, pool_id: str) -> None:
        pool = self.store.get(pool_id)
        if pool is None or pool.state is not PoolState.FIX_PROPOSED:
            logger.warning("risk check skipped for unknown or stale pool %s", pool_id)
            return
        decision = await risk_check_handler(
            self.build_risk_request(pool), self.devin, self.registry
        )
        await self.apply_risk_decision(pool, decision)

    async def apply_risk_decision(
        self, pool: ErrorPool, decision: RiskDecision
    ) -> None:
        pool.risk = decision.assessment
        if decision.outcome is ReviewOutcome.AUTO_MERGE:
            transition(pool, PoolState.AUTO_MERGED, reason="risk check: auto-merged")
            pool.merged_at = time.time()
            await self._merge_callback(pool, decision)
        else:
            pool.review_reason = "; ".join(decision.reasons) or "flagged for review"
            transition(pool, PoolState.AWAITING_REVIEW, reason=pool.review_reason)
        self._record(
            "risk_check",
            pool,
            {
                "tier": decision.tier.value,
                "score": decision.assessment.score,
                "decided_by": decision.decided_by,
                "outcome": decision.outcome.value,
                "triggered_rules": decision.assessment.triggered_rules,
                "review_session_url": decision.review_session_url,
            },
        )

    async def _default_merge_callback(
        self, pool: ErrorPool, decision: RiskDecision
    ) -> None:
        if not self.config.auto_merge_enabled:
            logger.info(
                "auto-merge disabled; pool %s would merge (tier=%s)",
                pool.pool_id,
                decision.tier.value,
            )
            return
        logger.info(
            "auto-merging pool %s (tier=%s, branch=%s)",
            pool.pool_id,
            decision.tier.value,
            pool.proposed_fix.branch if pool.proposed_fix else None,
        )

    # --------------------------------------------------------------- helpers

    def _record(self, stage: str, pool: ErrorPool, detail: dict[str, Any]) -> None:
        self.decisions.append(
            DecisionRecord(
                at=time.time(),
                stage=stage,
                pool_id=pool.pool_id,
                detail={"state": pool.state.value, **detail},
            )
        )

    def stats(self) -> dict[str, Any]:
        """Snapshot for the (future) observability layer."""
        return {
            "queues": {
                "ingest": self.ingest_queue.qsize(),
                "triage": self.triage_source.qsize(),
                "risk_check": self.risk_source.qsize(),
                "heap": len(self.store.heap),
                "pending_triage": len(self.store.pending),
                "dropped_events": self._dropped_events,
            },
            "lanes": {
                lane.name: lane.stats.as_dict()
                for lane in (self.triage_lane, self.remediation_lane, self.risk_lane)
            },
            "pools": self.store.snapshot(),
        }

    def pool_view(self, pool: ErrorPool) -> dict[str, Any]:
        return {
            "pool_id": pool.pool_id,
            "title": pool.title,
            "state": pool.state.value,
            "fingerprint": pool.fingerprint,
            "merged_fingerprints": sorted(pool.merged_fingerprints),
            "occurrences": pool.occurrences,
            "affected_users": len(pool.affected_users),
            "priority": round(compute_priority(pool, self.config.weights), 4),
            "first_seen": pool.first_seen,
            "last_seen": pool.last_seen,
            "risk": (
                {**asdict(pool.risk), "tier": pool.risk.tier.value}
                if pool.risk
                else None
            ),
            "review_reason": pool.review_reason,
            "triage_session_url": pool.triage_session_url,
            "fix_session_url": (
                pool.proposed_fix.session_url if pool.proposed_fix else None
            ),
        }

    async def drain(self, timeout: float = 10.0) -> bool:
        """Wait until every lane is idle. Intended for tests and shutdown."""
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            idle = (
                self.ingest_queue.empty()
                and self.triage_source.qsize() == 0
                and self.risk_source.qsize() == 0
                and len(self.store.heap) == 0
                and not self.store.pending
                and all(
                    lane.stats.in_flight == 0
                    for lane in (
                        self.triage_lane,
                        self.remediation_lane,
                        self.risk_lane,
                    )
                )
            )
            if idle:
                return True
            await asyncio.sleep(0.01)
        return False


__all__ = ["DecisionRecord", "Orchestrator"]
