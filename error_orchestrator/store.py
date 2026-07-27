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

"""In-memory state owned by the orchestrator.

The store is deliberately synchronous: every method completes without
awaiting, so on the orchestrator's single event loop each call is atomic and
no locking is required. Swapping it for a persistent backend later only means
re-implementing this class.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from error_orchestrator.fingerprint import CanonicalError, canonicalize
from error_orchestrator.models import (
    ErrorEvent,
    ErrorPool,
    PoolState,
    StateTransition,
)
from error_orchestrator.priority import compute_priority, PriorityHeap, PriorityWeights
from error_orchestrator.state_machine import transition

#: Pools in this state are candidates for the remediation lane.
REMEDIATION_READY = PoolState.TRIAGED


class IngestOutcome(str, Enum):
    #: Fingerprint already known — O(1) dedup, no Devin call needed.
    EXISTING_POOL = "existing_pool"
    #: Unknown fingerprint — needs a Devin triage session.
    NEEDS_TRIAGE = "needs_triage"
    #: Unknown fingerprint already being triaged; event was buffered.
    TRIAGE_IN_FLIGHT = "triage_in_flight"


@dataclass
class IngestResult:
    outcome: IngestOutcome
    fingerprint: str
    canonical: CanonicalError
    event: ErrorEvent
    pool: ErrorPool | None = None


@dataclass
class PoolStore:
    weights: PriorityWeights = field(default_factory=PriorityWeights)
    pools: dict[str, ErrorPool] = field(default_factory=dict)
    #: fingerprint -> pool_id (includes fingerprints merged in by triage).
    index: dict[str, str] = field(default_factory=dict)
    heap: PriorityHeap = field(default_factory=PriorityHeap)
    #: Events that arrived while their fingerprint was being triaged.
    pending: dict[str, list[ErrorEvent]] = field(
        default_factory=lambda: defaultdict(list)
    )

    # ---------------------------------------------------------------- ingest

    def record_event(self, event: ErrorEvent) -> IngestResult:
        """Route one occurrence: existing pool, buffered, or needs triage."""
        canonical = canonicalize(event)
        fingerprint = canonical.fingerprint
        if (pool_id := self.index.get(fingerprint)) is not None:
            pool = self.pools[pool_id]
            pool.add_event(event)
            self.reprioritize(pool)
            return IngestResult(
                IngestOutcome.EXISTING_POOL, fingerprint, canonical, event, pool
            )
        if fingerprint in self.pending:
            self.pending[fingerprint].append(event)
            return IngestResult(
                IngestOutcome.TRIAGE_IN_FLIGHT, fingerprint, canonical, event
            )
        self.pending[fingerprint] = [event]
        return IngestResult(IngestOutcome.NEEDS_TRIAGE, fingerprint, canonical, event)

    def _drain_pending(self, fingerprint: str) -> list[ErrorEvent]:
        return self.pending.pop(fingerprint, [])

    # ----------------------------------------------------------- triage sink

    def create_pool(
        self,
        fingerprint: str,
        canonical: CanonicalError,
        title: str | None = None,
        session_url: str | None = None,
    ) -> ErrorPool:
        """Register a brand new category and mark it ready for remediation."""
        pool = ErrorPool(
            fingerprint=fingerprint,
            title=title or canonical.title,
            signature=canonical.render(),
        )
        pool.first_seen = time.time()
        pool.triage_session_url = session_url
        self.pools[pool.pool_id] = pool
        self.index[fingerprint] = pool.pool_id
        for event in self._drain_pending(fingerprint):
            pool.add_event(event)
        transition(pool, PoolState.TRIAGED, reason="triage: new category")
        self.reprioritize(pool)
        return pool

    def merge_fingerprint(
        self, pool_id: str, fingerprint: str, reason: str = ""
    ) -> ErrorPool:
        """Attach a new fingerprint to an existing category (triage said same bug)."""
        pool = self.pools[pool_id]
        pool.merged_fingerprints.add(fingerprint)
        self.index[fingerprint] = pool_id
        for event in self._drain_pending(fingerprint):
            pool.add_event(event)
        if reason:
            pool.history.append(
                StateTransition(
                    from_state=pool.state, to_state=pool.state, reason=reason
                )
            )
        self.reprioritize(pool)
        return pool

    def abandon_triage(self, fingerprint: str) -> list[ErrorEvent]:
        """Give up on a fingerprint (e.g. Devin failed); release buffered events."""
        return self._drain_pending(fingerprint)

    # -------------------------------------------------------------- priority

    def reprioritize(self, pool: ErrorPool, now: float | None = None) -> float:
        """Recompute a pool's score and (lazily) re-key it in the heap."""
        pool.priority = compute_priority(pool, self.weights, now)
        pool.priority_version += 1
        if pool.state is REMEDIATION_READY and not pool.claimed:
            self.heap.push_pool(pool)
        return pool.priority

    def next_pool_for_remediation(self) -> ErrorPool | None:
        """Pop the highest-priority triaged pool and claim it. O(log n)."""
        while True:
            popped = self.heap.pop()
            if popped is None:
                return None
            pool_id, _ = popped
            pool = self.pools.get(pool_id)
            if pool is None or pool.claimed or pool.state is not REMEDIATION_READY:
                continue
            pool.claimed = True
            transition(pool, PoolState.REPRODUCING, reason="remediation lane claimed")
            return pool

    def release(self, pool: ErrorPool) -> None:
        """Unclaim a pool (only meaningful if it is back in a queued state)."""
        pool.claimed = False
        if pool.state is REMEDIATION_READY:
            self.reprioritize(pool)

    # --------------------------------------------------------------- queries

    def get(self, pool_id: str) -> ErrorPool | None:
        return self.pools.get(pool_id)

    def pool_for_fingerprint(self, fingerprint: str) -> ErrorPool | None:
        pool_id = self.index.get(fingerprint)
        return None if pool_id is None else self.pools[pool_id]

    def candidates_for_merge(self, limit: int = 20) -> list[ErrorPool]:
        """Existing categories a new fingerprint might belong to, newest first."""
        return sorted(self.pools.values(), key=lambda p: p.last_seen, reverse=True)[
            :limit
        ]

    def by_state(self, state: PoolState) -> list[ErrorPool]:
        return [pool for pool in self.pools.values() if pool.state is state]

    def snapshot(self) -> dict[str, int]:
        """Counts per state, for the (future) observability layer."""
        counts: dict[str, int] = {state.value: 0 for state in PoolState}
        for pool in self.pools.values():
            counts[pool.state.value] += 1
        return counts

    def all_pools(self) -> Iterable[ErrorPool]:
        return self.pools.values()
