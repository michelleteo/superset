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

"""Max-heap of error pools keyed on a priority score.

``heapq`` cannot cheaply update a key in place, and a pool's priority changes
every time a new occurrence lands in it. We therefore use lazy deletion: every
priority change pushes a *new* entry stamped with the pool's current version,
and :meth:`PriorityHeap.pop` discards entries whose version no longer matches.
"""

from __future__ import annotations

import heapq
import itertools
import math
import time
from dataclasses import dataclass, field

from error_orchestrator.models import ErrorPool

#: Half-life (seconds) of the recency term: a day-old pool loses half its boost.
DEFAULT_RECENCY_HALF_LIFE = 24 * 60 * 60


@dataclass(frozen=True)
class PriorityWeights:
    occurrences: float = 1.0
    affected_users: float = 2.0
    recency: float = 3.0
    recency_half_life: float = DEFAULT_RECENCY_HALF_LIFE


def compute_priority(
    pool: ErrorPool,
    weights: PriorityWeights | None = None,
    now: float | None = None,
) -> float:
    """Score a pool on occurrence count, affected users and recency."""
    weights = weights or PriorityWeights()
    now = time.time() if now is None else now
    age = max(now - pool.last_seen, 0.0)
    recency = 0.5 ** (age / weights.recency_half_life)
    return (
        weights.occurrences * math.log1p(pool.occurrences)
        + weights.affected_users * math.log1p(len(pool.affected_users))
        + weights.recency * recency
    )


@dataclass(order=True)
class _Entry:
    neg_priority: float
    sequence: int
    pool_id: str = field(compare=False)
    version: int = field(compare=False)


class PriorityHeap:
    """Max-heap over pool ids with lazy deletion of stale entries.

    ``push`` is O(log n); ``pop`` is amortized O(log n) plus the stale entries
    it discards (each stale entry is discarded exactly once).
    """

    def __init__(self) -> None:
        self._heap: list[_Entry] = []
        self._counter = itertools.count()
        #: pool_id -> version of the newest entry pushed for that pool.
        self._current_version: dict[str, int] = {}

    def __len__(self) -> int:
        """Number of *live* entries (stale ones are not counted)."""
        return sum(1 for entry in self._heap if self._is_live(entry))

    @property
    def raw_size(self) -> int:
        """Total entries including not-yet-discarded stale ones."""
        return len(self._heap)

    def _is_live(self, entry: _Entry) -> bool:
        return self._current_version.get(entry.pool_id) == entry.version

    def push(self, pool_id: str, priority: float, version: int) -> None:
        """Record a pool at a given priority/version, superseding older entries."""
        self._current_version[pool_id] = version
        entry = _Entry(-priority, next(self._counter), pool_id, version)
        heapq.heappush(self._heap, entry)

    def push_pool(self, pool: ErrorPool) -> None:
        self.push(pool.pool_id, pool.priority, pool.priority_version)

    def remove(self, pool_id: str) -> None:
        """Drop a pool: its entries become stale and are discarded on pop."""
        self._current_version.pop(pool_id, None)

    def pop(self) -> tuple[str, float] | None:
        """Return the highest-priority live ``(pool_id, priority)``, or ``None``."""
        while self._heap:
            entry = heapq.heappop(self._heap)
            if not self._is_live(entry):
                continue
            self._current_version.pop(entry.pool_id, None)
            return entry.pool_id, -entry.neg_priority
        return None

    def peek(self) -> tuple[str, float] | None:
        """Like :meth:`pop` but leaves the entry in place (still discards stale)."""
        while self._heap:
            entry = self._heap[0]
            if not self._is_live(entry):
                heapq.heappop(self._heap)
                continue
            return entry.pool_id, -entry.neg_priority
        return None
