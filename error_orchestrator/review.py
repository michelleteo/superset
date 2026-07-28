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

"""The human side of the pipeline.

Every terminal state is a hand-off, not an ending: an auto-merged fix still
needs someone to confirm it, a flagged fix needs a reviewer, and an error the
remediation lane could not reproduce needs an owner. A pool that reaches a
terminal state is therefore assigned to a person and stays open until that
person clears it.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from error_orchestrator.models import ErrorPool, PoolState, TERMINAL_STATES

logger = logging.getLogger(__name__)

DEFAULT_REVIEWERS = ("amara.o", "dev.patel", "jonas.k", "mei.lin")

#: What clearing a given terminal state means, shown as the default resolution.
DEFAULT_RESOLUTIONS = {
    PoolState.AUTO_MERGED: "merge verified",
    PoolState.AWAITING_REVIEW: "reviewed and merged",
    PoolState.COULD_NOT_REPRODUCE: "closed: not reproducible",
}


class ReviewError(RuntimeError):
    """Raised when an item cannot be assigned or cleared."""


@dataclass
class ReviewItem:
    """One terminal pool waiting on a human."""

    pool_id: str
    title: str
    state: PoolState
    assignee: str
    assigned_at: float = field(default_factory=time.time)
    cleared_at: float | None = None
    cleared_by: str | None = None
    resolution: str | None = None

    @property
    def open(self) -> bool:
        return self.cleared_at is None

    def as_dict(self) -> dict[str, Any]:
        return {
            "pool_id": self.pool_id,
            "title": self.title,
            "state": self.state.value,
            "assignee": self.assignee,
            "assigned_at": self.assigned_at,
            "cleared_at": self.cleared_at,
            "cleared_by": self.cleared_by,
            "resolution": self.resolution,
            "open": self.open,
        }


class ReviewQueue:
    """Assigns terminal pools to reviewers and tracks what is still open."""

    def __init__(self, reviewers: Iterable[str] = DEFAULT_REVIEWERS) -> None:
        names = [name for name in reviewers if name]
        if not names:
            raise ValueError("at least one reviewer is required")
        self.reviewers = names
        self.items: dict[str, ReviewItem] = {}
        #: Cleared items, newest last; used for the time-to-clear readout.
        self.cleared: list[ReviewItem] = []

    # ------------------------------------------------------------ assignment

    def assign(self, pool: ErrorPool, to: str | None = None) -> ReviewItem:
        """Assign a terminal pool to the least-loaded reviewer (or a named one)."""
        if pool.state not in TERMINAL_STATES:
            raise ReviewError(
                f"pool {pool.pool_id} is in {pool.state.value}, not a terminal state"
            )
        assignee = to or self._least_loaded()
        existing = self.items.get(pool.pool_id)
        if existing is not None and existing.open:
            existing.assignee = assignee
            existing.state = pool.state
            return existing
        item = ReviewItem(
            pool_id=pool.pool_id,
            title=pool.title,
            state=pool.state,
            assignee=assignee,
        )
        self.items[pool.pool_id] = item
        return item

    def _least_loaded(self) -> str:
        load = self.load()
        return min(self.reviewers, key=lambda name: (load[name], name))

    def load(self) -> dict[str, int]:
        """Open item count per reviewer."""
        counts = {name: 0 for name in self.reviewers}
        for item in self.items.values():
            if item.open and item.assignee in counts:
                counts[item.assignee] += 1
        return counts

    # -------------------------------------------------------------- clearing

    def clear(
        self, pool_id: str, by: str | None = None, resolution: str | None = None
    ) -> ReviewItem:
        item = self.items.get(pool_id)
        if item is None:
            raise ReviewError(f"pool {pool_id} is not in the review queue")
        if not item.open:
            raise ReviewError(f"pool {pool_id} was already cleared")
        item.cleared_at = time.time()
        item.cleared_by = by or item.assignee
        item.resolution = resolution or DEFAULT_RESOLUTIONS.get(item.state, "cleared")
        self.cleared.append(item)
        return item

    # --------------------------------------------------------------- queries

    def open_items(self) -> list[ReviewItem]:
        """Open items, oldest first — the backlog in the order it should be worked."""
        return sorted(
            (item for item in self.items.values() if item.open),
            key=lambda item: item.assigned_at,
        )

    def get(self, pool_id: str) -> ReviewItem | None:
        return self.items.get(pool_id)

    def mean_time_to_clear(self, window: int = 20) -> float | None:
        """Average seconds between assignment and clearing, over recent items."""
        recent = self.cleared[-window:]
        if not recent:
            return None
        return sum(
            (item.cleared_at or item.assigned_at) - item.assigned_at for item in recent
        ) / len(recent)

    def stats(self) -> dict[str, Any]:
        open_items = self.open_items()
        by_state: dict[str, int] = {}
        for item in open_items:
            by_state[item.state.value] = by_state.get(item.state.value, 0) + 1
        return {
            "open": len(open_items),
            "cleared": len(self.cleared),
            "open_by_state": by_state,
            "load": self.load(),
            "mean_time_to_clear": self.mean_time_to_clear(),
            "oldest_open_age": (
                time.time() - open_items[0].assigned_at if open_items else None
            ),
        }


class AutoReviewer:
    """A stand-in for the humans, so a demo shows the backlog draining.

    Clears the oldest open item every ``interval`` seconds (jittered). It is a
    convenience for unattended demos only — the same clearing path is what the
    dashboard buttons call.
    """

    def __init__(
        self,
        queue: ReviewQueue,
        interval: float = 12.0,
        jitter: float = 6.0,
        enabled: bool = True,
        seed: int | None = None,
        clear: Callable[[str], ReviewItem] | None = None,
    ) -> None:
        self.queue = queue
        self.interval = interval
        self.jitter = jitter
        self.enabled = enabled
        self._rng = random.Random(seed)  # noqa: S311 - simulation only
        self._task: asyncio.Task[None] | None = None
        self._clear = clear or (lambda pool_id: queue.clear(pool_id))
        self.cleared = 0

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="auto-reviewer")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _loop(self) -> None:
        while True:
            delay = max(self.interval + self._rng.uniform(0, self.jitter), 0.1)
            await asyncio.sleep(delay)
            if not self.enabled:
                continue
            open_items = self.queue.open_items()
            if not open_items:
                continue
            try:
                self._clear(open_items[0].pool_id)
            except ReviewError:
                continue
            self.cleared += 1


__all__ = [
    "AutoReviewer",
    "DEFAULT_REVIEWERS",
    "ReviewError",
    "ReviewItem",
    "ReviewQueue",
]
