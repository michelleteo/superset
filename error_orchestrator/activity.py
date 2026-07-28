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

"""Observability primitives: an activity feed and rolling throughput counters.

The orchestrator publishes one :class:`ActivityEvent` per interesting decision
(dedup hit, lane pickup, state change, human clear). The feed is a bounded ring
buffer with monotonic sequence numbers, so a dashboard can poll for "everything
after seq N" without re-reading history it already has.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque

DEFAULT_CAPACITY = 500
DEFAULT_WINDOW = 60.0


@dataclass(frozen=True)
class ActivityEvent:
    seq: int
    at: float
    kind: str
    message: str
    pool_id: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "at": self.at,
            "kind": self.kind,
            "message": self.message,
            "pool_id": self.pool_id,
            "detail": self.detail,
        }


class RollingRate:
    """Timestamps inside a sliding window, for per-minute rate readouts."""

    def __init__(self, window: float = DEFAULT_WINDOW) -> None:
        self.window = window
        self.total = 0
        self._marks: Deque[float] = deque()

    def mark(self, at: float | None = None) -> None:
        self.total += 1
        self._marks.append(time.time() if at is None else at)
        self._trim()

    def _trim(self, now: float | None = None) -> None:
        cutoff = (time.time() if now is None else now) - self.window
        while self._marks and self._marks[0] < cutoff:
            self._marks.popleft()

    def per_minute(self, now: float | None = None) -> float:
        """Observations in the window, extrapolated to a per-minute figure."""
        self._trim(now)
        return len(self._marks) * (60.0 / self.window)


class ActivityLog:
    """Bounded, sequence-numbered feed of orchestrator activity."""

    def __init__(self, capacity: int = DEFAULT_CAPACITY) -> None:
        self.capacity = capacity
        self._events: Deque[ActivityEvent] = deque(maxlen=capacity)
        self._seq = 0
        self.counters: dict[str, int] = {}
        self.rates: dict[str, RollingRate] = {}

    def publish(
        self,
        kind: str,
        message: str,
        pool_id: str | None = None,
        **detail: Any,
    ) -> ActivityEvent:
        self._seq += 1
        event = ActivityEvent(
            seq=self._seq,
            at=time.time(),
            kind=kind,
            message=message,
            pool_id=pool_id,
            detail=detail,
        )
        self._events.append(event)
        self.counters[kind] = self.counters.get(kind, 0) + 1
        return event

    def count(self, name: str, amount: int = 1) -> None:
        """Bump a named counter without emitting a feed entry."""
        self.counters[name] = self.counters.get(name, 0) + amount

    def mark(self, name: str) -> None:
        """Record an observation on a named rolling rate."""
        self.rates.setdefault(name, RollingRate()).mark()

    def rate(self, name: str) -> float:
        rate = self.rates.get(name)
        return 0.0 if rate is None else rate.per_minute()

    @property
    def last_seq(self) -> int:
        return self._seq

    def since(self, seq: int, limit: int = DEFAULT_CAPACITY) -> list[ActivityEvent]:
        """Events newer than ``seq``, oldest first."""
        return [event for event in self._events if event.seq > seq][-limit:]


__all__ = ["ActivityEvent", "ActivityLog", "RollingRate"]
