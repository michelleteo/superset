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

"""Domain objects: raw error events and the pools (categories) they roll up to."""

from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Mapping

MAX_SAMPLE_EVENTS = 10


class PoolState(str, Enum):
    """Lifecycle of an error category."""

    NEW = "new"
    TRIAGED = "triaged"
    REPRODUCING = "reproducing"
    COULD_NOT_REPRODUCE = "could_not_reproduce"
    FIX_PROPOSED = "fix_proposed"
    AWAITING_REVIEW = "awaiting_review"
    AUTO_MERGED = "auto_merged"


#: States a pool never leaves without a human.
TERMINAL_STATES = frozenset(
    {PoolState.COULD_NOT_REPRODUCE, PoolState.AWAITING_REVIEW, PoolState.AUTO_MERGED}
)


class RiskTier(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(frozen=True)
class ErrorEvent:
    """A single observed error occurrence.

    Mirrors the payload produced by ``superset.mcp_service.webhook_logging``
    with a couple of optional attribution fields.
    """

    message: str
    timestamp: float = field(default_factory=time.time)
    level: str = "ERROR"
    logger: str = ""
    module: str = ""
    func: str = ""
    line: int = 0
    traceback: str | None = None
    user_id: str | None = None
    service: str = "superset"
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_webhook_payload(cls, payload: Mapping[str, Any]) -> ErrorEvent:
        """Build an event from a webhook payload, tolerating missing fields."""
        known = {
            "timestamp",
            "level",
            "logger",
            "message",
            "module",
            "func",
            "line",
            "traceback",
            "user_id",
            "service",
            "event_id",
        }
        user_id = payload.get("user_id")
        return cls(
            message=str(payload.get("message", "")),
            timestamp=_as_float(payload.get("timestamp"), time.time()),
            level=str(payload.get("level", "ERROR")),
            logger=str(payload.get("logger", "")),
            module=str(payload.get("module", "")),
            func=str(payload.get("func", "")),
            line=int(_as_float(payload.get("line"), 0)),
            traceback=(str(payload["traceback"]) if payload.get("traceback") else None),
            user_id=None if user_id is None else str(user_id),
            service=str(payload.get("service", "superset")),
            event_id=str(payload.get("event_id") or uuid.uuid4().hex),
            extra={k: v for k, v in payload.items() if k not in known},
        )


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class StateTransition:
    from_state: PoolState | None
    to_state: PoolState
    at: float = field(default_factory=time.time)
    reason: str = ""


@dataclass
class RiskAssessment:
    """Outcome of evaluating a diff against the risk registry."""

    tier: RiskTier
    score: int
    reasons: list[str] = field(default_factory=list)
    triggered_rules: list[str] = field(default_factory=list)


@dataclass
class ProposedFix:
    """Diff (plus test) produced by the remediation lane."""

    diff: str
    summary: str = ""
    test_added: bool = False
    session_url: str | None = None
    branch: str | None = None


@dataclass
class ErrorPool:
    """A category of errors sharing a fingerprint (or merged into one)."""

    fingerprint: str
    title: str
    signature: str = ""
    pool_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    state: PoolState = PoolState.NEW
    occurrences: int = 0
    affected_users: set[str] = field(default_factory=set)
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    #: Extra fingerprints merged into this pool by triage.
    merged_fingerprints: set[str] = field(default_factory=set)
    samples: Deque[ErrorEvent] = field(
        default_factory=lambda: deque(maxlen=MAX_SAMPLE_EVENTS)
    )
    history: list[StateTransition] = field(default_factory=list)
    #: Bumped on every priority change; heap entries carry it for lazy deletion.
    priority_version: int = 0
    priority: float = 0.0
    #: True while a remediation worker owns the pool.
    claimed: bool = False
    triage_session_url: str | None = None
    proposed_fix: ProposedFix | None = None
    risk: RiskAssessment | None = None
    review_reason: str | None = None
    merged_at: float | None = None

    def add_event(self, event: ErrorEvent) -> None:
        self.occurrences += 1
        self.last_seen = max(self.last_seen, event.timestamp)
        self.first_seen = min(self.first_seen, event.timestamp)
        if event.user_id:
            self.affected_users.add(event.user_id)
        self.samples.append(event)

    @property
    def sample_event(self) -> ErrorEvent | None:
        return self.samples[-1] if self.samples else None
