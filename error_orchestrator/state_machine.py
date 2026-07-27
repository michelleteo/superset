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

"""The error-pool state machine.

``new -> triaged -> reproducing -> could_not_reproduce | fix_proposed
      -> awaiting_review | auto_merged``

The orchestrator is the only component allowed to move a pool between states;
handlers are pure and merely report what they found.
"""

from __future__ import annotations

import time

from error_orchestrator.models import ErrorPool, PoolState, StateTransition

ALLOWED_TRANSITIONS: dict[PoolState, frozenset[PoolState]] = {
    PoolState.NEW: frozenset({PoolState.TRIAGED}),
    PoolState.TRIAGED: frozenset({PoolState.REPRODUCING}),
    PoolState.REPRODUCING: frozenset(
        {PoolState.COULD_NOT_REPRODUCE, PoolState.FIX_PROPOSED}
    ),
    PoolState.FIX_PROPOSED: frozenset(
        {PoolState.AWAITING_REVIEW, PoolState.AUTO_MERGED}
    ),
    # Terminal: only a human moves a pool out of these.
    PoolState.COULD_NOT_REPRODUCE: frozenset(),
    PoolState.AWAITING_REVIEW: frozenset(),
    PoolState.AUTO_MERGED: frozenset(),
}


class InvalidTransitionError(RuntimeError):
    def __init__(self, pool: ErrorPool, to_state: PoolState) -> None:
        super().__init__(
            f"pool {pool.pool_id} cannot move {pool.state.value} -> {to_state.value}"
        )
        self.pool = pool
        self.to_state = to_state


def can_transition(from_state: PoolState, to_state: PoolState) -> bool:
    return to_state in ALLOWED_TRANSITIONS[from_state]


def transition(pool: ErrorPool, to_state: PoolState, reason: str = "") -> ErrorPool:
    """Move a pool to ``to_state``, recording history. Raises on illegal moves."""
    if not can_transition(pool.state, to_state):
        raise InvalidTransitionError(pool, to_state)
    pool.history.append(
        StateTransition(
            from_state=pool.state, to_state=to_state, at=time.time(), reason=reason
        )
    )
    pool.state = to_state
    return pool
