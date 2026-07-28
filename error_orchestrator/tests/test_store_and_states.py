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

import pytest

from error_orchestrator.fingerprint import canonicalize
from error_orchestrator.models import ErrorPool, PoolState
from error_orchestrator.state_machine import (
    can_transition,
    InvalidTransitionError,
    transition,
)
from error_orchestrator.store import IngestOutcome, PoolStore
from error_orchestrator.tests.conftest import make_event


def _seed_pool(store: PoolStore, event_kwargs: dict[str, str] | None = None) -> str:
    event = make_event(**(event_kwargs or {}))
    result = store.record_event(event)
    pool = store.create_pool(result.fingerprint, result.canonical)
    return pool.pool_id


def test_first_event_needs_triage_then_dedups_in_o1() -> None:
    store = PoolStore()
    first = store.record_event(make_event())
    assert first.outcome is IngestOutcome.NEEDS_TRIAGE

    # A second occurrence of the same trace arrives while triage is in flight.
    buffered = store.record_event(make_event(user_id="u2"))
    assert buffered.outcome is IngestOutcome.TRIAGE_IN_FLIGHT

    pool = store.create_pool(first.fingerprint, first.canonical)
    assert pool.occurrences == 2  # both buffered events landed in the pool
    assert pool.affected_users == {"u1", "u2"}
    assert pool.state is PoolState.TRIAGED

    again = store.record_event(make_event(user_id="u3"))
    assert again.outcome is IngestOutcome.EXISTING_POOL
    assert again.pool is pool
    assert pool.occurrences == 3


def test_merge_fingerprint_routes_a_variant_to_an_existing_pool() -> None:
    store = PoolStore()
    pool_id = _seed_pool(store)
    variant = make_event(
        traceback=(make_event().traceback or "").replace("explore", "explore_json")
    )
    result = store.record_event(variant)
    assert result.outcome is IngestOutcome.NEEDS_TRIAGE

    store.merge_fingerprint(pool_id, result.fingerprint, reason="same root cause")
    pool = store.pools[pool_id]
    assert result.fingerprint in pool.merged_fingerprints
    assert pool.occurrences == 2
    # Later occurrences of the variant now dedup straight into the pool.
    assert store.record_event(variant).outcome is IngestOutcome.EXISTING_POOL


def test_next_pool_for_remediation_is_priority_ordered_and_claims() -> None:
    store = PoolStore()
    quiet = _seed_pool(store)
    loud = _seed_pool(
        store, {"traceback": (make_event().traceback or "").replace("412", "999")}
    )
    for index in range(20):
        store.pools[loud].add_event(make_event(user_id=f"u{index}"))
    store.reprioritize(store.pools[loud])

    first = store.next_pool_for_remediation()
    assert first is not None
    assert first.pool_id == loud
    assert first.claimed is True
    assert first.state is PoolState.REPRODUCING

    second = store.next_pool_for_remediation()
    assert second is not None
    assert second.pool_id == quiet
    assert store.next_pool_for_remediation() is None


def test_new_occurrence_reprioritizes_without_duplicating_work() -> None:
    store = PoolStore()
    pool_id = _seed_pool(store)
    pool = store.pools[pool_id]
    store.record_event(make_event(user_id="u9"))
    assert store.heap.raw_size == 2  # lazy deletion: a superseding entry

    claimed = store.next_pool_for_remediation()
    assert claimed is pool
    # While a worker owns it, further occurrences must not re-queue it.
    store.record_event(make_event(user_id="u10"))
    assert store.next_pool_for_remediation() is None


def test_state_machine_allows_only_the_documented_path() -> None:
    pool = ErrorPool(fingerprint="fp", title="t")
    transition(pool, PoolState.TRIAGED)
    transition(pool, PoolState.REPRODUCING)
    transition(pool, PoolState.FIX_PROPOSED)
    transition(pool, PoolState.AUTO_MERGED)
    assert [item.to_state for item in pool.history] == [
        PoolState.TRIAGED,
        PoolState.REPRODUCING,
        PoolState.FIX_PROPOSED,
        PoolState.AUTO_MERGED,
    ]


@pytest.mark.parametrize(
    ("from_state", "to_state"),
    [
        (PoolState.NEW, PoolState.FIX_PROPOSED),
        (PoolState.TRIAGED, PoolState.AUTO_MERGED),
        (PoolState.REPRODUCING, PoolState.AWAITING_REVIEW),
        (PoolState.COULD_NOT_REPRODUCE, PoolState.FIX_PROPOSED),
        (PoolState.AWAITING_REVIEW, PoolState.AUTO_MERGED),
    ],
)
def test_illegal_transitions_raise(from_state: PoolState, to_state: PoolState) -> None:
    pool = ErrorPool(fingerprint="fp", title="t", state=from_state)
    assert not can_transition(from_state, to_state)
    with pytest.raises(InvalidTransitionError):
        transition(pool, to_state)


def test_a_human_can_send_an_auto_merged_pool_back_for_review() -> None:
    pool = ErrorPool(fingerprint="fp", title="t", state=PoolState.AUTO_MERGED)

    transition(pool, PoolState.AWAITING_REVIEW, reason="human revert")

    assert pool.state is PoolState.AWAITING_REVIEW


def test_snapshot_counts_states() -> None:
    store = PoolStore()
    event = make_event()
    result = store.record_event(event)
    store.create_pool(result.fingerprint, canonicalize(event))
    assert store.snapshot()[PoolState.TRIAGED.value] == 1
    assert store.snapshot()[PoolState.AUTO_MERGED.value] == 0
