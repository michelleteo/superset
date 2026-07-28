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

import time

from error_orchestrator.models import ErrorPool
from error_orchestrator.priority import compute_priority, PriorityHeap


def _pool(occurrences: int, users: int, age_seconds: float = 0.0) -> ErrorPool:
    pool = ErrorPool(fingerprint=f"fp{occurrences}{users}", title="t")
    pool.occurrences = occurrences
    pool.affected_users = {f"u{index}" for index in range(users)}
    pool.last_seen = time.time() - age_seconds
    return pool


def test_priority_rewards_occurrences_users_and_recency() -> None:
    quiet = compute_priority(_pool(1, 1))
    loud = compute_priority(_pool(100, 1))
    widespread = compute_priority(_pool(1, 50))
    stale = compute_priority(_pool(1, 1, age_seconds=30 * 24 * 3600))
    assert loud > quiet
    assert widespread > quiet
    assert stale < quiet


def test_heap_pops_highest_priority_first() -> None:
    heap = PriorityHeap()
    heap.push("low", 1.0, version=1)
    heap.push("high", 9.0, version=1)
    heap.push("mid", 5.0, version=1)
    assert [heap.pop(), heap.pop(), heap.pop()] == [
        ("high", 9.0),
        ("mid", 5.0),
        ("low", 1.0),
    ]
    assert heap.pop() is None


def test_lazy_deletion_discards_stale_entries() -> None:
    heap = PriorityHeap()
    heap.push("a", 1.0, version=1)
    heap.push("b", 2.0, version=1)
    # "a" gets hotter: a new entry supersedes the stale one.
    heap.push("a", 5.0, version=2)
    assert heap.raw_size == 3
    assert len(heap) == 2
    assert heap.pop() == ("a", 5.0)
    assert heap.pop() == ("b", 2.0)
    # The stale v1 entry for "a" is discarded rather than served twice.
    assert heap.pop() is None


def test_remove_makes_all_entries_stale() -> None:
    heap = PriorityHeap()
    heap.push("a", 1.0, version=1)
    heap.push("a", 3.0, version=2)
    heap.remove("a")
    assert len(heap) == 0
    assert heap.pop() is None


def test_peek_leaves_the_entry_in_place() -> None:
    heap = PriorityHeap()
    heap.push("a", 1.0, version=1)
    assert heap.peek() == ("a", 1.0)
    assert heap.pop() == ("a", 1.0)
