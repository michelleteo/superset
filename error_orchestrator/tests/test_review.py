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

import asyncio
from typing import Callable

import pytest

from error_orchestrator.models import ErrorPool, PoolState
from error_orchestrator.review import (
    AutoReviewer,
    ReviewError,
    ReviewItem,
    ReviewQueue,
)


def _pool(state: PoolState, title: str = "boom") -> ErrorPool:
    pool = ErrorPool(fingerprint=title, title=title)
    pool.state = state
    return pool


def test_only_terminal_pools_can_be_assigned() -> None:
    queue = ReviewQueue(["ana"])
    with pytest.raises(ReviewError):
        queue.assign(_pool(PoolState.REPRODUCING))


@pytest.mark.parametrize(
    "state",
    [PoolState.AWAITING_REVIEW, PoolState.AUTO_MERGED, PoolState.COULD_NOT_REPRODUCE],
)
def test_every_terminal_state_needs_a_human(state: PoolState) -> None:
    """Auto-merged is a hand-off too, not an ending."""
    queue = ReviewQueue(["ana"])
    item = queue.assign(_pool(state))
    assert item.open
    assert queue.stats()["open"] == 1


def test_assignment_goes_to_the_least_loaded_reviewer() -> None:
    queue = ReviewQueue(["ana", "bo"])
    first = queue.assign(_pool(PoolState.AWAITING_REVIEW, "one"))
    second = queue.assign(_pool(PoolState.AWAITING_REVIEW, "two"))
    third = queue.assign(_pool(PoolState.AWAITING_REVIEW, "three"))

    assert {first.assignee, second.assignee} == {"ana", "bo"}
    # Two of three items land on one reviewer, but never three on the same one.
    assert queue.load()[third.assignee] == 2
    assert sorted(queue.load().values()) == [1, 2]


def test_clearing_frees_the_reviewer_and_records_a_resolution() -> None:
    queue = ReviewQueue(["ana"])
    pool = _pool(PoolState.COULD_NOT_REPRODUCE)
    queue.assign(pool)

    item = queue.clear(pool.pool_id)

    assert not item.open
    assert item.cleared_by == "ana"
    assert item.resolution == "closed: not reproducible"
    assert queue.load()["ana"] == 0
    assert queue.stats()["cleared"] == 1


def test_an_item_cannot_be_cleared_twice() -> None:
    queue = ReviewQueue(["ana"])
    pool = _pool(PoolState.AUTO_MERGED)
    queue.assign(pool)
    queue.clear(pool.pool_id)

    with pytest.raises(ReviewError):
        queue.clear(pool.pool_id)


def test_clearing_an_unknown_pool_is_rejected() -> None:
    with pytest.raises(ReviewError):
        ReviewQueue(["ana"]).clear("nope")


def test_reassignment_keeps_one_open_item() -> None:
    queue = ReviewQueue(["ana", "bo"])
    pool = _pool(PoolState.AWAITING_REVIEW)
    queue.assign(pool)

    item = queue.assign(pool, to="bo")

    assert item.assignee == "bo"
    assert queue.stats()["open"] == 1
    assert queue.load() == {"ana": 0, "bo": 1}


def test_backlog_is_ordered_oldest_first() -> None:
    queue = ReviewQueue(["ana"])
    pools = [_pool(PoolState.AWAITING_REVIEW, f"p{index}") for index in range(3)]
    for pool in pools:
        queue.assign(pool)

    assert [item.pool_id for item in queue.open_items()] == [
        pool.pool_id for pool in pools
    ]


def test_queue_requires_a_reviewer() -> None:
    with pytest.raises(ValueError, match="reviewer"):
        ReviewQueue([])


# --------------------------------------------------- the stand-in reviewers


async def _run_reviewer(reviewer: AutoReviewer, until: Callable[[], bool]) -> None:
    """Let the loop tick until it has done its work (or fail the test)."""
    reviewer.start()
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2.0
        while loop.time() < deadline:
            if until():
                return
            await asyncio.sleep(0.01)
        raise AssertionError("the auto reviewer never got there")
    finally:
        await reviewer.stop()


@pytest.mark.asyncio
async def test_the_stand_in_reviewer_clears_the_oldest_item_first() -> None:
    queue = ReviewQueue(["ana"])
    first = queue.assign(_pool(PoolState.AWAITING_REVIEW, "one"))
    queue.assign(_pool(PoolState.AWAITING_REVIEW, "two"))
    reviewer = AutoReviewer(queue, interval=0.0, jitter=0.0, seed=1)

    await _run_reviewer(reviewer, lambda: reviewer.cleared >= 1)

    assert queue.items[first.pool_id].open is False
    assert queue.stats()["open"] >= 1


@pytest.mark.asyncio
async def test_a_disabled_reviewer_leaves_the_backlog_for_a_human() -> None:
    queue = ReviewQueue(["ana"])
    queue.assign(_pool(PoolState.AWAITING_REVIEW))
    reviewer = AutoReviewer(queue, interval=0.0, jitter=0.0, enabled=False, seed=1)

    reviewer.start()
    await asyncio.sleep(0.3)
    await reviewer.stop()

    assert reviewer.cleared == 0
    assert queue.stats()["open"] == 1


@pytest.mark.asyncio
async def test_an_item_a_human_got_to_first_is_not_counted_twice() -> None:
    queue = ReviewQueue(["ana"])
    pool = _pool(PoolState.AWAITING_REVIEW)
    queue.assign(pool)
    attempts = 0

    def clear(pool_id: str) -> ReviewItem:
        nonlocal attempts
        attempts += 1
        raise ReviewError("already cleared")

    reviewer = AutoReviewer(queue, interval=0.0, jitter=0.0, seed=1, clear=clear)

    await _run_reviewer(reviewer, lambda: attempts >= 2)

    assert reviewer.cleared == 0


@pytest.mark.asyncio
async def test_stopping_a_reviewer_that_never_started_is_harmless() -> None:
    reviewer = AutoReviewer(ReviewQueue(["ana"]), interval=0.0)
    await reviewer.stop()

    reviewer.start()
    reviewer.start()
    await reviewer.stop()
