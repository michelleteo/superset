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

"""A lane keeps its workers alive and its slots bounded whatever the work does."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from error_orchestrator.lanes import Lane, QueueSource, WorkItem


async def _settle(lane: Lane[Any], predicate: Any, timeout: float = 2.0) -> None:
    """Wait for the lane to reach a state, rather than for a fixed delay."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"lane {lane.name} never settled: {lane.stats.as_dict()}")


async def _no_work() -> int | None:
    return None


async def _noop(item: int) -> None:
    return None


def test_a_lane_without_a_worker_is_rejected() -> None:
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        Lane("triage", 0, _no_work, _noop)


@pytest.mark.asyncio
async def test_stopping_a_lane_that_never_started_is_harmless() -> None:
    lane = Lane("triage", 1, _no_work, _noop)
    await lane.stop()

    assert lane.running is False


@pytest.mark.asyncio
async def test_starting_twice_does_not_double_the_workers() -> None:
    source = QueueSource[int]()
    processed: list[int] = []

    async def process(item: int) -> None:
        processed.append(item)

    lane = Lane("triage", 2, source, process, idle_poll=0.01)
    lane.start()
    lane.start()
    try:
        await source.put(1)
        await _settle(lane, lambda: processed == [1])
    finally:
        await lane.stop()

    assert lane.stats.processed == 1
    assert source.qsize() == 0


@pytest.mark.asyncio
async def test_a_source_that_raises_does_not_take_the_lane_down() -> None:
    failures = 0

    async def source() -> int | None:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise RuntimeError("store unavailable")
        return None if failures > 2 else 7

    seen: list[int] = []

    async def process(item: int) -> None:
        seen.append(item)

    lane = Lane("remediate", 1, source, process, idle_poll=0.01)
    lane.start()
    try:
        await _settle(lane, lambda: seen == [7])
    finally:
        await lane.stop()

    assert lane.stats.failed == 0


@pytest.mark.asyncio
async def test_a_handler_that_raises_is_counted_and_the_slot_released() -> None:
    source = QueueSource[str]()

    async def process(item: str) -> None:
        raise RuntimeError(f"handler blew up on {item}")

    lane = Lane(
        "risk_check",
        1,
        source,
        process,
        idle_poll=0.01,
        describe=lambda item: WorkItem(label=item, pool_id=item),
    )
    lane.start()
    try:
        await source.put("pool-1")
        await _settle(lane, lambda: lane.stats.failed == 1)
    finally:
        await lane.stop()

    assert lane.stats.processed == 0
    assert lane.stats.in_flight == 0
    assert lane.active_work() == []


@pytest.mark.asyncio
async def test_the_lane_reports_what_each_worker_is_holding() -> None:
    source = QueueSource[str]()
    release = asyncio.Event()

    async def process(item: str) -> None:
        await release.wait()

    lane = Lane(
        "remediate",
        2,
        source,
        process,
        idle_poll=0.01,
        describe=lambda item: WorkItem(label=f"fixing {item}", pool_id=item),
    )
    lane.start()
    try:
        await source.put("pool-1")
        await source.put("pool-2")
        await _settle(lane, lambda: lane.stats.in_flight == 2)

        work = lane.active_work()
        assert {item["pool_id"] for item in work} == {"pool-1", "pool-2"}
        assert work[0]["label"].startswith("fixing ")
        assert work[0]["elapsed"] >= 0
        assert lane.stats.max_in_flight == 2
    finally:
        release.set()
        await lane.stop()


@pytest.mark.asyncio
async def test_a_parked_worker_wakes_up_when_notified() -> None:
    pending: list[int] = []
    processed: list[int] = []

    async def source() -> int | None:
        return pending.pop() if pending else None

    async def process(item: int) -> None:
        processed.append(item)

    # An idle poll far beyond the test's patience: only ``notify`` can wake it.
    lane = Lane("triage", 1, source, process, idle_poll=30.0)
    lane.start()
    try:
        await _settle(lane, lambda: True)
        pending.append(3)
        lane.notify()
        await _settle(lane, lambda: processed == [3])
    finally:
        await lane.stop()
