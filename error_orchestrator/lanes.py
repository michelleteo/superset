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

"""Bounded worker pools ("lanes") running inside the orchestrator process.

Each lane owns a fixed number of concurrent workers, which is how capacity
limits ("a fixed number of remediation sessions") are enforced centrally: a
lane can never have more than ``concurrency`` Devin sessions in flight.

A lane pulls work from a *source*. Queue-backed lanes block on the queue;
the remediation lane instead pulls the highest-priority pool from the heap and
returns ``None`` when there is nothing to do, in which case the worker parks on
the lane's wakeup event until the orchestrator signals new work.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Safety net so a worker cannot park forever if a wakeup is ever missed.
IDLE_POLL_SECONDS = 1.0


@dataclass(frozen=True)
class WorkItem:
    """What a lane is about to work on, in terms the dashboard understands."""

    label: str
    pool_id: str | None = None


@dataclass
class ActiveWork:
    """One occupied worker slot."""

    worker: int
    label: str
    pool_id: str | None
    started_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "worker": self.worker,
            "label": self.label,
            "pool_id": self.pool_id,
            "started_at": self.started_at,
            "elapsed": time.time() - self.started_at,
        }


@dataclass
class LaneStats:
    processed: int = 0
    failed: int = 0
    in_flight: int = 0
    max_in_flight: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "processed": self.processed,
            "failed": self.failed,
            "in_flight": self.in_flight,
            "max_in_flight": self.max_in_flight,
        }


class Lane(Generic[T]):
    """A semaphore-limited async loop over a work source."""

    def __init__(
        self,
        name: str,
        concurrency: int,
        source: Callable[[], Awaitable[T | None]],
        process: Callable[[T], Awaitable[Any]],
        idle_poll: float = IDLE_POLL_SECONDS,
        describe: Callable[[T], WorkItem] | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError(f"lane {name}: concurrency must be >= 1")
        self.name = name
        self.concurrency = concurrency
        self.stats = LaneStats()
        self._source = source
        self._process = process
        self._idle_poll = idle_poll
        self._describe = describe
        #: Worker index -> what that worker is on right now.
        self.active: dict[int, ActiveWork] = {}
        self._semaphore = asyncio.Semaphore(concurrency)
        self._wakeup = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def notify(self) -> None:
        """Tell parked workers that new work may be available."""
        self._wakeup.set()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(index), name=f"{self.name}-{index}")
            for index in range(self.concurrency)
        ]

    async def stop(self) -> None:
        self._running = False
        self.notify()
        for worker in self._workers:
            worker.cancel()
        for worker in self._workers:
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        self._workers = []

    async def _worker(self, index: int) -> None:
        while self._running:
            try:
                item = await self._source()
            except asyncio.CancelledError:
                raise
            except Exception:  # pylint: disable=broad-except
                logger.exception("lane %s: source failed", self.name)
                await asyncio.sleep(self._idle_poll)
                continue
            if item is None:
                await self._park()
                continue
            async with self._semaphore:
                if self._describe is not None:
                    work = self._describe(item)
                    self.active[index] = ActiveWork(
                        worker=index,
                        label=work.label,
                        pool_id=work.pool_id,
                        started_at=time.time(),
                    )
                self.stats.in_flight += 1
                self.stats.max_in_flight = max(
                    self.stats.max_in_flight, self.stats.in_flight
                )
                try:
                    await self._process(item)
                    self.stats.processed += 1
                except asyncio.CancelledError:
                    raise
                except Exception:  # pylint: disable=broad-except
                    self.stats.failed += 1
                    logger.exception("lane %s: handler failed", self.name)
                finally:
                    self.stats.in_flight -= 1
                    self.active.pop(index, None)

    def active_work(self) -> list[dict[str, Any]]:
        """Every occupied slot, oldest first."""
        return [
            work.as_dict()
            for work in sorted(self.active.values(), key=lambda w: w.started_at)
        ]

    async def _park(self) -> None:
        self._wakeup.clear()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._wakeup.wait(), timeout=self._idle_poll)


@dataclass
class QueueSource(Generic[T]):
    """Adapts an :class:`asyncio.Queue` to the lane source protocol."""

    queue: asyncio.Queue[T] = field(default_factory=asyncio.Queue)

    async def __call__(self) -> T | None:
        return await self.queue.get()

    async def put(self, item: T) -> None:
        await self.queue.put(item)

    def qsize(self) -> int:
        return self.queue.qsize()
