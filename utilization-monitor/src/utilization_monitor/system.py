from __future__ import annotations

import statistics
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable

import psutil

from .processes import ProcMeasurement, UniqueProcess

__all__ = [
    "SystemMeasurement",
]


TOTAL_CPUS = psutil.cpu_count()
TOTAL_MEM = psutil.virtual_memory().total / 2**30  # GiB


@dataclass
class SystemMeasurement:
    time_start: float
    time_end: float
    users: set[str]
    user_processes: set[UniqueProcess]
    cpu_usage: float = 0.0
    mem_usage: float = 0.0

    @classmethod
    def snapshot(
        cls,
        processes: list[ProcMeasurement],
        time_start: float,
        time_end: float | None = None,
    ) -> SystemMeasurement:
        users: set[str] = set()
        unique_processes: set[UniqueProcess] = set()
        for proc in processes:
            if proc.unique_id is not None:
                unique_processes.add(proc.unique_id)

            if proc.username is not None:
                users.add(proc.username)

        memory = psutil.virtual_memory()
        return SystemMeasurement(
            time_start=time_start,
            time_end=time.time() if time_end is None else time_end,
            cpu_usage=psutil.cpu_percent() / 100 * TOTAL_CPUS,
            mem_usage=memory.percent / 100 * TOTAL_MEM,
            users=users,
            user_processes=unique_processes,
        )


class SystemSnapshots:
    __slots__ = [
        "_measurements",
    ]

    _measurements: deque[SystemMeasurement]

    def __init__(self) -> None:
        self._measurements = deque()

    def add_measurement(self, it: SystemMeasurement) -> None:
        self._measurements.append(it)

    def reset(self) -> None:
        self._measurements.clear()

    def prune_before(self, min_timestamp: float) -> None:
        while self._measurements and self._measurements[0].time_end < min_timestamp:
            self._measurements.popleft()

    @property
    def time_start(self) -> float:
        return min((it.time_start for it in self._measurements), default=float("nan"))

    @property
    def time_end(self) -> float:
        return max((it.time_end for it in self._measurements), default=float("nan"))

    @property
    def timespan(self) -> float:
        if not self._measurements:
            return 0.0

        return self.time_start - self.time_end

    @property
    def average_cpu_usage(self) -> float:
        return self._mean_statistic(lambda it: it.cpu_usage)

    @property
    def average_mem_usage(self) -> float:
        return self._mean_statistic(lambda it: it.mem_usage)

    @property
    def peak_cpu_usage(self) -> float:
        return max((it.cpu_usage for it in self._measurements), default=0)

    @property
    def peak_mem_usage(self) -> float:
        return max((it.mem_usage for it in self._measurements), default=0)

    @property
    def users(self) -> int:
        usernames: set[str] = set()
        for it in self._measurements:
            usernames.update(it.users)

        return len(usernames)

    @property
    def user_processes(self) -> int:
        processes: set[UniqueProcess] = set()
        for it in self._measurements:
            processes.update(it.user_processes)

        return len(processes)

    def __len__(self) -> int:
        return len(self._measurements)

    def __bool__(self) -> bool:
        return bool(self._measurements)

    def _mean_statistic(self, key: Callable[[SystemMeasurement], float]) -> float:
        if not self._measurements:
            return 0.0

        total_timespan = 0.0
        measurements: list[float] = []
        for it in self._measurements:
            timespan = it.time_end - it.time_start
            measurements.append(key(it) * timespan)
            total_timespan += timespan

        return statistics.mean(measurements) / total_timespan
