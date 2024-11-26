from __future__ import annotations

import contextlib
import statistics
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

import psutil
from koda_validate import DataclassValidator, Valid

from .filters import CommandFilters
from .utilities import abort

__all__ = [
    "ProcMeasurement",
    "ProcessSnapshots",
]


TOTAL_SYSTEM_MEMORY = psutil.virtual_memory().total


@dataclass(frozen=True)
class UniqueProcess:
    """Processes are identified by their PID and creation time; it is assumed that
    those two properties uniquely identify a process even when PIDs are re-used."""

    pid: int
    create_time: float


@dataclass
class ProcMeasurement:
    pid: int
    username: str | None
    time_start: float
    time_end: float
    cpu_usage: float = 0.0
    mem_usage: float = 0.0
    command: list[str] | None = None
    create_time: float | None = None

    @property
    def unique_id(self) -> UniqueProcess | None:
        if self.create_time is None:
            return None

        return UniqueProcess(pid=self.pid, create_time=self.create_time)

    @classmethod
    def snapshot(
        cls,
        proc: psutil.Process,
        time_start: float,
        time_end: float,
        min_uid: int,
    ) -> ProcMeasurement:
        command: list[str] | None = None
        create_time: float | None = None

        with contextlib.suppress(psutil.ZombieProcess, psutil.NoSuchProcess):
            command = proc.cmdline()
            create_time = proc.create_time()

        return ProcMeasurement(
            pid=proc.pid,
            username=(None if proc.uids().effective < min_uid else proc.username()),
            time_start=time_start,
            time_end=time_end,
            mem_usage=proc.memory_info().rss / TOTAL_SYSTEM_MEMORY,
            cpu_usage=proc.cpu_percent() / 100.0,
            command=command,
            create_time=create_time,
        )

    def to_json(self) -> object:
        return {
            "time_start": self.time_start,
            "time_end": self.time_end,
            "cpu_usage": self.cpu_usage,
            "mem_usage": self.mem_usage,
            "process": None,
            "command": self.command,
        }

    @classmethod
    def load(cls, data: object) -> ProcMeasurement:
        validator = DataclassValidator(ProcMeasurement)
        result = validator(data)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val


@dataclass
class MergedMeasurement:
    username: str | None
    time_start: float
    time_end: float
    cpu_usage: float
    mem_usage: float
    processes: set[UniqueProcess]

    @staticmethod
    def from_measurements(items: list[ProcMeasurement]) -> MergedMeasurement:
        result = MergedMeasurement(
            username=items[0].username,
            time_start=items[0].time_start,
            time_end=items[0].time_end,
            cpu_usage=0.0,
            mem_usage=0.0,
            processes=set(),
        )

        for it in items:
            if result.username != it.username:
                raise ValueError("mismatching usernames")
            elif result.time_start != it.time_start or result.time_end != it.time_end:
                raise ValueError("mismatching timestamps")

            result.cpu_usage += it.cpu_usage
            result.mem_usage += it.mem_usage

            if it.unique_id is not None:
                result.processes.add(it.unique_id)

        return result


class ProcessSnapshots:
    __slots__ = [
        "_measurements",
        "_patterns",
        "name",
    ]

    name: str | None
    _measurements: deque[MergedMeasurement]
    _patterns: CommandFilters

    def __init__(self, name: str | None, patterns: Iterable[str]) -> None:
        self.name = name
        self._measurements = deque()
        self._patterns = CommandFilters(patterns)

        if not self._patterns:
            raise ValueError("no patterns")

    def add_measurements(self, items: Iterable[ProcMeasurement]) -> None:
        items = list(items)
        if not items:
            raise ValueError("no measurements")

        items = [it for it in items if self._patterns(it.command)]
        if items:
            self._measurements.append(MergedMeasurement.from_measurements(items))

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
        if not self._measurements:
            return 0.0

        return statistics.mean(it.cpu_usage for it in self._measurements)

    @property
    def average_mem_usage(self) -> float:
        if not self._measurements:
            return 0.0

        return statistics.mean(it.mem_usage for it in self._measurements)

    @property
    def peak_cpu_usage(self) -> float:
        return max((it.cpu_usage for it in self._measurements), default=0)

    @property
    def peak_mem_usage(self) -> float:
        return max((it.mem_usage for it in self._measurements), default=0)

    @property
    def processes(self) -> int:
        processes: set[UniqueProcess] = set()
        for it in self._measurements:
            processes.update(it.processes)

        return len(processes)

    def __len__(self) -> int:
        return len(self._measurements)

    def __bool__(self) -> bool:
        return bool(self._measurements)
