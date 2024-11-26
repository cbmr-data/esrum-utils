from __future__ import annotations

import contextlib
import json
import logging
import time
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import psutil
from koda_validate import DataclassValidator, ListValidator, Valid
from typing_extensions import Self

from .processes import ProcMeasurement
from .system import SystemMeasurement
from .utilities import abort

__all__ = [
    "Monitor",
    "ProcessMonitor",
    "Snapshot",
]

_LOGGER = logging.getLogger(__name__)

_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


@dataclass
class Snapshot:
    timestamp: float
    system: SystemMeasurement
    processes: list[ProcMeasurement]


class ProcessMonitor:
    __slots__ = [
        "_last_time",
        "_min_uid",
        "_users",
    ]

    def __init__(self, *, min_user_id: int) -> None:
        self._last_time: float = time.time()
        self._min_uid = min_user_id

    def collect(self) -> tuple[SystemMeasurement, list[ProcMeasurement]]:
        current_time = time.time()

        processes: list[ProcMeasurement] = []
        for proc in psutil.process_iter():
            with contextlib.suppress(psutil.ZombieProcess, psutil.NoSuchProcess):
                process = ProcMeasurement.snapshot(
                    proc=proc,
                    time_start=self._last_time,
                    time_end=current_time,
                    min_uid=self._min_uid,
                )

                processes.append(process)

        system = SystemMeasurement.snapshot(
            processes=processes,
            time_start=self._last_time,
            time_end=current_time,
        )

        self._last_time = current_time
        return (system, processes)


class Monitor:
    __slots__ = [
        "_interval",
        "_processes",
        "_replay_in",
        "_replay_out",
    ]

    _processes: ProcessMonitor
    _replay_in: list[Snapshot] | None
    _replay_out: IO[str] | None

    def __init__(
        self,
        *,
        min_user_id: int,
        interval: int,
        load_replay: Path | None,
        save_replay: Path | None,
    ) -> None:
        self._processes = ProcessMonitor(min_user_id=min_user_id)
        self._replay_in = None
        self._replay_out = None
        self._interval = interval

        if load_replay is not None and save_replay is not None:
            self._replay_out = save_replay.open("w", encoding="utf-8")
        elif load_replay is not None:
            self._replay_in = Monitor._load_replay(load_replay)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if self._replay_out is not None:
            self._replay_out.close()

    def __iter__(self) -> Generator[Snapshot]:
        if self._replay_in is not None:
            yield from self._replay_in
        else:
            # Ensure that records are start by the second, minute or hour
            last_loop = time.time()
            last_loop -= last_loop % self._interval

            while True:
                time_before_sleep = time.time()
                expected_sleep_time = (
                    self._interval - time_before_sleep % self._interval
                )

                time.sleep(expected_sleep_time)
                time_after_sleep = time.time()
                actual_sleep_time = time_after_sleep - time_before_sleep
                if actual_sleep_time >= expected_sleep_time + 0.5:
                    _warning(
                        "Drift of %.1f seconds detected after sleep",
                        actual_sleep_time - expected_sleep_time,
                    )

                system, processes = self._processes.collect()
                time_after_collect = time.time()
                if time_after_collect - time_after_sleep >= 1.0:
                    _warning(
                        "Drift of %.1f seconds during collection of process statistics",
                        time_after_collect - time_after_sleep,
                    )

                yield Snapshot(
                    timestamp=time_after_sleep,
                    system=system,
                    processes=processes,
                )

    @staticmethod
    def _load_replay(path: Path) -> list[Snapshot]:
        with path.open("rb", encoding="utf-8") as handle:
            data = json.load(handle)

        validator = ListValidator(DataclassValidator(Snapshot))
        result = validator(data)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val[::-1]
