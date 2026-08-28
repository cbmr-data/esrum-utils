# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import contextlib
import logging
import re
import shlex
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, TypeAlias

import psutil

from monitor_stats._utils import get_username

Metrics: TypeAlias = Literal["LoadAvg", "%CPU", "Memory"]

PATH_PROC = Path("/proc")
PATH_LOAD_AVERAGE = PATH_PROC / "loadavg"
PATH_CPU_STATS = PATH_PROC / "stat"
PATH_MEM_INFO = PATH_PROC / "meminfo"


@dataclass(frozen=True)
class IntensiveProcess:
    pid: int
    uid: int
    cpu: float
    mem: float
    proc: psutil.Process

    @property
    def cmd(self) -> str:
        try:
            return " ".join(shlex.quote(value) for value in self.proc.cmdline())
        except (FileNotFoundError, PermissionError, psutil.ZombieProcess):
            return "<error getting commandline>"


@dataclass
class BlacklistedProcess:
    pids: list[int]
    uid: int
    cmd: str
    runtime: float

    @staticmethod
    def merge(procs: list[BlacklistedProcess]) -> list[BlacklistedProcess]:
        """Merge identical commands; mostly intended for rsync"""
        runtimes: dict[tuple[int, str], float] = defaultdict(float)
        pids: dict[tuple[int, str], list[int]] = defaultdict(list)

        for it in procs:
            key = (it.uid, it.cmd)
            runtimes[key] += it.runtime
            pids[key].extend(it.pids)

        return [
            BlacklistedProcess(
                pids=pids[(uid, cmd)],
                uid=uid,
                cmd=cmd,
                runtime=runtime,
            )
            for (uid, cmd), runtime in runtimes.items()
        ]


@dataclass
class SystemTimes:
    user: float
    system: float
    idle: float

    @classmethod
    def now(cls) -> SystemTimes:
        times = psutil.cpu_times()

        return SystemTimes(
            user=times.user,
            system=times.system,
            idle=times.idle,
        )

    def since(self, last: SystemTimes) -> SystemTimes:
        return SystemTimes(
            user=max(0.0, self.user - last.user),
            system=max(0.0, self.system - last.system),
            idle=max(0.0, self.idle - last.idle),
        )

    def __str__(self) -> str:
        total = (self.user + self.system + self.idle) / 100
        user = self.user / total
        system = self.system / total
        idle = self.idle / total
        return f"user: {user:.1f}%, system: {system:.1f}%, idle: {idle:.1f}%"


@dataclass
class Summary:
    system: dict[Metrics, float]
    extras: dict[Metrics, str]
    blacklisted: list[BlacklistedProcess]
    top_processes_by_cpu: list[IntensiveProcess]
    top_processes_by_mem: list[IntensiveProcess]

    def top_processes(self) -> set[IntensiveProcess]:
        procs: set[IntensiveProcess] = set()
        if "Memory" in self.system:
            procs.update(self.top_processes_by_mem)
        if "%CPU" in self.system or "LoadAvg" in self.system:
            procs.update(self.top_processes_by_cpu)

        return procs


class Monitor:
    def __init__(
        self,
        process_whitelist: Iterable[str],
        process_blacklist: Iterable[str],
        loadavg_measure: Literal[1, 5, 15],
        min_process_uid: int,
        max_process_age: float,
    ) -> None:
        self._process_whitelist: tuple[re.Pattern[str], ...] = tuple(
            re.compile(it) for it in process_whitelist
        )
        self._process_blacklist: tuple[re.Pattern[str], ...] = tuple(
            re.compile(it) for it in process_blacklist
        )

        self._known_processes: dict[int, tuple[float, float]] = {}
        self._pid_whitelist: dict[int, float] = {}
        self._loadavg_measure = loadavg_measure
        self._min_process_uid = min_process_uid
        self._max_process_age = max_process_age
        self._last_system_times = SystemTimes.now()
        self.processes_mem: list[IntensiveProcess] = []
        self.processes_cpu: list[IntensiveProcess] = []

        self.get()

    def get(self) -> Summary:
        self._known_processes, processes = self._get_processes(self._known_processes)

        system_times_now = SystemTimes.now()
        system_times_delta = system_times_now.since(self._last_system_times)
        self._last_system_times = system_times_now

        return Summary(
            system={
                "%CPU": psutil.cpu_percent(),
                "LoadAvg": self._get_loadavg(),
                "Memory": self._get_mem_usage(),
            },
            extras={
                "%CPU": str(system_times_delta),
            },
            blacklisted=self._get_blacklisted_processes(),
            top_processes_by_cpu=self._filter_processes(
                processes,
                key=lambda it: it.cpu,
                min_value=0.5,
            ),
            top_processes_by_mem=self._filter_processes(
                processes,
                key=lambda it: it.mem,
                min_value=0.5,
            ),
        )

    def _get_processes(
        self,
        known_processes: Mapping[int, tuple[float, float]],
    ) -> tuple[dict[int, tuple[float, float]], list[IntensiveProcess]]:
        processes: list[IntensiveProcess] = []
        updated_processes: dict[int, tuple[float, float]] = {}
        for proc in psutil.process_iter():
            # Ignore processes that terminated before we can inspect them
            with contextlib.suppress(psutil.NoSuchProcess):
                # We also need CPU usage for newly spawned processes, so utilization is
                # counted manually and from the process creation time for new processes
                prev_time, prev_cpu_times = known_processes.get(
                    proc.pid, (proc.create_time(), 0.0)
                )

                curr_time = time.time()
                cpu_times = proc.cpu_times()
                curr_cpu_times = cpu_times.user + cpu_times.system

                updated_processes[proc.pid] = (curr_time, curr_cpu_times)

                age = curr_time - prev_time
                if age > 0.0:
                    processes.append(
                        IntensiveProcess(
                            pid=proc.pid,
                            uid=proc.uids().effective,
                            cpu=(curr_cpu_times - prev_cpu_times) / max(age, 0.001),
                            mem=proc.memory_percent(),
                            proc=proc,
                        )
                    )

        return updated_processes, processes

    @staticmethod
    def _filter_processes(
        processes: Iterable[IntensiveProcess],
        key: Callable[[IntensiveProcess], float],
        min_value: float,
        n: int = 3,
    ) -> list[IntensiveProcess]:
        processes = [it for it in processes if key(it) > min_value]
        processes.sort(key=key, reverse=True)

        return processes[:n]

    def _get_blacklisted_processes(self) -> list[BlacklistedProcess]:
        logger = logging.getLogger(__name__)
        processes: list[BlacklistedProcess] = []

        if self._process_blacklist:
            updated_whitelist: dict[int, float] = {}
            for it in PATH_PROC.iterdir():
                if it.name.isdigit():
                    pid = int(it.name)
                    try:
                        stat = it.lstat()
                        if (
                            stat.st_uid < self._min_process_uid
                            or self._pid_whitelist.get(pid) == stat.st_ctime
                        ):
                            updated_whitelist[pid] = stat.st_ctime
                            continue
                    except FileNotFoundError:
                        continue

                    runtime = time.time() - stat.st_ctime
                    if runtime < self._max_process_age:
                        continue

                    try:
                        cmdline_raw = (it / "cmdline").read_bytes()
                        cmdline = " ".join(
                            v.decode(errors="replace") for v in cmdline_raw.split(b"\0")
                        ).rstrip()
                    except FileNotFoundError:
                        continue
                    except PermissionError:
                        updated_whitelist[pid] = stat.st_ctime
                        continue

                    user = get_username(stat.st_uid)
                    logger.debug("checking PID %i (%s): %s", pid, user, cmdline)
                    if cmdline:

                        def is_on_list(
                            lst: Iterable[re.Pattern[str]],
                            cmdline: str,
                        ) -> bool:
                            return any(flt.search(cmdline) for flt in lst)

                        if is_on_list(self._process_whitelist, cmdline):
                            # do nothing; processed ignored subsequently
                            logger.info(
                                "whitelisted PID %i (%s): %s", pid, user, cmdline
                            )
                        elif is_on_list(self._process_blacklist, cmdline):
                            logger.warning(
                                "blacklisted PID %i (%s): %s", pid, user, cmdline
                            )

                            processes.append(
                                BlacklistedProcess(
                                    pids=[pid],
                                    uid=stat.st_uid,
                                    cmd=cmdline,
                                    runtime=runtime,
                                )
                            )

                    updated_whitelist[pid] = stat.st_ctime

            self._pid_whitelist = updated_whitelist

        return processes

    def _get_loadavg(self) -> float:
        with PATH_LOAD_AVERAGE.open("rb") as handle:
            loadavg = handle.readline().split(None)

        if self._loadavg_measure == 1:
            return float(loadavg[0])  # avg. last minute
        elif self._loadavg_measure == 5:
            return float(loadavg[1])  # avg. last five minutes
        elif self._loadavg_measure == 15:
            return float(loadavg[2])  # avg. last fifteen minutes
        else:
            raise NotImplementedError(self._get_loadavg)

    @classmethod
    def _get_mem_usage(cls) -> float:
        values: dict[bytes, int] = {}
        with PATH_MEM_INFO.open("rb") as handle:
            for line in handle:
                key, value, *_unit = line.split()
                values[key] = int(value)

        # MemAvailable does not include buffers / caches
        return 100 * (1 - values[b"MemAvailable:"] / values[b"MemTotal:"])
