#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "coloredlogs==15.0.1",
#     "koda-validate==4.1.1",
#     "psutil==7.2.1",
#     "requests~=2.33.0",
#     "tomli==2.0.1",
#     "typing-extensions==4.11.0",
# ]
# ///
from __future__ import annotations

import argparse
import contextlib
import functools
import json
import logging
import pwd
import re
import shlex
import socket
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
from typing import Callable, Literal, NoReturn, TypeAlias, Union

import coloredlogs
import psutil
import requests
import tomli
from koda_validate import DataclassValidator, Valid

_LOG = logging.getLogger("monitor-stats")

_debug = _LOG.debug
_error = _LOG.error
_info = _LOG.info
_warning = _LOG.warning
_log = _LOG.log

Metrics: TypeAlias = Literal["LoadAvg", "%CPU", "Memory"]
JSON: TypeAlias = (
    dict[
        str,
        Union[float, str, bool, "JSON"],
    ]
    | list[Union[float, str, bool, "JSON"]]
)


PATH_PROC = Path("/proc")
PATH_LOAD_AVERAGE = PATH_PROC / "loadavg"
PATH_CPU_STATS = PATH_PROC / "stat"
PATH_MEM_INFO = PATH_PROC / "meminfo"


def abort(msg: str, *values: object) -> NoReturn:
    _error(msg, *values)
    sys.exit(1)


@functools.cache
def get_username(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def format_time(seconds: float) -> str:
    fields: list[str] = []
    for cutoff in (3600, 60, 1):
        if seconds >= cutoff:
            value = int(seconds // cutoff)
            seconds %= cutoff
            fields.append(f"{value:02}" if fields else f"{value}")

    if not fields:
        fields.append(f"{seconds:.1f}")

    fields[-1] += "s"
    return ":".join(fields)


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
            return " ".join(map(shlex.quote, self.proc.cmdline()))
        except (FileNotFoundError, PermissionError):
            return "<error getting commandline>"


@dataclass
class BlacklistedProcess:
    pid: int
    uid: int
    cmd: str
    runtime: float


@dataclass
class Summary:
    system: dict[Metrics, float]
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

        self._pid_whitelist: dict[int, float] = {}
        self._loadavg_measure = loadavg_measure
        self._min_process_uid = min_process_uid
        self._max_process_age = max_process_age
        self.processes_mem: list[IntensiveProcess] = []
        self.processes_cpu: list[IntensiveProcess] = []

        self.get()

    def get(self) -> Summary:
        processes = self._get_processes()

        return Summary(
            system={
                "%CPU": psutil.cpu_percent(),
                "LoadAvg": self._get_loadavg(),
                "Memory": self._get_mem_usage(),
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

    @staticmethod
    def _get_processes() -> list[IntensiveProcess]:
        processes: list[IntensiveProcess] = []
        for proc in psutil.process_iter():
            # Ignore processes that terminated before we can inspect them
            with contextlib.suppress(psutil.NoSuchProcess):
                processes.append(
                    IntensiveProcess(
                        pid=proc.pid,
                        uid=proc.uids().effective,
                        cpu=proc.cpu_percent(interval=None) / 100,
                        mem=proc.memory_percent(),
                        proc=proc,
                    )
                )

        return processes

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
                    _debug("checking PID %i (%s) with command %r", pid, user, cmdline)
                    if cmdline:

                        def is_on_list(
                            lst: Iterable[re.Pattern[str]],
                            cmdline: str,
                        ) -> bool:
                            return any(flt.search(cmdline) for flt in lst)

                        if is_on_list(self._process_whitelist, cmdline):
                            # do nothing; processed ignored subsequently
                            _info("whitelisted PID %i (%s): %s", pid, user, cmdline)
                        elif is_on_list(self._process_blacklist, cmdline):
                            _warning("blacklisted PID %i (%s): %s", pid, user, cmdline)
                            processes.append(
                                BlacklistedProcess(
                                    pid=pid,
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


class SlackNotifier:
    def __init__(self, *, webhooks: list[str], timeout: float, host: str) -> None:
        self._log = logging.getLogger("slack")
        self._webhooks = list(webhooks)
        self._timeout = timeout
        self._host = host

    def notify(self, summary: Summary) -> bool:
        if not self._webhooks:
            self._log.warning("Slack LDAP update not sent; no webhooks configured")
            return False
        elif not (summary.system or summary.blacklisted):
            return False

        alerts: JSON = []

        if summary.system:
            alerts.extend(
                self._add_entry(
                    f"Resource usage at {self._host} exceeds thresholds",
                    [
                        self._add_metrics(key, value)
                        for key, value in summary.system.items()
                    ],
                )
            )

            if procs := summary.top_processes():
                alerts.extend(
                    self._add_entry(
                        "Top most resource intensive processes are",
                        [
                            self._add_process(
                                uid=it.uid,
                                pid=it.pid,
                                cmdline=it.cmd,
                                cpu_mem=(it.cpu, it.mem),
                            )
                            for it in sorted(procs, key=lambda it: -max(it.cpu, it.mem))
                        ],
                        warning=False,
                    )
                )

        if summary.blacklisted:
            alerts.extend(
                self._add_entry(
                    f"Blacklisted process is running on {self._host}",
                    [
                        self._add_process(
                            uid=it.uid,
                            pid=it.pid,
                            cmdline=it.cmd,
                            runtime=it.runtime,
                        )
                        for it in summary.blacklisted
                    ],
                )
            )

        blocks: list[JSON] = [
            {
                "type": "rich_text",
                "elements": alerts,
            },
        ]

        return self._send_message(blocks)

    @classmethod
    def _add_entry(
        cls,
        message: str,
        elements: JSON,
        *,
        warning: bool = True,
    ) -> Iterable[JSON]:
        text: JSON = [{"type": "emoji", "name": "warning"}] if warning else []
        text.append({"type": "text", "text": f"{message} "})

        yield {"type": "rich_text_section", "elements": text}

        yield {
            "type": "rich_text_list",
            "style": "bullet",
            "indent": 0,
            "elements": elements,
        }

    @classmethod
    def _add_metrics(cls, name: str, value: float) -> JSON:
        return {
            "type": "rich_text_section",
            "elements": [
                {"type": "text", "text": f" {name} is currently at {value:.2f}"},
            ],
        }

    @classmethod
    def _add_process(
        cls,
        *,
        uid: int,
        pid: int,
        cmdline: str,
        runtime: float | None = None,
        cpu_mem: tuple[float, float] | None = None,
    ) -> JSON:
        username = get_username(uid)

        elements: JSON = [
            {"type": "text", "text": f"Process {pid} ("},
            {"type": "text", "style": {"italic": True}, "text": username},
            {"type": "text", "text": ")"},
        ]

        if runtime is not None:
            elements.append(
                {
                    "type": "text",
                    "text": f" running for {format_time(runtime)}",
                },
            )

        if cpu_mem is not None:
            cpu, mem = cpu_mem
            elements.append(
                {
                    "type": "text",
                    "text": f" using {cpu:.1f} CPUs and {mem:.1f}% memory",
                },
            )

        if len(cmdline) > 200:
            cmdline = cmdline[:195] + "[...]"

        elements.append({"type": "text", "text": ": "})
        elements.append({"type": "text", "style": {"code": True}, "text": cmdline})

        return {
            "type": "rich_text_section",
            "elements": elements,
        }

    def _send_message(self, blocks: list[JSON]) -> bool:
        data = {"blocks": blocks}
        any_errors = False
        for url in self._webhooks:
            self._log.debug("sending blocks to slack at %r", url)
            try:
                result = requests.post(
                    url,
                    data=json.dumps(data),
                    headers={
                        "content-type": "application/json",
                    },
                    timeout=self._timeout,
                )
            except requests.exceptions.RequestException as error:
                self._log.error("request to slack webhook %r failed: %s", url, error)
                any_errors = True
                continue

            if result.status_code != 200:
                self._log.error(
                    "request to slack webhook %r failed with %s",
                    url,
                    result.status_code,
                )
                self._log.error("for request %s", data)
                any_errors = True

        return not any_errors


@dataclass
class Config:
    slack_webhooks: list[str]
    process_blacklist: list[str] = field(default_factory=list[str])
    process_whitelist: list[str] = field(default_factory=list[str])

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: object = tomli.load(handle)

        validator = DataclassValidator(Config)
        result = validator(toml)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val


def setup_logging(args: Args) -> None:
    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(message)s",
        level=args.log_level,
    )


@dataclass
class Args:
    config: PosixPath
    dry_run: bool
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
    loop: float
    slack_timeout: float

    loadavg_measure: Literal[1, 5, 15]
    loadavg_step: float
    cpu_step: float
    memory_step: float

    min_process_uid: int
    max_process_runtime: float


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        )
    )

    parser.add_argument(
        "config",
        metavar="TOML",
        type=Path,
        help="Path to TOML file containing notification configuration",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log updates that would be sent instead of sending them",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Verbosity level for console logging",
    )
    parser.add_argument(
        "--slack-timeout",
        metavar="S",
        type=float,
        default=30.0,
        help="Timeout used for POST requests to Slack API end-points",
    )
    parser.add_argument(
        "--loop",
        metavar="S",
        type=float,
        default=60.0,
        help="Check for updates every S seconds",
    )

    group = parser.add_argument_group("Measurements")
    group.add_argument(
        "--loadavg-measure",
        metavar="MIN",
        type=int,
        default=5,
        choices=(1, 5, 15),
        help="Use loadavg for the last MIN minutes",
    )

    group.add_argument(
        "--loadavg-step",
        metavar="X",
        type=float,
        default=15.0,
        help="Issue alerts every X increase in load average",
    )
    group.add_argument(
        "--cpu-step",
        metavar="X",
        type=float,
        default=15.0,
        help="Issue alerts every X percent increase in load average",
    )
    group.add_argument(
        "--memory-step",
        metavar="X",
        type=float,
        default=15.0,
        help="Issue alerts every X percent increase in memory usage",
    )

    group = parser.add_argument_group("Processes")
    group.add_argument(
        "--min-process-uid",
        metavar="X",
        type=int,
        default=1000,
        help="Ignore processes belonging a lower UID (e.g. system processes)",
    )
    group.add_argument(
        "--max-process-runtime",
        metavar="X",
        type=float,
        default=10.0 * 60.0,
        help="Issue alert if blacklisted process has run for more than X seconds",
    )

    args = parser.parse_args(argv)
    validator = DataclassValidator(Args)
    result = validator(vars(args))
    if not isinstance(result, Valid):
        abort("Error parsing command-line arguments: %s", result.err_type)

    return result.val


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    setup_logging(args)

    _info("Loading TOML config from %r", str(args.config))
    config = Config.load(args.config)

    notifier = SlackNotifier(
        webhooks=config.slack_webhooks,
        timeout=args.slack_timeout,
        host=socket.gethostname(),
    )

    monitor = Monitor(
        process_whitelist=config.process_whitelist,
        process_blacklist=config.process_blacklist,
        loadavg_measure=args.loadavg_measure,
        min_process_uid=args.min_process_uid,
        max_process_age=args.max_process_runtime,
    )
    steps: dict[Metrics, float] = {
        "LoadAvg": args.loadavg_step,
        "%CPU": args.cpu_step,
        "Memory": args.memory_step,
    }
    thresholds = dict.fromkeys(steps, 0.0)

    while True:
        time.sleep(args.loop)

        stats: dict[Metrics, float] = {}
        summary = monitor.get()
        for key, value in summary.system.items():
            step = steps[key]
            threshold = thresholds[key]

            if value > threshold + step:
                stats[key] = value
                thresholds[key] = value

                _info(
                    "Exceeded %s threshold: %.2f > %.2f; next warning at %.2f",
                    key,
                    value,
                    threshold,
                    thresholds[key] + step,
                )

            elif value + step < threshold:
                _info("Lowering %s threshold from %.2f to %.2f", key, threshold, value)
                thresholds[key] = value

        summary.system = stats
        if procs := summary.top_processes():
            for proc in sorted(procs, key=lambda it: -max(it.mem, it.cpu)):
                _info("  proc %i (%s): %s", proc.pid, get_username(proc.uid), proc.cmd)

        notifier.notify(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
