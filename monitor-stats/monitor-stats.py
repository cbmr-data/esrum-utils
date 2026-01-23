#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "coloredlogs==15.0.1",
#     "koda-validate==4.1.1",
#     "requests~=2.32.3",
#     "tomli==2.0.1",
#     "typing-extensions==4.11.0",
# ]
# exclude-newer = "2026-01-21T00:00:00Z"
# ///
from __future__ import annotations

import argparse
import functools
import json
import logging
import pwd
import re
import socket
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
from typing import Literal, NoReturn, TypeAlias, Union

import coloredlogs
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
        fields.append(f"{seconds:.1}")

    fields[-1] += "s"
    return ":".join(fields)


class Monitor:
    def __init__(
        self,
        process_filters: Iterable[str],
        min_process_uid: int,
        max_process_age: float,
    ) -> None:
        self._last_time: float = time.time()
        self._last_cpu_stat: float = 0.0

        self._process_filters: tuple[re.Pattern[str], ...] = tuple(
            re.compile(it) for it in process_filters
        )
        self._pid_whitelist: dict[int, float] = {}
        self._min_process_uid = min_process_uid
        self._max_process_age = max_process_age

        self.get()

    def get(self) -> dict[Metrics, float]:
        current_time = time.time()
        since_last = current_time - self._last_time
        self._last_time = current_time
        metrics: dict[Metrics, float] = {
            "%CPU": self._get_cpu_load(since_last),
            "LoadAvg": self._get_loadavg(since_last),
            "Memory": self._get_mem_usage(),
        }

        return metrics

    def get_processes(self) -> dict[int, tuple[int, str, float]]:
        processes: dict[int, tuple[int, str, float]] = {}

        if self._process_filters:
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

                    _debug("checking process %i with command %r", pid, cmdline)
                    if cmdline:
                        for flt in self._process_filters:
                            if flt.search(cmdline):
                                runtime = time.time() - stat.st_ctime
                                _debug("process %i is blacklisted", pid)
                                if runtime > self._max_process_age:
                                    _info(
                                        "Found blacklisted process %i (%s) running for "
                                        "%.1f seconds: %r",
                                        pid,
                                        get_username(stat.st_uid),
                                        runtime,
                                        cmdline,
                                    )
                                    processes[pid] = (stat.st_uid, cmdline, runtime)
                                    updated_whitelist[pid] = stat.st_ctime
                                break
                        else:
                            updated_whitelist[pid] = stat.st_ctime
                    else:
                        updated_whitelist[pid] = stat.st_ctime

            self._pid_whitelist = updated_whitelist

        return processes

    @classmethod
    def _get_loadavg(cls, since_last: float) -> float:
        with PATH_LOAD_AVERAGE.open("rb") as handle:
            loadavg = handle.readline().split(None)

        if since_last <= 90:
            return float(loadavg[0])  # avg. last minute
        elif since_last <= 7.5 * 60:
            return float(loadavg[1])  # avg. last five minutes
        else:
            return float(loadavg[2])  # avg. last fifteen minutes

    def _get_cpu_load(self, since_last: float) -> float:
        cpus = 0
        jiffies = 0
        with PATH_CPU_STATS.open("rb") as handle:
            for line in handle:
                if line.startswith(b"cpu "):
                    stats = line.split()
                    jiffies += float(stats[1]) + float(stats[2]) + float(stats[3])
                elif line.startswith(b"cpu"):
                    cpus += 1

        self._last_cpu_stat, last_cpu_stat = jiffies, self._last_cpu_stat

        return (jiffies - last_cpu_stat) / cpus / since_last

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

    def notify(
        self,
        *,
        stats: dict[Metrics, float],
        processes: dict[int, tuple[int, str, float]],
    ) -> bool:
        if not self._webhooks:
            self._log.warning("Slack LDAP update not sent; no webhooks configured")
            return False
        elif not (stats or processes):
            return False

        alerts: JSON = []

        if stats:
            alerts.extend(
                self._add_entry(
                    f"Resource usage at {self._host} exceeds tresholds",
                    [self._add_metrics(key, value) for key, value in stats.items()],
                )
            )

        if processes:
            alerts.extend(
                self._add_entry(
                    f"Blacklisted process is running on {self._host}",
                    [
                        self._add_process(uid, pid, cmdline, runtime)
                        for pid, (uid, cmdline, runtime) in processes.items()
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
    def _add_entry(cls, message: str, elements: JSON) -> Iterable[JSON]:
        yield {
            "type": "rich_text_section",
            "elements": [
                {"type": "text", "text": f"{message} "},
                {"type": "emoji", "name": "warning"},
            ],
        }

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
    def _add_process(cls, uid: int, pid: int, cmdline: str, runtime: float) -> JSON:
        username = get_username(uid)

        return {
            "type": "rich_text_section",
            "elements": [
                {"type": "text", "text": f"Process {pid} ("},
                {"type": "text", "style": {"italic": True}, "text": username},
                {
                    "type": "text",
                    "text": f") has been running for {format_time(runtime)}: ",
                },
                {
                    "type": "text",
                    "style": {"code": True},
                    "text": cmdline,
                },
            ],
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
        "--loadavg-step",
        metavar="X",
        type=float,
        default=10.0,
        help="Issue alerts every X increase in load average",
    )
    group.add_argument(
        "--cpu-step",
        metavar="X",
        type=float,
        default=10.0,
        help="Issue alerts every X percent increase in load average",
    )
    group.add_argument(
        "--memory-step",
        metavar="X",
        type=float,
        default=10.0,
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
        process_filters=config.process_blacklist,
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
        for key, value in monitor.get().items():
            step = steps[key]
            threshold = thresholds[key]

            if value > threshold + step:
                stats[key] = value
                thresholds[key] = value

                _info(
                    "%s exceeded threshold: %.2f > %.2f; next warning at %.2f",
                    key,
                    value,
                    threshold,
                    thresholds[key] + step,
                )
            elif value + step < threshold:
                _debug("%s lowering threshold from %.2f to %.2f", key, threshold, value)
                thresholds[key] = value

        processes = monitor.get_processes()

        if stats or processes:
            notifier.notify(stats=stats, processes=processes)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
