# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import argparse
import functools
from dataclasses import dataclass
from pathlib import Path, PosixPath
from typing import Literal

import colorlog
from koda_validate import DataclassValidator, Valid

from monitor_stats._utils import abort


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


def setup_logging(args: Args) -> None:
    formatter = colorlog.ColoredFormatter(
        "%(asctime)s %(levelname)s %(log_color)s%(message)s"
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(formatter)

    logger = colorlog.getLogger()
    logger.setLevel(args.log_level)
    logger.addHandler(handler)
