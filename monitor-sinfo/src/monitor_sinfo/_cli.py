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

from monitor_sinfo._utilities import abort


@dataclass
class Args:
    config: PosixPath
    state: PosixPath
    sinfo: str
    verbose: bool
    dry_run: bool
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
    loop: int | None
    slack_timeout: float


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
        "state",
        metavar="DB",
        type=Path,
        help="File used to recording the state nodes",
    )
    parser.add_argument(
        "--sinfo",
        type=str,
        default="/usr/bin/sinfo",
        help="Path to/name of sinfo executable",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Send updates on all state changes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
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
        default=None,
        help="Check for updates every S seconds, instead of exiting immediately",
    )

    validator = DataclassValidator(Args)
    result = validator(vars(parser.parse_args(argv)))
    if not isinstance(result, Valid):
        abort("Error validating command-line arguments: %s", result.err_type)

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
