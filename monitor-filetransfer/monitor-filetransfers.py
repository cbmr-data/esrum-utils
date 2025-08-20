#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "coloredlogs==15.0.1",
#     "koda-validate==4.1.1",
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
import os
import subprocess
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, NoReturn

import coloredlogs
import tomli
from koda_validate import DataclassValidator, Valid

_LOG = logging.getLogger("monitor-sinfo")

_debug = _LOG.debug
_error = _LOG.error
_info = _LOG.info
_warning = _LOG.warning
_log = _LOG.log


def abort(msg: str, *values: object) -> NoReturn:
    _error(msg, *values)
    sys.exit(1)


def format_size(size: int, *, delta: bool = False) -> str:
    for nth, label in ((4, "TB"), (3, "GB"), (2, "MB"), (1, "KB")):
        if size >= 1024**nth:
            pct = size / 1024**nth
            return f"{pct:+.1f} {label}" if delta else f"{pct:.1f} {label}"

    return f"{size:+} B" if delta else f"{size} B"


def send_notification(
    *,
    root: str,
    before: Result | None,
    after: Result,
    smtpserver: str,
    recipients: list[str],
) -> bool:
    def _fmt_s(r: Result, *, delta: bool = False) -> str:
        if delta:
            return f"{r.items:+,} items, {format_size(r.size, delta=delta)}"
        else:
            return f"{r.items:,} items, {format_size(r.size, delta=delta)}"

    def _fmt_l(r: Result) -> str:
        line = f"{r.timestamp}: {_fmt_s(r)}"
        if r.found is not None and r.expected is not None:
            line = f"{line} ({r.found * 100 / r.expected:.1f} %)"
        return line

    lines: list[str] = []
    if before and (before.items, before.size) == (after.items, after.size):
        subject = f"{root}: No changes"
        lines.append("No changes found:")
    else:
        delta = Result(
            timestamp=after.timestamp,
            items=after.items - (0 if before is None else before.items),
            size=after.size - (0 if before is None else before.size),
            expected=after.expected,
            found=after.found,
        )

        subject = f"{root}: {_fmt_s(delta, delta=True)}"

    if before is not None:
        lines.append(_fmt_l(before))

    lines.append(_fmt_l(after))
    body = "\n".join(lines)

    print(body)
    _debug("Sending email to %i recipients", len(recipients))
    try:
        proc = subprocess.Popen(
            [
                # "/usr/bin/echo",
                "/usr/bin/mail",
                "-S",
                f"smtp={smtpserver}",
                "-s",
                subject,
                *recipients,
            ],
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

        proc.communicate(input=body.encode("utf-8"))
    except OSError as error:
        _error("Error sending email notification: %s", error)
        return False

    return not proc.returncode


@dataclass
class Config:
    root: str
    database: str
    smtp_server: str
    email_recipients: list[str]

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: object = tomli.load(handle)

        validator = DataclassValidator(Config)
        result = validator(toml)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val


@dataclass
class Result:
    timestamp: str
    items: int = 0
    size: int = 0
    expected: int | None = None
    found: int | None = None

    @classmethod
    def load_last(cls, filename: str) -> Result | None:
        try:
            with Path(filename).open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(-min(size, 1024), os.SEEK_END)

                lines = handle.readlines()
        except FileNotFoundError:
            return None

        if not lines:
            return None

        record = json.loads(lines[-1])
        validator = DataclassValidator(Result)
        result = validator(record)
        if not isinstance(result, Valid):
            abort("Error parsing results file: %s", result.err_type)

        return result.val

    def append_to(self, filename: str) -> None:
        with Path(filename).open("+a") as handle:
            print(json.dumps(vars(self)), file=handle)


def count_files(root: Path, expected: set[Path]) -> Result:
    result = Result(timestamp=datetime.now().isoformat())  # noqa: DTZ005
    result.expected = len(expected)
    result.found = 0

    queue: deque[Path] = deque([root])
    while queue:
        it = queue.popleft()

        with contextlib.suppress(OSError):
            if not it.is_symlink() and it.is_dir():
                queue.extend(it.iterdir())

            stat = it.lstat()
            result.size += stat.st_size

        result.items += 1
        if it.relative_to(root) in expected:
            result.found += 1

    return result


def setup_logging(args: Args) -> None:
    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(message)s",
        level=args.log_level,
    )


@dataclass
class Args:
    config: Path
    expected: Path | None
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


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
        "expected",
        nargs="?",
        metavar="FILE",
        type=Path,
        help="File containing expected filenames",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Verbosity level for console logging",
    )

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    config = Config.load(args.config)
    last_results = Result.load_last(config.database)

    setup_logging(args)

    expected: set[Path] = set()
    if args.expected is not None:
        with args.expected.open() as handle:
            for line in handle:
                path = Path(line.strip())
                if path.is_absolute():
                    abort(f"Path in {args.expected} is absolute: {path}")

                expected.add(path)

    result = count_files(root=Path(config.root), expected=expected)
    if args.expected is None:
        result.expected = None

    send_notification(
        root=config.root,
        before=last_results,
        after=result,
        smtpserver=config.smtp_server,
        recipients=config.email_recipients,
    )

    # Write new results last, to prevent loss of updates due to sendmail failures
    result.append_to(config.database)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
