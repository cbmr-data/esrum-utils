#!/usr/bin/env python3.11
# pyright: strict
from __future__ import annotations

import argparse
import functools
import json
import socket
import subprocess
import sys
from typing import TypedDict


class Stats(TypedDict):
    host: str
    root: str
    tmp: str
    scratch: str


def to_gb(size: str) -> str:
    return f"{int(size) / 1024 / 1024:.1f}"


def run(command: list[str]) -> str:
    proc = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
    )

    stdout, _ = proc.communicate()
    if not proc.returncode:
        return stdout.decode("utf-8")

    sys.exit(1)


def main_list(_: argparse.Namespace) -> int:
    rows: list[Stats] = [
        # Collect statistics for the current/head node
        json.loads(run([sys.executable, __file__, "check"]).strip()),
    ]

    for partition in ("standardqueue", "gpuqueue"):
        stdout = run(
            [
                "/usr/bin/srun",
                # Run on all partitions
                f"--partition={partition}",
                # Run on all (available) nodes
                "--nodes=1-1024",
                "--immediate=5",
                sys.executable,
                __file__,
                "check",
            ],
        )

        for line in stdout.splitlines():
            rows.append(json.loads(line))

    print("host", "root", "tmp", "scratch", sep="\t")
    for row in sorted(rows, key=lambda it: it["host"]):
        print(
            row["host"],
            to_gb(row["root"]),
            to_gb(row["tmp"]),
            to_gb(row["scratch"]),
            sep="\t",
        )

    return 0


def main_check(_: argparse.Namespace) -> int:
    results: Stats = {
        "host": socket.gethostname(),
        "scratch": "",
        "tmp": "",
        "root": "",
    }

    for path, key in (("/", "root"), ("/tmp", "tmp"), ("/scratch", "scratch")):  # noqa: S108
        # Collect utilization in KB (1024)
        results[key] = run(["df", "-kP", path]).splitlines()[-1].split()[2]

    print(json.dumps(results))

    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "command",
        choices=("check", "list"),
        default="list",
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.command == "check":
        return main_check(args)
    elif args.command == "list":
        return main_list(args)

    raise NotImplementedError(args.command)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
