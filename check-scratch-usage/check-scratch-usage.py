#!/usr/bin/env python3.11
# pyright: strict
from __future__ import annotations

import argparse
import contextlib
import functools
import json
import socket
import subprocess
import sys
from itertools import zip_longest

LOCATIONS = {
    "root": "/",
    "tmp": "/tmp",  # noqa: S108
    "var-tmp": "/var/tmp",  # noqa: S108
    "scratch": "/scratch",
}


def print_table(table: list[list[str]]) -> None:
    if sys.stdout.isatty():
        sep = "  "
        widths = []
        for row in table:
            widths = [
                max(width, 0 if isinstance(it, int) else len(it))
                for width, it in zip_longest(widths, row, fillvalue=0)
            ]

        # The final column should not be padded
        widths[-1] = 0

        padded_table: list[list[str]] = []
        for row in table:
            result: list[str] = []
            for width, value in zip(widths, row):
                result.append(value.ljust(width))

            padded_table.append(result)

        table = padded_table
    else:
        sep = "\t"

    with contextlib.suppress(KeyboardInterrupt):
        for row in table:
            print(*row, sep=sep)


def gethostname() -> str:
    return socket.gethostname().split(".", 1)[0]


def to_gb(size: str | int) -> str:
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
    nodes: dict[str, dict[str, str] | None] = {}
    for node in run(["sinfo", "--noheader", "--format", "%N"]).split(","):
        if node := node.strip():
            nodes[node] = None

    # Collect statistics for the current/head node
    nodes[gethostname()] = json.loads(run([sys.executable, __file__, "check"]).strip())

    for partition in ("standardqueue", "gpuqueue"):
        stdout = run(
            [
                "/usr/bin/srun",
                # Run on all partitions
                f"--partition={partition}",
                # Run on all (available) nodes
                "--spread-job",
                sys.executable,
                __file__,
                "check",
            ],
        )

        for line in stdout.splitlines():
            row = json.loads(line)
            if nodes.get(row["host"]):
                raise AssertionError(f"duplicate node {row}")

            nodes[row["host"]] = row

    rows = [["host", *LOCATIONS, *(f"{it}-free" for it in LOCATIONS)]]
    for host, row in sorted(nodes.items()):
        output: list[str] = [host]
        remaining: list[str] = []

        if row is None:
            remaining.extend("NA" for _ in range(len(LOCATIONS) * 2))
        else:
            for loc in LOCATIONS:
                output.append(to_gb(row[loc]))
                remaining.append(to_gb(row[f"{loc}-free"]))

        output.extend(remaining)
        rows.append(output)

    print_table(rows)

    return 0


def main_check(_: argparse.Namespace) -> int:
    results: dict[str, str] = {
        "host": gethostname(),
    }

    for key, path in LOCATIONS.items():
        # Collect utilization and capacity in KB (1024)
        row = run(["df", "-kP", path]).splitlines()[-1].split()
        results[key] = row[2]
        results[f"{key}-free"] = row[3]

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
