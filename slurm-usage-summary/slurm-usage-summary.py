#!/usr/bin/env python3
# pyright: strict
from __future__ import annotations

import argparse
import functools
import shlex
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, NoReturn

if TYPE_CHECKING:
    from typing_extensions import Literal


@dataclass
class Statistics:
    cpu_hours: float = 0
    gpu_hours: float = 0
    jobs: set[int] = field(default_factory=set)
    users: set[str] = field(default_factory=set)


def quote(value: str | Path) -> str:
    return shlex.quote(str(value))


def eprint(msg: str, *values: object) -> None:
    print(msg, *values, file=sys.stderr)


def abort(msg: str, *values: object) -> NoReturn:
    eprint("ERROR:", msg, *values)
    sys.exit(1)


def parse_time(value: str) -> datetime | None:
    if value == "Unknown":
        return None

    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")  # noqa: DTZ007


def parse_range(start: datetime, end: datetime) -> Iterable[tuple[date, float]]:
    if start > end:
        raise ValueError((start, end))

    while start.date() < end.date():
        tomorrow = datetime.combine(start.date() + timedelta(days=1), time())

        yield start.date(), (tomorrow - start).seconds / 60 / 60
        start = tomorrow

    if start < end:
        yield start.date(), (end - start).seconds / 60 / 60


def parse_requested_mem_to_mb(value: str, cores: int) -> float:
    if value.endswith("n"):
        multiplier = 1
        value = value[:-1]
    elif value.endswith("c"):
        multiplier = cores
        value = value[:-1]
    else:
        raise ValueError(value)

    if value.endswith("T"):
        multiplier *= 1024 * 1024
    elif value.endswith("G"):
        multiplier *= 1024
    elif value.isdigit():
        # Assume K
        value += "K"
    elif not value.endswith("M"):
        raise ValueError(value)

    return multiplier * float(value[:-1])


def read_xsv(
    filepath: Path,
    *,
    columns: Iterable[str],
    sep: str = "\t",
) -> list[dict[str, str]]:
    with filepath.open() as handle:
        return list(
            parse_xsv(
                handle,
                source=quote(filepath),
                columns=columns,
                sep=sep,
            )
        )


def parse_xsv(
    lines: Iterable[str],
    *,
    source: str,
    sep: str = "\t",
    columns: Iterable[str],
) -> Iterable[dict[str, str]]:
    it = iter(lines)
    try:
        header = next(it).rstrip().split(sep)
    except StopIteration:
        abort(f"{source} is empty")

    missing_columns = set(columns) - set(header)
    if missing_columns:
        abort(f"Required columns missing in source: {missing_columns}")

    for linenum, line in enumerate(map(str.rstrip, it), start=2):
        if line and not line.startswith("#"):
            row = line.split(sep)
            if len(row) != len(header):
                abort(f"Malformed line {linenum} in {source}")

            yield dict(zip(header, row))


def read_user_groups(filepath: Path | None) -> dict[str, str] | None:
    if filepath is not None:
        groups: dict[str, str] = {}
        for row in read_xsv(filepath, columns=("User", "Group")):
            groups[row["User"]] = row["Group"]

        return groups

    return None


def parse_sacct_output(text: str, *, source: str) -> list[tuple[date, Statistics]]:
    items: list[tuple[date, Statistics]] = []
    columns = ("User", "JobID", "Start", "End", "ReqCPUS", "Partition")
    for row in parse_xsv(text.splitlines(), source=source, columns=columns, sep="|"):
        if not row["User"]:
            continue  # Ignore sub-tasks; -X should normally omit those

        starttime = parse_time(row["Start"])
        endtime = parse_time(row["End"])

        # Ignore ongoing or never started jobs
        if starttime is not None and endtime is not None:
            for day, hours in parse_range(starttime, endtime):
                cpu_hours = hours * int(row["ReqCPUS"])
                gpu_hours = 0

                if row["Partition"] != "standardqueue":
                    cpu_hours, gpu_hours = gpu_hours, cpu_hours

                items.append(
                    (
                        day,
                        Statistics(
                            cpu_hours=cpu_hours,
                            gpu_hours=gpu_hours,
                            jobs={int(row["JobID"])},
                            users={row["User"].lower()},
                        ),
                    )
                )

    return items


def run_sacct(executable: Path, *, starttime: str) -> str:
    proc = subprocess.Popen(
        [
            executable,
            "-X",
            "--parsable2",
            "--format=User,JobID,ReqCPUs,Start,End,Partition",
            "--allusers",
            "--starttime",
            starttime,
        ],
        text=True,
        stdout=subprocess.PIPE,
    )

    stdout, _ = proc.communicate()
    if proc.returncode:
        abort(f"sacct command failed with return-code {proc.returncode}")

    return stdout


class Resolution:
    def __call__(self, value: date) -> tuple[str | int, ...]:
        raise NotImplementedError

    keys: tuple[str, ...]


class ResolutionDay(Resolution):
    def __call__(self, value: date) -> tuple[str | int, ...]:
        return (value.isoformat(),)

    keys = ("Date",)


class ResolutionWeek(Resolution):
    def __call__(self, value: date) -> tuple[str | int, ...]:
        return (value.year, value.isocalendar().week)

    keys = ("Year", "Week")


class ResolutionMonth(Resolution):
    def __call__(self, value: date) -> tuple[str | int, ...]:
        return (value.year, value.month)

    keys = ("Year", "Month")


class ResolutionYear(Resolution):
    def __call__(self, value: date) -> tuple[str | int, ...]:
        return (value.year,)

    keys = ("Year",)


def print_summary(
    *,
    sacct_output: list[tuple[date, Statistics]],
    user_groups: dict[str, str] | None,
    resolution: Resolution,
) -> int:
    columns: list[str] = list(resolution.keys)
    if user_groups:
        columns += ["Group", "Users"]
    else:
        columns += ["User"]
    columns += ["Jobs", "CPUNodeHours", "GPUNodeHours"]

    summary: dict[tuple[str | int, ...], Statistics] = defaultdict(Statistics)
    for timestamp, stats in sacct_output:
        (name,) = stats.users
        if user_groups:
            name = user_groups.get(name, "Unknown")

        totals = summary[(*resolution(timestamp), name)]
        totals.cpu_hours += stats.cpu_hours
        totals.gpu_hours += stats.gpu_hours
        totals.jobs.update(stats.jobs)
        totals.users.update(stats.users)

    print(*columns, sep="\t")
    for key, stats in sorted(summary.items()):
        row = list(key)
        if user_groups:
            row.append(len(stats.users))
        row += [len(stats.jobs), f"{stats.cpu_hours:.1f}", f"{stats.gpu_hours:.1f}"]

        print(*row, sep="\t")

    return 0


def print_report(
    *,
    sacct_output: list[tuple[date, Statistics]],
    user_groups: dict[str, str],
    resolution: Resolution,
) -> int:
    columns: list[str] = [
        *resolution.keys,
        "Group",
        "User",
        "Jobs",
        "CPUNodeHours",
        "GPUNodeHours",
    ]

    summary: dict[tuple[str | int, ...], Statistics] = defaultdict(Statistics)
    for timestamp, stats in sacct_output:
        (name,) = stats.users
        group = user_groups[name]

        totals = summary[(*resolution(timestamp), group, name)]
        totals.cpu_hours += stats.cpu_hours
        totals.gpu_hours += stats.gpu_hours
        totals.jobs.update(stats.jobs)
        totals.users.update(stats.users)

    print(*columns, sep="\t")
    for key, stats in sorted(summary.items()):
        print(
            *key,
            len(stats.jobs),
            f"{stats.cpu_hours:.1f}",
            f"{stats.gpu_hours:.1f}",
            sep="\t",
        )

    return 0


RESOLUTION_FUNCTIONS = {
    "day": ResolutionDay(),
    "week": ResolutionWeek(),
    "month": ResolutionMonth(),
    "year": ResolutionYear(),
}


class Args(argparse.Namespace):
    mode: Literal["summary", "report"]
    user_groups: Path | None
    sacct: Path
    sacct_output: Path | None
    sacct_starttime: str
    write_sacct_output: Path | None
    time_resolution: Literal["day", "week", "month", "year"]


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "mode",
        type=str.lower,
        choices=("summary", "report"),
        help="Either generate a 'summary' of usage over some time period or generate "
        "a daily, per-user report",
    )

    parser.add_argument(
        "user_groups",
        nargs="?",
        type=Path,
        help="Table containing mapping of 'User' names to 'Group' names. Depending on "
        "the mode of operation, users are either grouped by their group or the group "
        "is reported for each user",
    )

    parser.add_argument(
        "--sacct",
        type=Path,
        default=Path("sacct"),
        help="Location/name of the `sacct` executable",
    )
    parser.add_argument(
        "--sacct-starttime",
        default="now-365days",
        help="Report jobs from this starttime, defaulting to 1 year ago",
    )
    parser.add_argument(
        "--sacct-output",
        type=Path,
        help="Read sacct output from the specified file instead of running `sacct`",
    )
    parser.add_argument(
        "--write-sacct-output",
        type=Path,
        help="Write the `sacct` output to the specified file. This can then be reused "
        "via the `--sacct-output` option",
    )

    parser.add_argument(
        "--time-resolution",
        type=str.lower,
        choices=tuple(RESOLUTION_FUNCTIONS),
        default="month",
        help="Aggregate summaries by this time-unit",
    )

    return parser.parse_args(argv, namespace=Args())


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    # Read saved sacct output or invoke sacct directly
    if args.sacct_output is not None:
        raw_sacct_output = args.sacct_output.read_text()
        raw_sacct_source = quote(args.sacct_output)
    else:
        raw_sacct_output = run_sacct(args.sacct, starttime=args.sacct_starttime)
        raw_sacct_source = "sacct output"

    if args.write_sacct_output:
        args.write_sacct_output.write_text(raw_sacct_output)

    sacct_output = parse_sacct_output(raw_sacct_output, source=raw_sacct_source)
    user_groups = read_user_groups(args.user_groups)

    if user_groups is not None:
        unique_users: set[str] = set()
        for _, stats in sacct_output:
            unique_users.update(stats.users)

        unknown_users = unique_users - user_groups.keys()
        if unknown_users:
            abort("Unknown users:", unknown_users)

    if args.mode == "report":
        if user_groups is None:
            abort("--user-groups required for reports")

        return print_report(
            sacct_output=sacct_output,
            user_groups=user_groups,
            resolution=ResolutionDay(),
        )
    elif args.mode == "summary":
        return print_summary(
            sacct_output=sacct_output,
            user_groups=user_groups,
            resolution=RESOLUTION_FUNCTIONS[args.time_resolution],
        )
    else:
        raise AssertionError(f"invalid mode {args.mode!r}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
