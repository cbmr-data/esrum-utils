#!/opt/software/python/3.11.3/bin/python3
# pyright: strict
import argparse
import contextlib
import datetime
import functools
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from itertools import zip_longest
from typing import IO, Any, Callable, Dict, List, NoReturn, Optional, Sequence, Union

# MB of memory allocated per CPU by default; see DefMemPerCPU in /etc/slurm/slurm.conf
_DEFAULT_MEM_PER_CPU = 15948

_RE_ELAPSED = re.compile(r"(?:(\d+)-)?(?:(\d{2}):)?(\d{2}):(\d{2}\.?\d*)")

_STATE_WHITELIST = frozenset(
    ("COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL", "PREEMPTED", "TIMEOUT")
)

__VERSION__ = (2024, 8, 29, 1)
__VERSION_STR__ = "{}{:02}{:02}.{}".format(*__VERSION__)


#######################################################################################
# Utility functions


def abort(*values: object) -> NoReturn:
    print(*values, file=sys.stderr)
    sys.exit(1)


#######################################################################################
# sacct output parsing functions


def parse_time_to_h(value: str) -> float:
    match = _RE_ELAPSED.match(value)
    if match is None:
        raise ValueError(value)

    days, hours, minutes, seconds = match.groups()

    if days is None:
        days = "0"
    if hours is None:
        hours = "0"

    return int(days) * 24 + int(hours) + int(minutes) / 60 + float(seconds) / 3600


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


def parse_rss_to_mb(value: str) -> float:
    if not value.strip():
        return 0.0
    elif value.endswith("K"):
        return float(value[:-1]) / 1042
    elif value.endswith("M"):
        return float(value[:-1])

    raise ValueError(value)


#######################################################################################
# output formatting functions


def format_h(hours: float) -> str:
    return "{:02}:{:02}:{:02}s".format(
        int(hours),
        int((hours - int(hours)) * 60),
        int((hours - int(hours)) * 60 * 60 % 60),
    )


def print_table(
    table: List[List[str]],
    is_numerical: Sequence[bool] = (),
    column_separator: Optional[str] = None,
    out: IO[str] = sys.stdout,
) -> None:
    if column_separator is None or column_separator.lower() in ("space", "spaces"):
        sep = "  "
        widths = []
        for row in table:
            widths = [
                max(width, len(it))
                for width, it in zip_longest(widths, row, fillvalue=0)
            ]

        padded_table: List[List[str]] = []
        for row in table:
            result: List[str] = []
            for idx, (width, value) in enumerate(zip(widths, row)):
                if is_numerical and is_numerical[idx]:
                    result.append(value.rjust(width))
                else:
                    result.append(value.ljust(width))

            padded_table.append(result)

        table = padded_table
    elif column_separator.lower() in ("tab", "tabs"):
        sep = "\t"
    else:
        sep = column_separator

    with contextlib.suppress(KeyboardInterrupt):
        for row in table:
            print(*row, sep=sep, file=out)


#######################################################################################


@dataclass
class Usage:
    job: str
    user: str
    cpus: int
    cpu_total: float
    mem: float
    mem_total: float
    mem_default: float
    elapsed: float
    state: str
    age: float

    @property
    def has_measurements(self) -> bool:
        return not math.isnan(self.cpu_total)

    @property
    def cpu_utilization(self):
        if self.elapsed > 0:
            return min(1.0, (self.cpu_total / self.elapsed) / self.cpus)

        return float("nan")

    @property
    def extra_mem(self) -> float:
        return max(0, self.mem - self.mem_default)

    @property
    def extra_mem_utilization(self):
        extra_mem = self.extra_mem
        if extra_mem:
            return max(0, self.mem - max(self.mem_total, extra_mem)) / extra_mem

        return 1.0


def parse_usage(text: str, args: argparse.Namespace) -> List[Usage]:
    now = datetime.datetime.now()

    jobs: Dict[str, Usage] = {}
    lines = text.split("\n")
    header = lines[0].rstrip().split("|")
    for line in lines[1:]:
        if not line.strip():
            continue

        row = dict(zip(header, line.rstrip().split("|")))
        jobid = row["JobID"]
        state: str = row["State"].split()[0]

        if "." not in jobid:
            if "_" in jobid:
                jobid, arrayid = jobid.split("_", 1)
                jobid = f"{jobid}[{arrayid}]"

            if row["End"] == "Unknown":
                age = 0
            else:
                delta = now - datetime.datetime.strptime(
                    row["End"], "%Y-%m-%dT%H:%M:%S"
                )
                age = delta.total_seconds() / 60 / 60

            # A CPU time of 00:00:00 indicates that no statistics were collected
            cpu_total = mem_total = float("nan")
            if row["TotalCPU"] != "00:00:00":
                cpu_total = parse_time_to_h(row["TotalCPU"])
                mem_total = parse_rss_to_mb(row["MaxRSS"])

            cpus = int(row["AllocCPUS"])
            jobs[jobid] = Usage(
                job=jobid,
                user=row["User"],
                cpus=cpus,
                cpu_total=cpu_total,
                mem=parse_requested_mem_to_mb(row["ReqMem"], cpus),
                mem_default=cpus * _DEFAULT_MEM_PER_CPU,
                mem_total=mem_total,
                elapsed=parse_time_to_h(row["Elapsed"]),
                state=state,
                age=age,
            )
        else:
            jobid, _ = jobid.split(".", 1)
            if "_" in jobid:
                jobid, arrayid = jobid.split("_", 1)
                jobid = f"{jobid}[{arrayid}]"

            if jobid in jobs:
                jobs[jobid].mem_total += parse_rss_to_mb(row["MaxRSS"])

    return list(jobs.values())


def sort_table(
    table: List[Dict[str, Union[str, int, float]]],
    columns: List[str],
    sort_key: str,
) -> None:
    sort_key = sort_key.strip()
    reverse_sort = True
    while sort_key.startswith("-"):
        reverse_sort = not reverse_sort
        sort_key = sort_key[1:].strip()

    sort_key = sort_key.lower()
    for key in columns:
        if key.lower() == sort_key:
            sort_key = key
            break
    else:
        abort(f"ERROR: Sort key {sort_key!r} does not exit!")

    table.sort(key=lambda it: it[sort_key], reverse=reverse_sort)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
        epilog="WARNING: The ``Wasted`` statistics are based on snapshots of "
        "resource usage produced by Slurm and are therefore not 100% accurate. "
        "Notably, the memory usage statistics are based on maximum memory usage of "
        "individual processes, rather than the maximum cumulative memory usage, and "
        "may therefore greatly overestimate wasted memory if you are running multiple "
        "simultaneous processes in a pipeline.",
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"{os.path.basename(__file__)} v{__VERSION_STR__}",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show extra information: Mem and MemWasted",
    )
    parser.add_argument(
        "--filter",
        dest="filter",
        action="store_true",
        help="Filter jobs according to the --min-* options",
    )
    parser.add_argument(
        "--min-cpus",
        metavar="N",
        default=2,
        help="Ignore jobs with less than this number of CPUs, regardless of utilization",
    )
    parser.add_argument(
        "--min-cpu-utilization",
        metavar="X",
        default=1.00,
        help="Ignore jobs with a greater than or equal CPU utilization",
    )
    parser.add_argument(
        "--min-mem-utilization",
        metavar="X",
        default=1.00,
        help="Ignore jobs with a greater than or equal RAM utilization",
    )
    parser.add_argument(
        "--min-runtime",
        metavar="X",
        default=30 / 60 / 60,
        help="Ignore jobs with a runtime less than this value in seconds",
    )
    parser.add_argument(
        "--sort-key",
        default="CPUHoursWasted",
        help="Column name to sort by; prefix with `-` to reverse sort (ascending)",
    )
    parser.add_argument(
        "--column-separator",
        default="spaces",
        help="Character or characters to use to separate columns in the output. "
        "Possible values are 'spaces', 'commas', or any single character. By "
        "default columns are aligned using spaces for readability",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-a",
        "--allusers",
        "--all-users",
        dest="all_users",
        action="store_true",
        help="Display all users' jobs",
    )
    group.add_argument(
        "-u",
        "--uid",
        "--user",
        dest="user",
        metavar="USERS",
        help="Display the specified user's jobs; see `man sacct`",
    )

    parser.add_argument(
        "-S",
        "--starttime",
        "--start-time",
        dest="start_time",
        metavar="TIME",
        help="Show tasks starting at this time; see `man sacct` for format",
    )
    parser.add_argument(
        "-E",
        "--endtime",
        "--end-time",
        metavar="TIME",
        dest="end_time",
        help="Show tasks ending at this time; see `man sacct` for format",
    )
    # TODO: Impl
    parser.add_argument(
        "-T",
        "--truncate",
        dest="truncate",
        action="store_true",
        help="Truncate time; see `man sacct`",
    )
    parser.add_argument(
        "-g",
        "--gid",
        "--group",
        metavar="GROUP",
        dest="group",
        help="Show jobs belonging to group(s); see `man sacct`",
    )

    parser.add_argument(
        "-j",
        "--jobs",
        metavar="job(.step)",
        dest="jobs",
        help="Show only the specified jobs; see `man sacct`",
    )

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    if len(args.column_separator) != 1 and args.column_separator not in (
        "tab",
        "tabs",
        "space",
        "spaces",
    ):
        abort(
            "Invalid --column-separator option; must be 'tabs', 'spaces', or a single "
            f"character, not {args.column_separator!r}!"
        )

    command = [
        "sacct",
        "--parsable2",
        "--format=JobID,AllocCPUS,Elapsed,MaxRSS,MaxRSS,ReqMem,State,TotalCPU,User,End",
    ]

    if args.start_time is not None:
        command.append(f"--starttime={args.start_time}")
    if args.end_time is not None:
        command.append(f"--endtime={args.end_time}")

    if args.all_users:
        command.append("--allusers")
    elif args.user is not None:
        command.append(f"--user={args.user}")

    if args.truncate:
        command.append("--truncate")
    if args.group is not None:
        command.append(f"--group={args.group}")
    if args.jobs is not None:
        command.append(f"--jobs={args.jobs}")

    # Default to showing something more useful
    if args.start_time is None and args.jobs is None:
        command.append("--starttime=now-24hours")

    proc = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )

    stdout, _ = proc.communicate()
    if proc.returncode:
        return proc.returncode

    rows: List[Dict[str, Union[str, int, float]]] = []
    for it in parse_usage(stdout, args=args):
        if (it.state in _STATE_WHITELIST) and (
            not args.filter
            or args.jobs is not None
            or (
                it.elapsed >= args.min_runtime
                and (it.cpus >= args.min_cpus or it.extra_mem > 0)
                and (
                    it.cpu_utilization < args.min_cpu_utilization
                    or it.extra_mem_utilization < args.min_mem_utilization
                )
            )
        ):
            extra_mem = max(0, it.mem - it.cpus * _DEFAULT_MEM_PER_CPU)

            if it.has_measurements:
                wasted_cpus = it.cpus * (1.0 - it.cpu_utilization)
                wasted_mem = it.mem - it.mem_total
                wasted_extra_mem = max(
                    0, it.mem - max(it.mem_total, it.cpus * _DEFAULT_MEM_PER_CPU)
                )

                # The amount of CPUs hours wasted due to lack of use and due to un-used
                # extra reserved memory preventing CPUs from being reserved
                wasted_cpu_hours = (
                    wasted_cpus
                    + (wasted_extra_mem + _DEFAULT_MEM_PER_CPU - 1.0)
                    // _DEFAULT_MEM_PER_CPU
                ) * it.elapsed
            else:
                wasted_cpus = float("nan")
                wasted_mem = float("nan")
                wasted_extra_mem = float("nan")
                wasted_cpu_hours = float("nan")

            rows.append(
                {
                    "Age": it.age,
                    "User": it.user,
                    "Job": it.job,
                    "State": it.state,
                    "Elapsed": it.elapsed,
                    "CPUs": it.cpus,
                    "CPUsWasted": wasted_cpus,
                    "Mem": it.mem / 1024.0,
                    "MemWasted": wasted_mem / 1024.0,
                    "ExtraMem": extra_mem / 1024.0,
                    "ExtraMemWasted": wasted_extra_mem / 1024.0,
                    "CPUHoursWasted": wasted_cpu_hours,
                }
            )

    verbose_columns: Dict[str, Union[str, Callable[[Any], str]]] = {}
    if args.verbose:
        verbose_columns = {
            "Mem": "{:.1f}",
            "MemWasted": "{:.1f}",
        }

    columns: Dict[str, Union[str, Callable[[Any], str]]] = {
        "Age": format_h,
        "User": "{}",
        "Job": "{}",
        "State": "{}",
        "Elapsed": format_h,
        "CPUs": "{}",
        "CPUsWasted": "{:.1f}",
        **verbose_columns,
        "ExtraMem": "{:.1f}",
        "ExtraMemWasted": "{:.1f}",
        "CPUHoursWasted": "{:.2f}",
    }

    sort_table(
        table=rows,
        columns=list(columns),
        sort_key=args.sort_key,
    )

    table: List[List[str]] = [list(columns)]
    is_numerical = [True] * len(columns)
    for row in rows:
        values: List[str] = []
        for idx, (key, formatter) in enumerate(columns.items()):
            value = row[key]

            if isinstance(value, float) and math.isnan(value):
                values.append("NA")
            elif callable(formatter):
                values.append(formatter(value))
            else:
                values.append(formatter.format(value))

            is_numerical[idx] = is_numerical[idx] and isinstance(value, (int, float))

        table.append(values)

    print_table(
        table=table,
        is_numerical=is_numerical,
        column_separator=args.column_separator,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
