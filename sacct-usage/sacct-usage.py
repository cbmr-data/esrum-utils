#!/usr/bin/python3.11
# pyright: strict
from __future__ import annotations

import argparse
import contextlib
import datetime
import functools
import getpass
import math
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator, Sequence
from itertools import zip_longest
from pathlib import Path
from typing import IO, Any, Callable, Literal, NoReturn

# MB of memory allocated per CPU by default; see DefMemPerCPU in /etc/slurm/slurm.conf
_DEFAULT_MEM_PER_CPU = 15948 / 1024

_RE_ELAPSED = re.compile(r"(?:(\d+)-)?(?:(\d{2}):)?(\d{2}):(\d{2}\.?\d*)")

__VERSION__ = (2025, 11, 13, 1)
__VERSION_STR__ = "{}{:02}{:02}.{}".format(*__VERSION__)


#######################################################################################
# Utility functions


def abort(*values: object) -> NoReturn:
    print(*values, file=sys.stderr)
    sys.exit(1)


def run_command(command: list[str]) -> str | None:
    proc = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
    )

    stdout, _ = proc.communicate()
    return None if proc.returncode else stdout


def parse_slurm_output(text: str) -> Iterator[dict[str, str]]:
    lines = text.split("\n")
    header = lines[0].rstrip().split("|")
    for line in lines[1:]:
        if line := line.strip():
            yield dict(zip(header, line.split("|"), strict=True))


def values_or_nan(values: Iterable[float]) -> list[float]:
    """Return list of non-NAN values, or NAN if therer are no non-NAN values"""
    values = [value for value in values if not math.isnan(value)]

    return values if values else [math.nan]


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


def parse_n_to_gb(value: str) -> float:
    if value.isdigit():
        multiplier = 1
    else:
        if value.endswith("K"):
            multiplier = 1042**-2
        elif value.endswith("M"):
            multiplier = 1024**-1
        elif value.endswith("G"):
            multiplier = 1
        elif value.endswith("T"):
            multiplier = 1024
        else:
            raise ValueError(value)

        value = value[:-1]

    return float(value) * multiplier


def parse_requested_mem_to_gb(value: str, cores: int) -> float:
    if value.endswith("n"):
        multiplier = 1
        value = value[:-1]
    elif value.endswith("c"):
        multiplier = cores
        value = value[:-1]
    else:
        raise ValueError(value)

    return multiplier * parse_n_to_gb(value)


def parse_rss_to_gb(value: str) -> float:
    if not value.strip():
        return 0.0

    return parse_n_to_gb(value)


#######################################################################################
# output formatting functions


def format_h(hours: float) -> str:
    seconds = int((hours - int(hours)) * 60 * 60 % 60)
    minutes = int((hours - int(hours)) * 60)
    hours = int(hours)

    return f"{hours:02}:{minutes:02}:{seconds:02}s"


def print_table(
    table: list[list[str]],
    is_numerical: Sequence[bool] = (),
    column_separator: str | None = None,
    out: IO[str] = sys.stdout,
) -> None:
    if column_separator is None or column_separator.lower() in ("space", "spaces"):
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


class Usage:
    def __init__(
        self,
        job: str,
        raw_job: str,
        user: str,
        name: str,
        state: str,
        cpus: int = 1,
        cpus_used: float = 0.0,
        mem: float = 0.0,
        mem_used: float = 0.0,
        elapsed: float = 0.0,
        start: datetime.datetime | None = None,
    ) -> None:
        self.job = job
        self.raw_job = raw_job
        self.user = user
        self.name = name
        self.state = state

        self._cpus = cpus
        self._cpus_used = cpus_used
        self._mem = mem
        self._mem_used = mem_used

        self.elapsed = elapsed
        self.start = start

        self.jobs: list[Usage] = []
        self.steps: list[Usage] = []

    @property
    def cpus(self) -> int:
        return sum(it.cpus for it in self.jobs) if self.jobs else self._cpus

    @property
    def cpus_used(self) -> float:
        if self.jobs:
            return sum(it.cpus_used for it in self.jobs)
        elif self.steps:
            return sum(values_or_nan(it.cpus_used for it in self.steps))

        return self._cpus_used

    @property
    def mem(self) -> float:
        return sum(it.mem for it in self.jobs) if self.jobs else self._mem

    @property
    def mem_used(self) -> float:
        if self.jobs:
            return sum(it.mem_used for it in self.jobs)
        elif self.steps:
            return max(values_or_nan(it.mem_used for it in self.steps))

        return self._mem_used

    @property
    def has_measurements(self) -> bool:
        return not math.isnan(self.cpus_used)

    @property
    def cpus_wasted(self) -> float:
        return self.cpus - self.cpus_used

    @property
    def cpu_hours(self) -> float:
        if self.jobs:
            return sum(it.cpu_hours for it in self.jobs)

        return self.elapsed * max(self.cpus, math.ceil(self.mem / _DEFAULT_MEM_PER_CPU))

    @property
    def overhead(self) -> float:
        if self.jobs:
            return sum(it.overhead for it in self.jobs)

        # Users are not required to fine-tune memory if they use <= default allocation
        mem_waste = max(0, self.mem - max(self.mem_used, self.default_mem))
        # Total CPUs wasted, including CPUs "blocked" by excessive memory allocations
        cpus_wasted = self.cpus_wasted + math.ceil(mem_waste / _DEFAULT_MEM_PER_CPU)

        return self.elapsed * cpus_wasted

    @property
    def default_mem(self) -> float:
        """The default/assumed memory entitlement"""
        return self.cpus * _DEFAULT_MEM_PER_CPU

    @property
    def mem_wasted(self) -> float:
        return max(0.0, self.mem - self.mem_used)

    def replace_usage(self, cpus_used: float, mem_used: float) -> None:
        self._cpus_used = cpus_used
        self._mem_used = mem_used


def run_sacct(
    *,
    start_time: str | None,
    end_time: str | None,
    all_users: bool,
    user: str | None,
    truncate: bool,
    group: str | None,
    jobs: str | None,
    state: str | None,
) -> str | None:
    command = [
        "sacct",
        "--parsable2",
        "--format=Start,JobID,JobIDRaw,AllocCPUS,Elapsed,MaxRSS,ReqMem,State,TotalCPU,User,"
        "JobName",
    ]

    # Avoid confusing output: would include finished jobs if starttime/endtime were set
    if not (state and state.upper() == "RUNNING"):
        if start_time is not None:
            command.append(f"--starttime={start_time}")
        if end_time is not None:
            command.append(f"--endtime={end_time}")

    if all_users:
        command.append("--allusers")
    elif user is not None:
        command.append(f"--user={user}")

    if truncate:
        command.append("--truncate")
    if group is not None:
        command.append(f"--group={group}")
    if jobs is not None:
        command.append(f"--jobs={jobs}")
    if state is not None:
        command.append(f"--state={state}")

    return run_command(command)


def parse_sacct(text: str) -> list[Usage]:
    jobs: dict[str, Usage] = {}
    for row in parse_slurm_output(text):
        jobid = row["JobID"]
        state: str = row["State"].split()[0]

        is_step = False
        if "." in jobid:
            is_step = True
            jobid, _ = jobid.split(".", 1)

        if "_" in jobid:
            jobid, arrayid = jobid.split("_", 1)
            if not arrayid.startswith("["):
                arrayid = f"[{arrayid}]"

            jobid = f"{jobid}{arrayid}"

        start: datetime.datetime | None = None
        if row["Start"] != "Unknown":
            start = datetime.datetime.fromisoformat(row["Start"])

        cpus = int(row["AllocCPUS"])
        cpus_used = mem_used = float("nan")
        elapsed = parse_time_to_h(row["Elapsed"])

        # A CPU time of 00:00:00 indicates that no statistics were collected
        if row["TotalCPU"] != "00:00:00":
            cpu_total = parse_time_to_h(row["TotalCPU"])
            if elapsed > 0:
                cpus_used = min(cpus, (cpu_total / elapsed))

            mem_used = parse_rss_to_gb(row["MaxRSS"])

        job = Usage(
            job=jobid,
            raw_job=row["JobIDRaw"],
            user=row["User"],
            name=row["JobName"],
            cpus=cpus,
            cpus_used=cpus_used,
            mem=parse_requested_mem_to_gb(row["ReqMem"], cpus),
            mem_used=mem_used,
            elapsed=elapsed,
            state=state,
            start=start,
        )

        if is_step:
            jobs[jobid].steps.append(job)
        else:
            jobs[jobid] = job

    return list(jobs.values())


def update_running_jobs(jobs: list[Usage]) -> bool:
    # 1. sstat identifies jobs by their "raw"/unique ID, not by job array IDs (if any).
    # 2. sstat does permit access to other users' jobs by default
    user = getpass.getuser()
    running_jobs: dict[str, Usage] = {
        job.raw_job: job
        for job in jobs
        if job.state == "RUNNING" and user in ("root", job.user)
    }

    if running_jobs:
        stdout = run_command(
            [
                "sstat",
                "--allsteps",
                "--parsable2",
                "--format",
                "JobID,MaxRSS,AveCPU",
                "--jobs",
                ",".join(sorted(running_jobs)),
            ]
        )

        if stdout is None:
            return False

        for row in parse_slurm_output(stdout):
            jobid, step = row["JobID"].split(".")
            # Extern steps do not provide useful information about the users' processes
            if step == "extern":
                continue

            # filter since sstat returns full array if the ID matches the main array ID
            if job := running_jobs.get(jobid):
                mem_used = parse_rss_to_gb(row["MaxRSS"])
                cpus_used = parse_time_to_h(row["AveCPU"]) / job.elapsed

                for step in job.steps:
                    if step.raw_job == row["JobID"]:
                        step.replace_usage(cpus_used=cpus_used, mem_used=mem_used)
                        break
                else:
                    job.steps.append(
                        Usage(
                            job=job.job,
                            user=job.user,
                            name=job.name,
                            state=job.state,
                            raw_job=row["JobID"],
                            cpus_used=cpus_used,
                            mem_used=mem_used,
                        )
                    )

    return True


def aggregate_statistics(jobs: list[Usage]) -> list[Usage]:
    by_user: dict[str, Usage] = {}
    for it in jobs:
        if it.has_measurements:
            merged = by_user.get(it.user)
            if merged is None:
                user = Usage(
                    job=it.user,
                    raw_job=it.user,
                    user=it.user,
                    name="*",
                    state="*",
                )
                user.jobs.append(it)
                by_user[it.user] = user
            else:
                merged.jobs.append(it)

    return list(by_user.values())


def sort_table(
    table: list[Usage],
    sort_key: str,
    getters: dict[str, Callable[[Usage], str] | Callable[[Usage], float]],
) -> None:
    sort_key = sort_key.strip()
    reverse_sort = True
    while sort_key.startswith("-"):
        reverse_sort = not reverse_sort
        sort_key = sort_key[1:].strip()

    sort_key = sort_key.lower()
    for key, func in getters.items():
        if key.lower() == sort_key:
            getter = func
            break
    else:
        abort(f"ERROR: Sort key {sort_key!r} does not exit!")

    def sort_with_na(row: Usage) -> str | float:
        value = getter(row)
        if isinstance(value, float) and math.isnan(value):
            return -math.inf

        return value

    table.sort(key=sort_with_na, reverse=reverse_sort)


def select_columns(
    *,
    mode: Literal["per-job", "per-user"],
    metric: Literal["Used", "Wasted", "Both"],
    show_overhead: bool,
) -> list[str]:
    columns: list[str] = []

    def add_used_and_or_wasted(key: str) -> None:
        columns.append(f"{key}Reserved")
        if metric in ("Used", "Both"):
            columns.append(f"{key}Used")
        if metric in ("Wasted", "Both"):
            columns.append(f"{key}Wasted")

    columns.append("User")

    if mode == "per-job":
        columns += ["Job", "Start", "Elapsed", "State"]
    else:
        columns.append("Jobs")

    add_used_and_or_wasted("CPUs")
    add_used_and_or_wasted("Mem")

    if show_overhead:
        columns.append("Overhead")

    if mode == "per-job":
        columns.append("Name")

    return columns


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"{Path(__file__).name} v{__VERSION_STR__}",
    )

    parser.add_argument(
        "--mode",
        type=str.lower,
        default="per-job",
        choices=("per-job", "per-user"),
        help="Either show per-job or per-user statistics; 'per-user' implies "
        "--allusers, unless the --user option is used",
    )

    parser.add_argument(
        "--metric",
        type=str.title,
        default="Used",
        choices=("Used", "Wasted", "Both"),
        help="Show either resources used or wasted, relative to reservations",
    )

    # deprecated in favor of just showing complete information by default
    parser.add_argument("--verbose", action="store_true", help=argparse.SUPPRESS)

    parser.add_argument(
        "--sort",
        "--sort-key",
        dest="sort_key",
        default="Start",
        help="Column name to sort by; prefix with `-` to reverse sort (ascending)",
    )
    parser.add_argument(
        "--column-separator",
        default="auto",
        help="Character or characters to use to separate columns in the output. "
        "Possible values are 'spaces', 'commas', or any single character. By "
        "default columns are aligned using spaces when outputting to a terminal, "
        "and tabs when outputting to a pipe",
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
        default="now-24hours",
        help="Show tasks starting at this time; see `man sacct` for format",
    )
    parser.add_argument(
        "-E",
        "--endtime",
        "--end-time",
        metavar="TIME",
        dest="end_time",
        default="now",
        help="Show tasks ending at this time; see `man sacct` for format",
    )
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

    parser.add_argument(
        "-s",
        "--state",
        metavar="state_list",
        dest="state",
        help="Show only jobs with the specified state(s); see `man sacct`",
        default=None,
    )

    parser.add_argument(
        "--show-overhead",
        action="store_true",
        help="Show overhead in terms of unused memory and unused/blocked CPUs. This is "
        "intended as a tool to help the cluster admins",
        default=False,
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if len(args.column_separator) != 1 and args.column_separator not in (
        "auto",
        "tab",
        "tabs",
        "space",
        "spaces",
    ):
        abort(
            "Invalid --column-separator option; must be 'auto', 'tabs', 'spaces', or a "
            f"single character, not {args.column_separator!r}!"
        )
    elif args.column_separator == "auto":
        args.column_separator = "spaces" if sys.stdout.isatty() else "tabs"

    if args.mode == "per-user" and not args.user:
        args.all_users = True

    stdout = run_sacct(
        start_time=args.start_time,
        end_time=args.end_time,
        all_users=args.all_users,
        user=args.user,
        truncate=args.truncate,
        group=args.group,
        jobs=args.jobs,
        state=args.state,
    )

    if stdout is None:
        return 1

    jobs = parse_sacct(stdout)

    # Add sstat statistics, if possible
    if not update_running_jobs(jobs):
        return 1

    if args.mode == "per-user":
        jobs = aggregate_statistics(jobs)

    column_getters: dict[str, Callable[[Usage], float] | Callable[[Usage], str]] = {
        "Start": lambda it: "Unknown" if it.start is None else str(it.start),
        "User": lambda it: it.user,
        "Jobs": lambda it: max(1, len(it.jobs)),
        "Job": lambda it: it.job,
        "State": lambda it: it.state,
        "Elapsed": lambda it: it.elapsed,
        "CPUsReserved": lambda it: it.cpus,
        "CPUsUsed": lambda it: it.cpus_used,
        "CPUsWasted": lambda it: it.cpus_wasted,
        "MemReserved": lambda it: it.mem,
        "MemUsed": lambda it: it.mem_used,
        "MemWasted": lambda it: it.mem_wasted,
        "Overhead": lambda it: it.overhead,
        "Name": lambda it: it.name,
    }

    column_formatters: dict[str, str | Callable[[Any], str]] = {
        "Start": "{}",
        "User": "{}",
        "Jobs": "{}",
        "Job": "{}",
        "State": "{}",
        "Elapsed": format_h,
        "CPUsReserved": "{}",
        "CPUsUsed": "{:.1f}",
        "CPUsWasted": "{:.1f}",
        "MemReserved": "{:.1f}",
        "MemUsed": "{:.1f}",
        "MemWasted": "{:.1f}",
        "Overhead": "{:.1f}",
        "Name": "{}",
    }

    columns = select_columns(
        mode=args.mode,
        metric=args.metric,
        show_overhead=args.show_overhead,
    )

    sort_table(
        table=jobs,
        sort_key=args.sort_key,
        getters=column_getters,
    )

    table: list[list[str]] = [list(columns)]
    is_numerical = [True] * len(columns)
    for it in jobs:
        values: list[str] = []
        for idx, key in enumerate(columns):
            value = column_getters[key](it)
            formatter = column_formatters[key]

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
