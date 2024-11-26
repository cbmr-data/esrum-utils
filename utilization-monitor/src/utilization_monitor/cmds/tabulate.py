from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Literal

import typed_argparse as tap
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from utilization_monitor.cmds import common
from utilization_monitor.models import ProcUtilization, SystemUtilization

_LOGGER = logging.getLogger(__name__)

_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


def mean(values: Sequence[float]) -> float | str:
    if len(values) >= 1:
        return statistics.mean(values)

    return "NA"


class Args(tap.TypedArgs):
    database: Path = tap.arg(
        positional=True,
        metavar="DB",
        type=Path,
        help="Path to sqlite3 database containing utilization statistics",
    )

    ####################################################################################
    # Logging

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = tap.arg(
        default="INFO",
        help="Verbosity level for console logging",
    )
    log_sql: bool = tap.arg(
        help="Log database commands",
    )

    ####################################################################################
    # Logging

    min_cpu_hours: float = tap.arg(
        default=0.25,
        help="Count only users who have used at least this many CPU hours for a given",
    )


def main(args: Args) -> None:
    common.setup_logging(log_level=args.log_level, log_sql=args.log_sql)

    engine = create_engine(f"sqlite+pysqlite:///{args.database}")
    with Session(engine) as session:
        cpu_hours: dict[date, dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )

        for user, time_start, time_end, average_cpu in session.execute(
            select(
                ProcUtilization.user,
                ProcUtilization.time_start,
                ProcUtilization.time_end,
                ProcUtilization.average_cpu,
            ).where(ProcUtilization.group == None)
        ).unique():
            key = time_start.date()
            duration = (time_end - time_start).total_seconds() / 3600

            cpu_hours[key][user] += duration * average_cpu

        print(cpu_hours)

        average_cpu_load: dict[date, list[float]] = defaultdict(list)
        average_mem_load: dict[date, list[float]] = defaultdict(list)

        for time_start, average_cpu, average_mem in session.execute(
            select(
                SystemUtilization.time_start,
                SystemUtilization.average_cpu,
                SystemUtilization.average_mem,
            )
        ):
            key = time_start.date()
            average_cpu_load[key].append(average_cpu)
            average_mem_load[key].append(average_mem)

        for key in cpu_hours.keys() | average_cpu_load.keys():
            print(
                key,
                mean(average_cpu_load.get(key, ())),
                mean(average_mem_load.get(key, ())),
                sum(
                    1 for value in cpu_hours[key].values() if value > args.min_cpu_hours
                ),
                sep="\t",
            )
