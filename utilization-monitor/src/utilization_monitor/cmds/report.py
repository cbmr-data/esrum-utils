from __future__ import annotations

import colorsys
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Iterator, Literal

import typed_argparse as tap
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from utilization_monitor.cmds import common
from utilization_monitor.models import ProcUtilization
from utilization_monitor.template import Report

_LOGGER = logging.getLogger(__name__)

_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


def usernames_to_colors(names: Iterable[str]) -> dict[str, str]:
    names = sorted(set(names), reverse=True)
    names.append("<idle users>")
    names.append("<system>")
    n = len(names)
    colors = [colorsys.hsv_to_rgb(x / n, 0.7, 0.7) for x in range(n)]
    result: dict[str, str] = {}
    for name, (r, g, b) in zip(reversed(names), colors, strict=True):
        result[name] = f"#{int(255 * r):02x}{int(255 * g):02x}{int(255 * b):02x}"

    return result


@dataclass
class SystemUtilizationColumns:
    user: list[str] = field(default_factory=list)
    time_start: list[datetime] = field(default_factory=list)
    average_cpu: list[float] = field(default_factory=list)

    def flatten_idle_users(
        self,
        min_utilization: float = 0.1,
    ) -> SystemUtilizationColumns:
        active_users = {
            user
            for user, average_cpu in zip(self.user, self.average_cpu, strict=True)
            if average_cpu >= min_utilization
        }

        idle_processes: dict[datetime, float] = defaultdict(float)
        result = SystemUtilizationColumns()
        for u, ts, ac in self:
            if u in active_users or u.startswith("<"):
                result.user.append(u)
                result.time_start.append(ts)
                result.average_cpu.append(ac)
            else:
                idle_processes[ts] += ac

        for ts, ac in sorted(idle_processes.items()):
            result.user.append("<idle users>")
            result.time_start.append(ts)
            result.average_cpu.append(ac)

        return result

    def __iter__(self) -> Iterator[tuple[str, datetime, float]]:
        return zip(
            self.user,
            self.time_start,
            self.time_end,
            self.average_cpu,
            strict=True,
        )


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


def main(args: Args) -> None:
    common.setup_logging(log_level=args.log_level, log_sql=args.log_sql)

    try:
        import altair as alt
        import pandas as pd
    except ImportError as error:
        _error("Optional dependency missing: %s", error)
        _error("Install with `pip install /path/to/utilization_monitor[report]")
        sys.exit(1)

    alt.data_transformers.enable("vegafusion")

    engine = create_engine(f"sqlite+pysqlite:///{args.database}")
    with Session(engine) as session:
        report = Report("Server utilization")

        users = {
            user
            for (user,) in session.execute(
                select(ProcUtilization.user).where(ProcUtilization.user != None)
            )
        }
        users.add("<system>")
        users.add("<idle users>")
        colors = usernames_to_colors(users)

        days: dict[date, SystemUtilizationColumns] = defaultdict(
            SystemUtilizationColumns
        )

        for user, time_start, average_cpu in session.execute(
            select(
                ProcUtilization.user,
                ProcUtilization.time_start,
                ProcUtilization.average_cpu,
            )
            .where(
                ProcUtilization.group == None,  # noqa: E711
                ProcUtilization.time_start
                >= datetime.now().replace(hour=8, minute=0, second=0),
            )
            .order_by(ProcUtilization.time_start)
        ):
            it = days[time_start.date()]
            it.user.append("<system>" if user is None else user)
            it.time_start.append(time_start)
            it.average_cpu.append(average_cpu)

        for day, columns in days.items():
            days[day] = columns.flatten_idle_users()

        s = report.add()
        for day, columns in sorted(days.items()):
            df = pd.DataFrame(
                {
                    "user": columns.user,
                    "time_start": columns.time_start,
                    "average_cpu": columns.average_cpu,
                }
            ).sort_values(by=["user", "time_start"])

            df["average_cpu"] = df["average_cpu"].round(3)

            # Maintain pre-defined order of users between charts
            subusers = set(columns.user)
            subcolors = {key: value for key, value in colors.items() if key in subusers}

            selection = alt.selection_point(fields=["user"], bind="legend")
            chart = (
                alt.Chart(
                    data=df,
                    width=1024,
                    title=day.strftime("%Y-%m-%d (%A)"),
                )
                .mark_area(
                    interpolate="step",
                )
                .encode(
                    x="time_start",
                    y="average_cpu",
                    color=alt.Color("user").scale(
                        domain=list(subcolors),
                        range=list(subcolors.values()),
                    ),
                    opacity=alt.condition(selection, alt.value(1), alt.value(0.1)),
                    tooltip=["user", "average_cpu"],
                )
                .add_params(
                    selection,
                )
            )

            s.add_chart(chart)

        print(report.render())
