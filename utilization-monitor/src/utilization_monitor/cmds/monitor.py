from __future__ import annotations

import contextlib
import logging
import socket
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import sqlalchemy
import typed_argparse as tap
from sqlalchemy.orm import Session

from utilization_monitor.cmds import common
from utilization_monitor.config import Config
from utilization_monitor.models import Base, ProcUtilization, SystemUtilization
from utilization_monitor.monitors import Monitor
from utilization_monitor.processes import ProcessSnapshots
from utilization_monitor.system import SystemSnapshots
from utilization_monitor.utilities import aggregate

_LOGGER = logging.getLogger(__name__)

_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


def timestamp_to_round_utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)


def create_user_trackers(groups: dict[str, list[str]]) -> list[ProcessSnapshots]:
    result: list[ProcessSnapshots] = []
    for key, patterns in groups.items():
        result.append(ProcessSnapshots(name=key, patterns=patterns))

    result.append(ProcessSnapshots(name=None, patterns="*"))

    return result


def commit_utilization_records(
    session: Session,
    system: SystemSnapshots,
    users: dict[str | None, list[ProcessSnapshots]],
) -> None:
    hostname = socket.gethostname()
    records: list[ProcUtilization | SystemUtilization] = []

    for user, groups in users.items():
        for group in groups:
            time_start = timestamp_to_round_utc(group.time_start)
            time_end = timestamp_to_round_utc(group.time_end)
            _debug(
                "Comitting record for %s/%s from %s to %s",
                user,
                group.name,
                time_start,
                time_end,
            )

            records.append(
                ProcUtilization.new(
                    hostname=hostname,
                    user=user,
                    group=group.name,
                    time_start=time_start,
                    time_end=time_end,
                    average_cpu=group.average_cpu_usage,
                    average_mem=group.average_mem_usage,
                    peak_cpu=group.peak_cpu_usage,
                    peak_mem=group.peak_mem_usage,
                    processes=group.processes,
                )
            )

    time_start = timestamp_to_round_utc(system.time_start)
    time_end = timestamp_to_round_utc(system.time_end)

    _debug("Comitting system records from %s to %s", time_start, time_end)
    records.append(
        SystemUtilization.new(
            hostname=hostname,
            time_start=time_start,
            time_end=time_end,
            average_cpu=system.average_cpu_usage,
            average_mem=system.average_mem_usage,
            peak_cpu=system.peak_cpu_usage,
            peak_mem=system.peak_mem_usage,
            users=system.users,
            user_processes=system.user_processes,
        )
    )

    start_time = time.time()
    session.add_all(records)
    session.commit()
    duration = start_time - time.time()
    if duration >= 0.5:
        _warning("Writing database records took %.1f seconds", duration)


########################################################################################


class Args(tap.TypedArgs):
    config: Path = tap.arg(
        positional=True,
        metavar="TOML",
        type=Path,
        help="Path to TOML file containing notification configuration",
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
    # Debugging

    load_replay: Path | None = tap.arg(
        default=None,
        help="Load previously stored processes replay and process it. This is mainly "
        "intended for development work",
    )
    save_replay: Path | None = tap.arg(
        default=None,
        help="Save process snapshots to replay, allowing run to be re-analyzed. This "
        "is mainly intended for development work",
    )

    ####################################################################################
    #

    utilization_resolution: int = tap.arg(
        metavar="MIN",
        default=5,
        help="Resolution at which system utilization is recorded in minutes",
    )

    ####################################################################################
    # Alerts

    verbose: bool = tap.arg(
        help="Send updates on all state changes",
    )
    dry_run: bool = tap.arg(
        help="Log updates that would be sent instead of sending them",
    )

    user_max_cpus: float = tap.arg(
        metavar="CPUs",
        default=1.0,
        help="Trigger a warning if a user utilizes more CPU/hour than this limit. For "
        "example, a limit of 3 CPUs would not trigger if the user used between 0 and 3 "
        "CPUs, but would trigger after 60 * 3 / (3 + N) minutes for N additional CPUs. "
        "For example, using a total of 10 CPUs in this scenario, would trigger a "
        "warning after 60 * 3 / (3 + 7) = 18 minutes",
    )

    user_max_memory: float = tap.arg(
        metavar="PCT",
        default=20.0,
        help="Trigger a warning if the current MEM utilization of a user exceeds this "
        "percentage limit",
    )

    system_max_memory: float = tap.arg(
        metavar="PCT",
        default=80.0,
        help="Trigger a warning if the current MEM utilization on the system exceeds "
        "this percentage limit",
    )

    notification_interval: float = tap.arg(
        metavar="MINUTES",
        default=60.0,
        help="Trigger a notification for the system or for any one user no more often "
        " than every N minutes",
    )

    min_user_id: int = tap.arg(
        metavar="UID",
        default=1000,
        help="Ignore users with a UID below this value (e.g. system processes)",
    )


def main(args: Args) -> None:
    common.setup_logging(log_level=args.log_level, log_sql=args.log_sql)

    if args.utilization_resolution < 1:
        _error("invalid --utilization-resolution: %r", args.utilization_resolution)
        sys.exit(1)

    refresh_interval = 5
    record_interval = args.utilization_resolution * 60

    def _calculate_next_snapshot(timestamp: float) -> float:
        return (timestamp // record_interval + 1) * record_interval

    conf = Config.load(args.config)
    engine = sqlalchemy.create_engine(f"sqlite+pysqlite:///{conf.database}")

    Base.metadata.create_all(engine)
    with (
        Session(engine) as session,
        Monitor(
            min_user_id=args.min_user_id,
            interval=refresh_interval,
            load_replay=args.load_replay,
            save_replay=args.save_replay,
        ) as monitor,
        contextlib.suppress(KeyboardInterrupt),
    ):
        # Windowed measurements of resource utilization
        system_utilization = SystemSnapshots()
        user_utilization: dict[str | None, list[ProcessSnapshots]] = defaultdict(
            lambda: create_user_trackers(conf.process_groups)
        )

        # Ensure that records start by the minute
        next_commit = _calculate_next_snapshot(time.time())

        for snapshot in monitor:
            _debug("Time until next commit: %.3f", next_commit - snapshot.timestamp)
            # Records are merged if latency or other issues cause interruptions
            if snapshot.timestamp - next_commit >= 10.0:
                _warning(
                    "Snapshots would drift by %.3s; extending current snapshot",
                    snapshot.timestamp - next_commit,
                )

                next_commit = _calculate_next_snapshot(snapshot.timestamp)

            system_utilization.add_measurement(snapshot.system)
            for user, user_processes in aggregate(
                snapshot.processes,
                lambda it: it.username,
            ).items():
                for group in user_utilization[user]:
                    group.add_measurements(user_processes)

            if snapshot.timestamp + 0.1 >= next_commit:
                commit_utilization_records(
                    session=session,
                    system=system_utilization,
                    users=user_utilization,
                )

                timestamp = min(snapshot.timestamp + 0.1, time.time())
                next_commit = _calculate_next_snapshot(timestamp)
                system_utilization.reset()
                user_utilization.clear()
