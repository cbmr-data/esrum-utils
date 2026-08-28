# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import logging
import socket
import sys
import time
from typing import Never

from monitor_stats._cli import parse_args, setup_logging
from monitor_stats._config import Config
from monitor_stats._monitor import Metrics, Monitor
from monitor_stats._notifications import SlackNotifier
from monitor_stats._utils import get_username

_LOG = logging.getLogger("monitor-stats")

_debug = _LOG.debug
_error = _LOG.error
_info = _LOG.info
_warning = _LOG.warning
_log = _LOG.log


def main(argv: list[str] | None = None) -> Never:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    setup_logging(args)

    _info("Loading TOML config from %r", str(args.config))
    config = Config.load(args.config)

    notifier = SlackNotifier(
        webhooks=config.slack_webhooks,
        timeout=args.slack_timeout,
        host=socket.gethostname(),
    )

    monitor = Monitor(
        process_whitelist=config.process_whitelist,
        process_blacklist=config.process_blacklist,
        loadavg_measure=args.loadavg_measure,
        min_process_uid=args.min_process_uid,
        max_process_age=args.max_process_runtime,
    )
    steps: dict[Metrics, float] = {
        "LoadAvg": args.loadavg_step,
        "%CPU": args.cpu_step,
        "Memory": args.memory_step,
    }
    thresholds = dict.fromkeys(steps, 0.0)

    while True:
        time.sleep(args.loop)
        stats: dict[Metrics, float] = {}
        summary = monitor.get()
        for key, value in summary.system.items():
            step = steps[key]
            threshold = thresholds[key]

            if value > threshold + step:
                stats[key] = value
                thresholds[key] = value

                _info(
                    "Exceeded %s threshold: %.2f > %.2f; next warning at %.2f",
                    key,
                    value,
                    threshold,
                    thresholds[key] + step,
                )

            elif value + step < threshold:
                _info("Lowering %s threshold from %.2f to %.2f", key, threshold, value)
                thresholds[key] = value

        summary.system = stats
        if procs := summary.top_processes():
            for proc in sorted(procs, key=lambda it: -max(it.mem, it.cpu)):
                _info("  proc %i (%s): %s", proc.pid, get_username(proc.uid), proc.cmd)

        notifier.notify(summary)
