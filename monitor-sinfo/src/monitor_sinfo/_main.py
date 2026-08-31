# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import logging
import sys
import time

from monitor_sinfo._cli import parse_args, setup_logging
from monitor_sinfo._notifications import setup_notifications
from monitor_sinfo._sinfo import (
    collect_node_status,
    diff_node_states,
    load_node_status,
    save_node_status,
)
from monitor_sinfo._utilities import abort

_LOG = logging.getLogger("monitor-sinfo")

_debug = _LOG.debug
_error = _LOG.error
_info = _LOG.info
_warning = _LOG.warning
_log = _LOG.log


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    setup_logging(args)
    notifiers = setup_notifications(args)
    prev_states = load_node_status(args.state)

    while True:
        curr_states = collect_node_status(args.sinfo)
        if curr_states is None:
            abort("Could not collect node status; aborting")

        if prev_states is None:
            save_node_status(filepath=args.state, nodes=curr_states)
            prev_states = curr_states

        if prev_states != curr_states:
            save_node_status(filepath=args.state, nodes=curr_states)

        updates = diff_node_states(prev_states=prev_states, curr_states=curr_states)
        if updates:
            for notifier in notifiers:
                notifier.send_notification(
                    nodes=curr_states,
                    updates=updates,
                    dry_run=args.dry_run,
                )

        if args.loop is None:
            break

        prev_states = curr_states
        try:
            time.sleep(args.loop)
        except KeyboardInterrupt:
            _info("Interrupt detected; ending loop")
            break

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
