from __future__ import annotations

import logging

import typed_argparse as tap

from utilization_monitor.cmds import monitor, report, tabulate

_LOGGER = logging.getLogger(__name__)

_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


def main_w() -> None:
    tap.Parser(
        tap.SubParserGroup(
            tap.SubParser("monitor", monitor.Args),
            tap.SubParser("report", report.Args),
            tap.SubParser("tabulate", tabulate.Args),
        )
    ).bind(
        monitor.main,
        report.main,
        tabulate.main,
    ).run()


if __name__ == "__main__":
    main_w()
