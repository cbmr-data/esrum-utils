from __future__ import annotations

import sys

import typed_argparse as tap

from monitor_members.commands import monitor, refresh, sacct


def main_w() -> None:
    tap.Parser(
        tap.SubParserGroup(
            tap.SubParser("monitor", monitor.Args),
            tap.SubParser("refresh", refresh.Args),
            tap.SubParser("sacct", sacct.Args),
        ),
    ).bind(
        monitor.main,
        refresh.main,
        sacct.main,
    ).run(sys.argv[1:])


if __name__ == "__main__":
    main_w()
