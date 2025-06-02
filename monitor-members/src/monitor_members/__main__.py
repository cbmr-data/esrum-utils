from __future__ import annotations

import sys

import typed_argparse as tap

from monitor_members.commands import ldap, refresh, sacct


def main_w() -> None:
    tap.Parser(
        tap.SubParserGroup(
            tap.SubParser("ldap", ldap.Args),
            tap.SubParser("refresh", refresh.Args),
            tap.SubParser("sacct", sacct.Args),
        ),
    ).bind(
        ldap.main,
        refresh.main,
        sacct.main,
    ).run(sys.argv[1:])


if __name__ == "__main__":
    main_w()
