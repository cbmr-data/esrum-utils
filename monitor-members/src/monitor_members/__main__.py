from __future__ import annotations

import sys

import typed_argparse as tap

from monitor_members.commands import ldap, orphan, refresh, sacct


def main_w() -> None:
    tap.Parser(
        tap.SubParserGroup(
            tap.SubParser("ldap", ldap.Args),
            tap.SubParser("sacct", sacct.Args),
            tap.SubParser("orphan", orphan.Args),
            tap.SubParser("refresh", refresh.Args),
        ),
    ).bind(
        ldap.main,
        orphan.main,
        refresh.main,
        sacct.main,
    ).run(sys.argv[1:])


if __name__ == "__main__":
    main_w()
