from __future__ import annotations

from typing import Literal

import typed_argparse as tap

from monitor_members.common import main_func, setup_logging, which
from monitor_members.kerberos import Kerberos


class Args(tap.TypedArgs):
    username: str = tap.arg(
        positional=True,
        help="username including kerberos domain",
    )
    keytab: str | None = tap.arg(
        positional=True,
        help="Path to existing keytab file",
    )

    ####################################################################################
    # Executables

    kinit_exe: str = tap.arg(
        default=which("kinit"),
        help="Optional path to kinit executable",
    )

    ####################################################################################
    # Logging

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = tap.arg(
        default="INFO",
        help="Verbosity level for console logging",
    )


@main_func
def main(args: Args) -> int:
    log = setup_logging("refresh", log_level=args.log_level)
    kerb = Kerberos(
        keytab=args.keytab,
        username=args.username,
        kinit_exe=args.kinit_exe,
    )

    log.info("attempting to refresh kerberos ticket")
    if not kerb.refresh():
        log.error("failed to refresh kerberos ticket")
        return 1

    return 0
