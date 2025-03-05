from __future__ import annotations

from pathlib import Path
from typing import Literal

import typed_argparse as tap

from monitor_members.common import main_func, setup_logging, which
from monitor_members.config import Config
from monitor_members.kerberos import Kerberos
from monitor_members.ldap import LDAP
from monitor_members.sacctmgr import Sacctmgr


class Args(tap.TypedArgs):
    config: Path = tap.arg(
        positional=True,
        metavar="TOML",
        help="Path to TOML configuration file",
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
    log_sql: bool = tap.arg(
        help="Log database commands",
    )


@main_func
def main(args: Args) -> int:
    log = setup_logging(
        name="sacct",
        log_level=args.log_level,
        log_sql=args.log_sql,
    )

    if not (conf := Config.load(args.config)):
        log.critical("aborting due to config error")
        return 1

    if (sacct := conf.sacct) is None:
        log.critical("SACCT account, cluster, and LDAP group must be set in TOML file")
        return 1

    kerb = Kerberos(
        keytab=conf.kerberos.keytab,
        username=conf.kerberos.username,
        kinit_exe=args.kinit_exe,
    )

    if not kerb.refresh():
        log.error("no kerberos ticket available; unable to determine current users")
        return 1

    ldap = LDAP(
        uri=conf.ldap.uri,
        searchbase=conf.ldap.searchbase,
    )

    ldap_members = ldap.members(sacct.ldap_group)
    if ldap_members is None:
        log.error("failed to get members of LDAP group %r", sacct.ldap_group)
        return 1

    sacct = Sacctmgr(cluster=sacct.cluster, account=sacct.account)
    sacct_members = sacct.get_associations()
    if sacct_members is None:
        log.error("failed to get sacct members")
        return 1

    for user in sorted(ldap_members - sacct_members):
        print("-", user)

    for user in sorted(sacct_members - ldap_members):
        print("+", user)

    return 0
