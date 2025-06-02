from __future__ import annotations

import logging
from collections.abc import Collection, Iterable
from pathlib import Path
from typing import Literal

import typed_argparse as tap

from monitor_members.common import main_func, run_subprocess, setup_logging, which
from monitor_members.config import Config
from monitor_members.kerberos import Kerberos
from monitor_members.ldap import LDAP
from monitor_members.sacctmgr import Sacctmgr


def validate_commands(
    log: logging.Logger,
    commands: Iterable[list[str]],
    keys: Collection[str],
) -> bool:
    for command in commands:
        if command and not any((key in value) for key in keys for value in command):
            log.error("Command %s does not contain any of %s", command, tuple(keys))
            return False

    return True


def update_command(command: list[str], fields: dict[str, str]) -> list[str]:
    res: list[str] = []
    for arg in command:
        for key, value in fields.items():
            arg = arg.replace(key, value)

        res.append(arg)

    return res


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

    ldapsearch_exe: str = tap.arg(
        default=which("ldapsearch"),
        help="Optional path to ldapsearch executable",
    )

    sacctmgr_exe: str = tap.arg(
        default=which("sacctmgr"),
        help="Optional path to sacctmgr executable",
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

    fields: dict[str, str] = {
        "{cluster}": sacct.cluster,
        "{account}": sacct.account,
        "{user}": "?",
    }

    if not validate_commands(log, (sacct.add_member, sacct.remove_member), fields):
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
        ldapsearch_exe=args.ldapsearch_exe,
    )

    ldap_members = ldap.members(sacct.ldap_group)
    if ldap_members is None:
        log.error("failed to get members of LDAP group %r", sacct.ldap_group)
        return 1

    manager = Sacctmgr(cluster=sacct.cluster, account=sacct.account)
    sacct_members = manager.get_associations()
    if sacct_members is None:
        log.error("failed to get sacct members")
        return 1

    updates: tuple[tuple[str, list[str], set[str]], ...] = (
        ("added", sacct.add_member, ldap_members - sacct_members),
        ("removed", sacct.remove_member, sacct_members - ldap_members),
    )

    for desc, cmd_template, users in updates:
        for user in sorted(users):
            log.info("%s user %r", desc, user)

            if cmd_template:
                command = update_command(cmd_template, {**fields, "{user}": user})
                if not (proc := run_subprocess(log, command)):
                    log.error("Failed to run command:")
                    proc.log_stderr(log)
                    return 1

    return 0
