from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import typed_argparse as tap

from monitor_members.common import main_func, setup_logging, which
from monitor_members.config import Config
from monitor_members.database import Database
from monitor_members.groups import GroupType, collect_groups
from monitor_members.kerberos import Kerberos
from monitor_members.ldap import LDAP
from monitor_members.models import ReportKind, timestamp
from monitor_members.sacctmgr import Sacctmgr
from monitor_members.slack import SlackNotifier


class Args(tap.TypedArgs):
    config: Path = tap.arg(
        positional=True,
        metavar="TOML",
        help="Path to TOML configuration file",
    )

    interval: int = tap.arg(
        metavar="N",
        default=0,
        help="Repeat monitoring steps every N minutes, if value is greater than 0",
    )

    sacct_interval: int = tap.arg(
        metavar="N",
        default=24 * 60,
        help="Report missing sacctmgr users no more frequently than every N minutes. "
        "This interval should be divisible by --interval",
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
        name="monitor",
        log_level=args.log_level,
        log_sql=args.log_sql,
    )

    if not (conf := Config.load(args.config)):
        log.critical("aborting due to config error")
        return 1

    groups = collect_groups(
        regular_groups=conf.ldap.groups,
        mandatory_groups=conf.ldap.mandatory_groups,
        sensitive_groups=conf.ldap.sensitive_groups,
    )

    for typ in (GroupType.SENSITIVE, GroupType.MANDATORY, GroupType.REGULAR):
        vals = sorted(k for k, v in groups.items() if v == typ)
        log.info("Found %i %s groups: %s", len(vals), typ.name.title(), ", ".join(vals))

    ldap = LDAP(
        uri=conf.ldap.uri,
        searchbase=conf.ldap.searchbase,
        ldapsearch_exe=args.ldapsearch_exe,
    )

    if conf.sacct is None:
        log.warning("Sacct account monitoring disabled")
        sacct = None
    else:
        sacct = Sacctmgr(
            cluster=conf.sacct.cluster,
            account=conf.sacct.account,
        )

    notifier = SlackNotifier(
        webhooks=conf.slack.urls,
        timeout=60,
        verbose=True,
    )

    kerb = Kerberos(
        keytab=conf.kerberos.keytab,
        username=conf.kerberos.username,
        kinit_exe=args.kinit_exe,
    )

    with Database(database=conf.database, ldap=ldap, groups=groups) as database:
        # display names are assumed to be unchanging over the runtime of the script
        displaynames: dict[str, str | None] = {}

        # loop intervals in seconds
        loop_interval = args.interval * 60
        sacct_interval = timedelta(minutes=args.sacct_interval)

        while True:
            if kerb.refresh():
                if not database.update_ldap_groups():
                    log.error("could not update group memberships")
                    return 1

                if changes := database.unreported_updates():
                    for change in changes:
                        if change.user not in displaynames:
                            displaynames[change.user] = ldap.display_name(change.user)

                    report_sent = notifier.send_ldap_notification(
                        displaynames=displaynames,
                        changes=changes,
                    )

                    database.add_report(kind=ReportKind.LDAP, success=report_sent)

                if sacct is not None and conf.sacct is not None:
                    last_report = database.last_succesful_report(ReportKind.SACCT)

                    if (
                        last_report is None
                        or (last_report - timestamp()) >= sacct_interval
                    ):
                        log.info("Checking for membership in sacctmgr")

                        if (sacct_users := sacct.get_associations()) is not None:
                            ldap_users = database.get_users(conf.sacct.ldap_group)
                            if missing_users := ldap_users.difference(sacct_users):
                                report_sent = notifier.send_sacct_message(
                                    users=missing_users,
                                    cluster=conf.sacct.cluster,
                                    account=conf.sacct.account,
                                )

                                database.add_report(
                                    kind=ReportKind.SACCT,
                                    success=report_sent,
                                )
            else:
                log.error("unable to check group memberships; sleeping")

            if args.interval <= 0:
                break

            try:
                wake_at = datetime.now() + timedelta(seconds=loop_interval)
                log.info("Next loop at %s", wake_at)
                time.sleep(loop_interval)
            except KeyboardInterrupt:
                break

    return 0
