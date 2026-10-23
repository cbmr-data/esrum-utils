from __future__ import annotations

import collections
from pathlib import Path
from typing import Literal

import sqlalchemy
import sqlalchemy.orm
import typed_argparse as tap

from monitor_members.common import main_func, setup_logging
from monitor_members.config import Config
from monitor_members.database import Database
from monitor_members.groups import GroupType, collect_groups
from monitor_members.models import Base, Group, Membership, Profile, User


class Args(tap.TypedArgs):
    config: Path = tap.arg(
        positional=True,
        metavar="TOML",
        help="Path to TOML configuration file",
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

    primary_group = conf.ldap.primary_group
    if primary_group not in groups:
        log.critical("Required group %r not found in '%s'", primary_group, args.config)
        return 1

    for typ in (GroupType.SENSITIVE, GroupType.MANDATORY, GroupType.REGULAR):
        vals = sorted(k for k, v in groups.items() if v == typ)
        log.info("Found %i %s groups: %s", len(vals), typ.name.title(), ", ".join(vals))

    engine = Database.create_engine(conf.database_url)
    Base.metadata.create_all(engine)
    session = sqlalchemy.orm.Session(engine)

    users: dict[int, set[str]] = collections.defaultdict(set)
    query = sqlalchemy.select(User.id, Group.name).where(
        (User.id == Membership.user_id)
        & (Membership.group_id == Group.id)
        & (Membership.removed == None)
    )
    for userid, group in session.execute(query):
        users[userid].add(group)

    query = sqlalchemy.select(Profile).where(
        Profile.user_id.in_(users),
        Profile.removed == None,
    )

    userinfo: dict[int, tuple[str, str, str, str]] = {}
    for user in session.scalars(query):
        userinfo[user.user_id] = (
            user.display_name,
            user.department,
            user.division,
            user.title,
        )

    usernames: dict[int, str] = {}
    query = sqlalchemy.select(User.id, User.username).where(User.id.in_(users))
    for user_id, username in session.execute(query):
        usernames[user_id] = username

    rows: list[list[str]] = []
    for user_id, groups in users.items():
        if primary_group not in groups:
            name, department, division, title = userinfo[user_id]

            rows.append(
                [
                    name,
                    usernames[user_id],
                    department,
                    division,
                    title,
                    ",".join(sorted(groups)),
                ]
            )

    print("Name", "Username", "Department", "Division", "Title", "Groups", sep="\t")
    for row in sorted(rows):
        print(*row, sep="\t")

    return 0
