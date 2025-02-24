from __future__ import annotations

import enum
import logging
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm

from monitor_members.groups import GroupType
from monitor_members.ldap import LDAP
from monitor_members.models import Base, Group, Report, User


class ChangeType(enum.Enum):
    ADD = enum.auto()
    DEL = enum.auto()


@dataclass
class GroupChange:
    user: str
    group: str
    change: ChangeType
    warning: bool


class Database:
    __slots__ = [
        "_database",
        "_engine",
        "_ldap",
        "_log",
        "_session",
    ]

    _database: Path
    _engine: sqlalchemy.Engine | None
    _ldap: LDAP
    _log: logging.Logger
    _session: sqlalchemy.orm.Session | None

    def __init__(self, database: Path, ldap: LDAP) -> None:
        self._database = database
        self._engine = None
        self._ldap = ldap
        self._log = logging.getLogger("database")
        self._session = None

    def __enter__(self) -> Self:
        if not (self._engine is None and self._session is None):
            raise RuntimeError("database already initialized")

        self._engine = Database.create_engine(self._database)
        Base.metadata.create_all(self._engine)
        self._session = sqlalchemy.orm.Session(self._engine)

        return self

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        if self._engine is None or self._session is None:
            raise RuntimeError("database not initialized")

        self._session.close()
        self._session = None
        self._engine = None

    def update_ldap_groups(
        self,
        groups: dict[str, GroupType],
    ) -> list[GroupChange] | None:
        self._log.info("updating LDAP group memberships for %i groups", len(groups))
        if self._session is None:
            raise RuntimeError("database not initialized")

        updates: list[GroupChange] = []
        for group_name, group_type in sorted(groups.items()):
            self._log.debug("checking group %r for updates", group_name)
            group_stmt = sqlalchemy.select(Group).where(Group.name == group_name)
            group = self._session.scalars(group_stmt).one_or_none()

            if initializing := group is None:
                self._log.info("initializing group %r", group_name)
                group = Group.new(name=group_name)
                self._session.add(group)
            else:
                group.update_last_checked()

            self._session.commit()

            current_users_stmt = sqlalchemy.select(User).where(
                User.group == group,
                User.removed == None,  # noqa: E711
            )
            current_users = {
                it.name: it for it in self._session.scalars(current_users_stmt).all()
            }

            ldap_users = self._ldap.members(group_name)
            if ldap_users is None:
                self._log.error("failed to get LDAP members for %r", group_name)
                return None

            for username in ldap_users - set(current_users):
                self._log.info("adding user to group %r: %r", group_name, username)
                self._session.add(
                    User.new(
                        name=username,
                        group=group,
                        initial=initializing,
                    )
                )

                if not initializing:
                    updates.append(
                        GroupChange(
                            user=username,
                            group=group_name,
                            change=ChangeType.ADD,
                            warning=group_type == GroupType.SENSITIVE,
                        )
                    )

            for username in set(current_users) - ldap_users:
                self._log.info("removing user from group %r: %r", group_name, username)
                current_users[username].mark_as_removed()

                if not initializing:
                    updates.append(
                        GroupChange(
                            user=username,
                            group=group_name,
                            change=ChangeType.DEL,
                            warning=group_type != GroupType.REGULAR,
                        )
                    )
        self._session.commit()

        return updates

    def add_report(self, *, success: bool) -> None:
        if self._session is None:
            raise RuntimeError("database not initialized")

        self._session.add(Report.new(success=success))

    @staticmethod
    def create_engine(path: Path) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(f"sqlite+pysqlite:///{path}")
