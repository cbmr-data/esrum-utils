from __future__ import annotations

import enum
import logging
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import sqlalchemy
import sqlalchemy.orm

from monitor_members.groups import GroupType
from monitor_members.ldap import LDAP
from monitor_members.models import Base, Group, Report, ReportKind, User


class ChangeType(enum.Enum):
    ADD = enum.auto()
    DEL = enum.auto()


@dataclass
class GroupChange:
    user: str
    group: str
    group_type: GroupType
    changes: tuple[ChangeType, ...]

    @property
    def warning(self) -> bool:
        return self.warning_sensitive or self.warning_mandatory

    @property
    def warning_sensitive(self) -> bool:
        return self.group_type == GroupType.SENSITIVE

    @property
    def warning_mandatory(self) -> bool:
        return self.group_type == GroupType.MANDATORY and ChangeType.DEL in self.changes


class Database:
    __slots__ = [
        "_database",
        "_engine",
        "_groups",
        "_ldap",
        "_log",
        "_session",
    ]

    _database: Path
    _engine: sqlalchemy.Engine | None
    _groups: dict[str, GroupType]
    _ldap: LDAP
    _log: logging.Logger
    _session: sqlalchemy.orm.Session | None

    def __init__(
        self,
        *,
        database: Path,
        ldap: LDAP,
        groups: dict[str, GroupType],
    ) -> None:
        self._database = database
        self._engine = None
        self._groups = dict(groups)
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

    def unreported_updates(self) -> list[GroupChange]:
        """Returns updates since the last (successful) report"""
        if self._session is None:
            raise RuntimeError("database not initialized")

        # All changes are selected, since changes to `groups` are expected to be rare
        since = self.last_succesful_report(ReportKind.LDAP)
        if since is None:
            self._log.info("collecting all unreported changes")
            query = sqlalchemy.select(User)
        else:
            self._log.info("collecting unreported changes since %s", since)
            query = sqlalchemy.select(User).where(
                (User.added >= since)
                | ((User.removed != None) & (User.removed >= since))  # noqa: E711
            )

        # group by user/group
        updates: dict[tuple[str, str], list[tuple[datetime, ChangeType]]] = defaultdict(
            list
        )
        for user in self._session.scalars(query):
            if user.group.name not in self._groups:
                # This may happen if the user removes groups from the config file
                self._log.warning(
                    "skipping update to group %r for %r; group not configured",
                    user.group.name,
                    user.name,
                )
                continue

            key = (user.name, user.group.name)

            if not user.initial and (since is None or user.added > since):
                updates[key].append((user.added, ChangeType.ADD))

            if user.removed is not None and (since is None or user.removed > since):
                updates[key].append((user.removed, ChangeType.DEL))

        changes: list[GroupChange] = []
        for (user, group), values in sorted(updates.items()):
            changes.append(
                GroupChange(
                    user=user,
                    group=group,
                    group_type=self._groups[group],
                    changes=tuple(change for _, change in sorted(values)),
                )
            )

        return changes

    def update_ldap_groups(self, max_errors: int = 3) -> bool:
        self._log.info(
            "updating LDAP group memberships for %i groups", len(self._groups)
        )
        if self._session is None:
            raise RuntimeError("database not initialized")

        loop_errors = 0
        for group_name in sorted(self._groups):
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

                loop_errors += 1
                if loop_errors >= max_errors:
                    break

                continue
            elif not (ldap_users or current_users):
                self._log.debug("no members in LDAP group %r", group_name)

            for username in sorted(ldap_users - set(current_users)):
                self._log.info("adding user to group %r: %r", group_name, username)
                self._session.add(
                    User.new(
                        name=username,
                        group=group,
                        initial=initializing,
                    )
                )

            for username in sorted(set(current_users) - ldap_users):
                self._log.info("removing user from group %r: %r", group_name, username)
                current_users[username].mark_as_removed()

        self._session.commit()

        return loop_errors < max_errors and loop_errors < len(self._groups)

    def add_report(self, *, kind: ReportKind, success: bool) -> None:
        if self._session is None:
            raise RuntimeError("database not initialized")

        self._session.add(Report.new(kind=kind, success=success))
        self._session.commit()

    def last_succesful_report(self, kind: ReportKind) -> datetime | None:
        if self._session is None:
            raise RuntimeError("database not initialized")

        report = self._session.scalars(
            sqlalchemy.select(Report)
            .where(Report.success & (Report.kind == kind))
            .order_by(Report.attempted.desc())
        ).first()

        return None if report is None else report.attempted

    def get_users(self, name: str) -> set[str]:
        if self._session is None:
            raise RuntimeError("database not initialized")

        group = self._session.scalars(
            sqlalchemy.select(Group).where(Group.name == name)
        ).one()

        return set(
            self._session.scalars(
                sqlalchemy.select(User.name).where(
                    (User.removed == None) & (User.group == group)  # noqa: E711
                )
            ).all()
        )

    @staticmethod
    def create_engine(path: Path) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(f"sqlite+pysqlite:///{path}")
