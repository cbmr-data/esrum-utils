from __future__ import annotations

import enum
import itertools
import logging
import os
import types
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Self

import sqlalchemy
import sqlalchemy.orm

from monitor_members.groups import GroupType
from monitor_members.ldap import LDAP, LDAPGroup, LDAPUser
from monitor_members.models import (
    Base,
    Group,
    Membership,
    Profile,
    Report,
    ReportKind,
    User,
    one_or_none,
)


class ChangeType(enum.Enum):
    ADD = enum.auto()
    DEL = enum.auto()


@dataclass
class GroupChange:
    group: str
    group_type: GroupType
    changes: tuple[ChangeType, ...]

    @property
    def warning(self) -> bool:
        return self.group_type != GroupType.REGULAR

    @property
    def warning_sensitive(self) -> bool:
        return self.group_type == GroupType.SENSITIVE

    @property
    def warning_mandatory(self) -> bool:
        return self.group_type == GroupType.MANDATORY and ChangeType.DEL in self.changes


@dataclass
class ProfileChange:
    @dataclass
    class ValueChange:
        old: str
        new: str

        def __repr__(self) -> str:
            if self.old == self.new:
                return f"{self.old!r}"
            return f"ValueChange(old={self.old!r}, new={self.new!r})"

    # Each profile field, with the current and the new value
    changes: dict[str, ValueChange] = field(default_factory=dict)


@dataclass
class OrphanMembership:
    groups: dict[str, GroupType]


@dataclass
class UserChange:
    username: str
    display_name: str

    group_changes: list[GroupChange] = field(default_factory=list)
    profile_changes: list[ProfileChange] = field(default_factory=list)
    orphan_memberships: dict[str, GroupType] = field(default_factory=dict)


class Database:
    __slots__ = [
        "_database_url",
        "_engine",
        "_groups",
        "_ldap",
        "_log",
        "_session",
        "_user_queries",
    ]

    _database_url: str | None
    _engine: sqlalchemy.Engine | None
    _groups: dict[str, GroupType]
    _user_queries: tuple[str, ...]
    _ldap: LDAP
    _log: logging.Logger
    _session: sqlalchemy.orm.Session | None

    def __init__(
        self,
        *,
        ldap: LDAP,
        groups: dict[str, GroupType],
        database_url: str | None = None,
        user_queries: list[str] | tuple[str, ...] = (),
    ) -> None:
        self._database_url = database_url
        self._engine = None
        self._groups = dict(groups)
        self._ldap = ldap
        self._log = logging.getLogger("database")
        self._session = None
        self._user_queries = tuple(user_queries)

    def __enter__(self) -> Self:
        if not (self._engine is None and self._session is None):
            raise RuntimeError("database already initialized")

        self._engine = Database.create_engine(self._database_url)
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

    def unreported_updates(self, primary_group: str | None) -> list[UserChange]:
        """Returns updates since the last (successful) report"""
        if self._session is None:
            raise RuntimeError("database not initialized")

        # All changes are selected, since changes to `groups` are expected to be rare
        if (since := self.last_succesful_report(ReportKind.LDAP)) is None:
            self._log.info("collecting all unreported changes")
        else:
            self._log.info("collecting unreported changes since %s", since)

        group_changes = self._group_changes_since(self._session, since)
        profile_changes = self._profile_changes_since(self._session, since)
        orphan_memberships = self._get_orphaned_memberships(
            self._session,
            usernames=group_changes.keys() | profile_changes,
            primary_group=primary_group,
        )

        return [
            UserChange(
                username=username,
                display_name=self._get_display_name(self._session, username),
                group_changes=group_changes.get(username, []),
                profile_changes=profile_changes.get(username, []),
                orphan_memberships=orphan_memberships[username],
            )
            for username in group_changes | profile_changes
        ]

    def update(self, max_errors: int = 3) -> bool:
        self._log.info("updating memberships and users in %i groups", len(self._groups))

        if self._session is None:
            raise RuntimeError("database not initialized")
        elif (ldap_groups := self._ldap.get_groups(list(self._groups))) is None:
            self._log.error("failed to get LDAP group members")
            return False

        # Previously and currently active users
        usernames = {name.lower() for name in self._recorded_users(self._session)}
        for group in ldap_groups.values():
            usernames.update(name.lower() for name in group.members)

        additional_usernames: set[str] = set()
        for query in self._user_queries:
            self._log.info("getting additional usernames via user-query %r", query)
            result = self._session.execute(sqlalchemy.text(query))
            try:
                row = next(result)
                if len(row) != 1:
                    self._log.error("Query did not return single column: %r", query)
                    break
            except StopIteration:
                break

            additional_usernames.add(row[0])
            additional_usernames.update(username for (username,) in result)

        if additional_usernames:
            nnew = len(additional_usernames - usernames)
            self._log.info("adding %i additional usernames for monitoring", nnew)
            usernames.update(additional_usernames)

        if (ldap_users := self._ldap.get_users(list(usernames))) is None:
            self._log.error("failed to get LDAP users")
            return False

        result = self._update_users(
            session=self._session,
            ldap_users=ldap_users,
        ) and self._update_groups(
            session=self._session,
            ldap_groups=ldap_groups,
            max_errors=max_errors,
        )

        return result

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

    def _group_changes_since(
        self,
        session: sqlalchemy.orm.Session,
        since: datetime | None,
    ) -> dict[str, list[GroupChange]]:
        if since is None:
            query = sqlalchemy.select(Membership)
        else:
            query = sqlalchemy.select(Membership).where(
                (Membership.added > since) | (Membership.removed > since)
            )

        # group by user/group
        updates: dict[tuple[str, str], list[tuple[datetime, ChangeType]]] = defaultdict(
            list
        )
        for membership in session.scalars(query):
            if membership.group.name not in self._groups:
                # This may happen if the user removes groups from the config file
                self._log.warning(
                    "skipping update to group %r for %r; group not configured",
                    membership.group.name,
                    membership.user.username,
                )
                continue

            key = (membership.user.username, membership.group.name)

            if not membership.initial and (since is None or membership.added > since):
                updates[key].append((membership.added, ChangeType.ADD))

            if membership.removed is not None and (
                since is None or membership.removed > since
            ):
                updates[key].append((membership.removed, ChangeType.DEL))

        changes_by_user: dict[str, list[GroupChange]] = defaultdict(list)
        for (user, group), values in sorted(updates.items()):
            changes_by_user[user].append(
                GroupChange(
                    group=group,
                    group_type=self._groups[group],
                    changes=tuple(change for _, change in sorted(values)),
                )
            )

        return changes_by_user

    def _profile_changes_since(
        self,
        session: sqlalchemy.orm.Session,
        since: datetime | None,
    ) -> dict[str, list[ProfileChange]]:
        query = sqlalchemy.select(Profile)
        if since is not None:
            user_ids = (
                sqlalchemy.select(Profile.user_id)
                .where((Profile.added > since) | (Profile.removed > since))
                .distinct()
            )
            query = query.where(Profile.user_id.in_(user_ids))

        profiles: dict[User, list[Profile]] = defaultdict(list)
        for profile in session.scalars(query):
            profiles[profile.user].append(profile)

        profile_changes: dict[str, list[ProfileChange]] = defaultdict(list)
        for user, values in profiles.items():
            changes: list[ProfileChange] = []
            values.sort(key=lambda it: it.added)
            for prev, next in itertools.pairwise(values):
                if since is None or (
                    next.added > since or (prev.removed and prev.removed > since)
                ):
                    item = ProfileChange()
                    any_changes = False
                    for key, old, new in (
                        ("display_name", prev.display_name, next.display_name),
                        ("department", prev.department, next.department),
                        ("division", prev.division, next.division),
                        ("title", prev.title, next.title),
                    ):
                        item.changes[key] = ProfileChange.ValueChange(old=old, new=new)
                        any_changes |= old != new

                    if any_changes:
                        changes.append(item)

            if changes:
                profile_changes[user.username] = changes

        return profile_changes

    def _update_users(
        self,
        session: sqlalchemy.orm.Session,
        ldap_users: dict[str, LDAPUser],
    ) -> bool:
        self._log.debug("Updating user/user profiles from LDAP")
        for cn, ldap_user in ldap_users.items():
            user = one_or_none(session, User, User.username == cn)
            if user is None:
                self._log.debug("adding new user %r", cn)
                user = User.new(username=cn)
            else:
                user.update_last_checked()
            session.add(user)

            profile = one_or_none(
                session, Profile, (Profile.user == user) & (Profile.removed == None)
            )

            if (
                profile is None
                or profile.display_name != ldap_user.display_name
                or profile.department != ldap_user.department
                or profile.division != ldap_user.division
                or profile.title != ldap_user.title
            ):
                if profile is not None:
                    profile.mark_as_removed()
                    session.add(profile)
                    self._log.debug(
                        "Marking old profile for %r: display_name=%r, "
                        "department=%r, division=%r, title=%r",
                        user.username,
                        profile.display_name,
                        profile.department,
                        profile.division,
                        profile.title,
                    )

                profile = Profile.new(
                    user=user,
                    display_name=ldap_user.display_name,
                    department=ldap_user.department,
                    division=ldap_user.division,
                    title=ldap_user.title,
                )

                self._log.debug(
                    "Adding new profile for %r: display_name=%r, "
                    "department=%r, division=%r, title=%r",
                    user.username,
                    profile.display_name,
                    profile.department,
                    profile.division,
                    profile.title,
                )

                session.add(profile)

        session.commit()

        return True

    def _update_groups(
        self,
        session: sqlalchemy.orm.Session,
        *,
        ldap_groups: dict[str, LDAPGroup],
        max_errors: int,
    ) -> bool:
        loop_errors = 0
        for group_name in sorted(self._groups):
            self._log.debug("checking group %r for updates", group_name)
            if (ldap_group := ldap_groups.get(group_name)) is None:
                self._log.error("LDAP record not found for group %r", group_name)
                loop_errors += 1
                continue

            group = one_or_none(session, Group, Group.name == ldap_group.cn)
            if initializing := group is None:
                self._log.info("initializing group %r", ldap_group.cn)
                group = Group.new(name=ldap_group.cn)
            else:
                group.update_last_checked()
            session.add(group)

            current_users = set(ldap_group.members)
            recorded_users = set(self._recorded_users_in(session, group))
            if not (current_users or recorded_users):
                self._log.debug("no members in LDAP group %r", ldap_group.cn)
                continue

            any_errors = False
            for username in sorted(current_users - recorded_users):
                user = one_or_none(session, User, User.username == username)
                if user is None:
                    self._log.error("unknown user %r in group %r", username, group.name)
                    any_errors = True
                    continue

                self._log.info("adding user to group %r: %r", group.name, username)
                session.add(
                    Membership.new(
                        user=user,
                        group=group,
                        initial=initializing,
                    )
                )

            for username in sorted(recorded_users - current_users):
                self._log.info("removing user %r from %r", username, group_name)
                membership = one_or_none(
                    session,
                    Membership,
                    (Membership.removed == None)
                    & (Membership.group == group)
                    & (Membership.user_id == User.id)
                    & (User.username == username),
                )
                if membership is None:
                    self._log.error("user %r not a member of %r", username, group.name)
                    any_errors = True
                    continue

                membership.mark_as_removed()

            if any_errors:
                loop_errors += 1

        session.commit()

        return loop_errors < max_errors and loop_errors < len(self._groups)

    def _get_orphaned_memberships(
        self,
        session: sqlalchemy.orm.Session,
        usernames: Iterable[str],
        primary_group: str | None,
    ) -> dict[str, dict[str, GroupType]]:
        results: dict[str, dict[str, GroupType]] = {
            username: {} for username in usernames
        }

        if primary_group is not None:
            self._log.info("Collecting orphaned memberships")
            pgroup = one_or_none(session, Group, Group.name == primary_group)
            if pgroup is None:
                raise RuntimeError(f"Could not find primary group {primary_group}")

            users = sqlalchemy.select(User.id).where(
                User.username.in_(results),
                sqlalchemy.exists(1).where(
                    (Membership.user_id == User.id),
                    Membership.removed == None,
                    Membership.group_id != pgroup.id,
                ),
                sqlalchemy.not_(
                    sqlalchemy.exists(1).where(
                        (Membership.user_id == User.id),
                        Membership.removed == None,
                        Membership.group_id == pgroup.id,
                    )
                ),
            )

            groups = sqlalchemy.select(User.username, Group.name).where(
                User.id.in_(users),
                User.id == Membership.user_id,
                Membership.removed == None,
                Membership.group_id != pgroup.id,
                Membership.group_id == Group.id,
            )

            for username, group in session.execute(groups):
                results[username][group] = self._groups[group]

        return results

    def _get_display_name(self, session: sqlalchemy.orm.Session, username: str) -> str:
        profile = one_or_none(
            session,
            Profile,
            (User.username == username)
            & (Profile.user_id == User.id)
            & (Profile.removed == None),
        )

        return username if profile is None else profile.display_name

    def _recorded_users(self, session: sqlalchemy.orm.Session) -> Iterable[str]:
        """Returns recorded users with active memberships"""
        stmt = (
            sqlalchemy.select(User.username)
            .where(
                (Membership.removed == None) & (Membership.user_id == User.id),
            )
            .distinct()
        )

        return session.scalars(stmt).all()

    def _recorded_users_in(
        self,
        session: sqlalchemy.orm.Session,
        group: Group,
    ) -> Iterable[str]:
        """Returns recorded users with active memberships"""
        stmt = (
            sqlalchemy.select(User.username)
            .where(
                (Membership.group == group)
                & (Membership.removed == None)
                & (Membership.user_id == User.id),
            )
            .distinct()
        )

        return session.scalars(stmt).all()

    @staticmethod
    def create_engine(url: sqlalchemy.engine.URL | str | None) -> sqlalchemy.Engine:
        if url is None and not (url := os.getenv("DB_URL")):
            # https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
            url = sqlalchemy.engine.URL.create(
                drivername="mssql+pyodbc",
                username=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                host=f"tcp:{os.environ['DB_HOST']}",
                port=int(os.environ["DB_PORT"]),
                database=os.environ["DB_NAME"],
                query={
                    "driver": os.environ["DB_DRIVER"],
                    "encrypt": "yes",
                    "trustservercertificate": "yes",
                },
            )

        engine = sqlalchemy.create_engine(url)
        if schema := os.getenv("DB_SCHEMA"):
            engine = engine.execution_options(schema_translate_map={None: schema})

        return engine
