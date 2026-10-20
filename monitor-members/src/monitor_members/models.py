from __future__ import annotations

import enum
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import ClassVar

import sqlalchemy
from sqlalchemy import (
    ColumnExpressionArgument,
    DateTime,
    Dialect,
    ForeignKey,
    String,
    Text,
    TypeDecorator,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

__all__ = [
    "Base",
    "Group",
    "User",
    "timestamp",
]


# https://docs.sqlalchemy.org/en/20/core/custom_types.html#store-timezone-aware-timestamps-as-timezone-naive-utc
class TZDateTime(TypeDecorator[datetime]):
    impl = DateTime
    cache_ok = True

    def process_bind_param(
        self,
        value: datetime | None,
        dialect: Dialect,
    ) -> datetime | None:
        if value is not None:
            if not value.tzinfo or value.tzinfo.utcoffset(value) is None:
                raise TypeError("tzinfo is required")
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value

    def process_result_value(
        self,
        value: datetime | None,
        dialect: Dialect,
    ) -> datetime | None:
        return None if value is None else value.replace(tzinfo=UTC)


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[Mapping[type[datetime], TZDateTime]] = {
        # Record timestamps with timezone
        datetime: TZDateTime(),
    }


class _TimestampedModel:
    # Timestamp of last check
    last_checked: Mapped[datetime] = mapped_column()

    # FIXME: Should use timestamp from when LDAP was queried
    def update_last_checked(self) -> None:
        self.last_checked = timestamp()


class Group(_TimestampedModel, Base):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The group being tracked
    name: Mapped[str] = mapped_column(String(255))

    @staticmethod
    def new(*, name: str) -> Group:
        return Group(name=name, last_checked=timestamp())


class User(_TimestampedModel, Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The KU user being tracked
    username: Mapped[str] = mapped_column(String(255))

    @staticmethod
    def new(*, username: str) -> User:
        return User(username=username, last_checked=timestamp())


class _SnapshotModel:
    # The earliest date/time at which this information was recorded
    added: Mapped[datetime] = mapped_column()

    # The earliest date/time at which this information was no longer recorded
    removed: Mapped[datetime | None] = mapped_column()

    def mark_as_removed(self) -> None:
        if self.removed is not None:
            raise RuntimeError("attempted to flag already removed user")

        self.removed = timestamp()


class Profile(_SnapshotModel, Base):
    __tablename__ = "profiles"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The associated user
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped[User] = relationship(viewonly=True)

    display_name: Mapped[str] = mapped_column(Text)

    department: Mapped[str] = mapped_column(Text)

    division: Mapped[str] = mapped_column(Text)

    title: Mapped[str] = mapped_column(Text)

    @staticmethod
    def new(
        *,
        user: User,
        display_name: str,
        department: str,
        division: str,
        title: str,
    ) -> Profile:
        return Profile(
            user_id=user.id,
            display_name=display_name,
            department=department,
            division=division,
            title=title,
            added=timestamp(),
            removed=None,
        )


class Membership(_SnapshotModel, Base):
    __tablename__ = "memberships"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The associated user
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped[User] = relationship(viewonly=True)

    # The associated group
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    group: Mapped[Group] = relationship(viewonly=True)

    # Indicates if the user was added while initializing a group
    initial: Mapped[bool] = mapped_column()

    @staticmethod
    def new(*, user: User, group: Group, initial: bool) -> Membership:
        return Membership(
            user_id=user.id,
            group_id=group.id,
            added=timestamp(),
            removed=None,
            initial=initial,
        )


class ReportKind(enum.Enum):
    LDAP = enum.auto()
    SACCT = enum.auto()


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(primary_key=True)

    kind: Mapped[ReportKind] = mapped_column()

    # The time at which a report was attempted
    attempted: Mapped[datetime] = mapped_column()

    # Was the report a success
    success: Mapped[bool] = mapped_column()

    @staticmethod
    def new(*, kind: ReportKind, success: bool) -> Report:
        return Report(
            kind=kind,
            attempted=timestamp(),
            success=success,
        )


def timestamp() -> datetime:
    return datetime.now(tz=UTC)


def one_or_none[M: Base](
    session: sqlalchemy.orm.Session,
    model: type[M],
    *args: ColumnExpressionArgument[bool],
) -> M | None:
    query = sqlalchemy.select(model).where(*args)
    return session.scalars(query).one_or_none()
