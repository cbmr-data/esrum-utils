from __future__ import annotations

import enum
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import ClassVar

from sqlalchemy import DateTime, Dialect, ForeignKey, String, TypeDecorator
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
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    def process_result_value(
        self,
        value: datetime | None,
        dialect: Dialect,
    ) -> datetime | None:
        return None if value is None else value.replace(tzinfo=timezone.utc)


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[Mapping[type[datetime], TZDateTime]] = {
        # Record timestamps with timezone
        datetime: TZDateTime(),
    }


class Group(Base):
    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The group being tracked
    name: Mapped[str] = mapped_column(String(32))

    # Timestamp of last check
    last_checked: Mapped[datetime] = mapped_column()

    def update_last_checked(self) -> None:
        self.last_checked = timestamp()

    @staticmethod
    def new(*, name: str) -> Group:
        return Group(name=name, last_checked=timestamp())


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The user being tracked
    # Max length based on Linux limits (see `man useradd`)
    name: Mapped[str] = mapped_column(String(32))

    # The assosiated group
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id"))
    group: Mapped[Group] = relationship(viewonly=True)

    # The earliest date/time at which the user was recorded in the group
    added: Mapped[datetime] = mapped_column()

    # The earliest date/time at which the user was no longer recorded in the group
    removed: Mapped[datetime | None] = mapped_column()

    # Indicates if the user was added while initializing a group
    initial: Mapped[bool] = mapped_column()

    def mark_as_removed(self) -> None:
        if self.removed is not None:
            raise RuntimeError("attempted to flag already removed user")

        self.removed = timestamp()

    @staticmethod
    def new(*, name: str, group: Group, initial: bool) -> User:
        return User(
            name=name,
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
    return datetime.now(timezone.utc)
