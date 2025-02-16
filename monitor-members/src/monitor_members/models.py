from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import ClassVar

from sqlalchemy import TIMESTAMP, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

__all__ = [
    "Base",
    "Group",
    "User",
    "timestamp",
]


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[Mapping[type[datetime], TIMESTAMP]] = {
        # Record timestamps with timezone
        datetime: TIMESTAMP(timezone=True),
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

    def mark_as_removed(self) -> None:
        if self.removed is not None:
            raise RuntimeError("attempted to flag already removed user")

        self.removed = timestamp()

    @staticmethod
    def new(*, name: str, group: Group) -> User:
        return User(
            name=name,
            group_id=group.id,
            added=timestamp(),
            removed=None,
        )


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(primary_key=True)

    # The time at which a report was attempted
    attempted: Mapped[datetime] = mapped_column()

    # Was the report a success
    success: Mapped[bool] = mapped_column()

    @staticmethod
    def new(*, success: bool) -> User:
        return User(
            attempted=timestamp(),
            success=success,
        )


def timestamp() -> datetime:
    return datetime.now(timezone.utc)
