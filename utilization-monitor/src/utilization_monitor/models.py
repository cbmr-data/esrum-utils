from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import ClassVar

from sqlalchemy import TIMESTAMP, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = [
    "Base",
    "ProcUtilization",
    "SystemUtilization",
]


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[Mapping[type[datetime], TIMESTAMP]] = {
        # Record timestamps with timezone
        datetime: TIMESTAMP(timezone=True),
    }


class SystemUtilization(Base):
    __tablename__ = "systemstats"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Maximum length based on POSIX standard
    hostname: Mapped[str] = mapped_column(String(255))

    # Time when tracking of this time slice was started
    time_start: Mapped[datetime] = mapped_column()
    # Time when this time slice was last updated; at most 24 hours after `time_start`
    time_end: Mapped[datetime] = mapped_column()

    # Average CPU usage (0..1)
    average_cpu: Mapped[float] = mapped_column()
    # Average memory usage (0..1)
    average_mem: Mapped[float] = mapped_column()

    # Average CPU usage (0..1)
    peak_cpu: Mapped[float] = mapped_column()
    # Average memory usage (0..1)
    peak_mem: Mapped[float] = mapped_column()

    # The number of unique users with processes
    users: Mapped[int] = mapped_column()
    # The number of unique user processes
    user_processes: Mapped[int] = mapped_column()

    @staticmethod
    def new(
        *,
        hostname: str,
        time_start: datetime,
        time_end: datetime,
        average_cpu: float,
        average_mem: float,
        peak_cpu: float,
        peak_mem: float,
        users: int,
        user_processes: int,
    ) -> SystemUtilization:
        return SystemUtilization(**locals())


class ProcUtilization(Base):
    __tablename__ = "userstats"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Maximum length based on POSIX standard
    hostname: Mapped[str] = mapped_column(String(255))

    # The user / group being tracked
    # Max length based on Linux limits (see `man useradd`)
    user: Mapped[str | None] = mapped_column(String(32))
    # Optional process group
    group: Mapped[str | None] = mapped_column()

    # Time when tracking of this time slice was started
    time_start: Mapped[datetime] = mapped_column()
    # Time when this time slice was last updated; at most 24 hours after `time_start`
    time_end: Mapped[datetime] = mapped_column()

    # Average CPU usage (0..1)
    average_cpu: Mapped[float] = mapped_column()
    # Average memory usage (0..1)
    average_mem: Mapped[float] = mapped_column()

    # Average CPU usage (0..1)
    peak_cpu: Mapped[float] = mapped_column()
    # Average memory usage (0..1)
    peak_mem: Mapped[float] = mapped_column()

    # The number of unique processes recorded
    processes: Mapped[int] = mapped_column()

    @staticmethod
    def new(
        *,
        hostname: str,
        user: str | None,
        group: str | None,
        time_start: datetime,
        time_end: datetime,
        average_cpu: float,
        average_mem: float,
        peak_cpu: float,
        peak_mem: float,
        processes: int,
    ) -> ProcUtilization:
        return ProcUtilization(**locals())
