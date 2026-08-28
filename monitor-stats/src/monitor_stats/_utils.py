# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import functools
import logging
import pwd
import sys
from typing import NoReturn


def abort(msg: str, *values: object) -> NoReturn:
    logger = logging.getLogger(__name__)
    logger.error(msg, *values)
    sys.exit(1)


@functools.cache
def get_username(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def format_time(seconds: float) -> str:
    fields: list[str] = []
    for cutoff in (3600, 60, 1):
        if seconds >= cutoff:
            value = int(seconds // cutoff)
            seconds %= cutoff
            fields.append(f"{value:02}" if fields else f"{value}")

    if not fields:
        fields.append(f"{seconds:.1f}")

    fields[-1] += "s"
    return ":".join(fields)
