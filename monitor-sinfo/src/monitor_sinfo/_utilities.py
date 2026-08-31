# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
import logging
import sys
from typing import NoReturn, TypeAlias, Union

JSONValue: TypeAlias = Union[float, str, bool, "JSON"]
JSON: TypeAlias = dict[str, Union[float, str, bool, "JSON", list[JSONValue]]]


def abort(msg: str, *values: object) -> NoReturn:
    logger = logging.getLogger(__name__)
    logger.error(msg, *values)
    sys.exit(1)
