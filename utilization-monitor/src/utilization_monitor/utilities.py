from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Hashable, Iterable
from pathlib import Path
from typing import NoReturn, TypeVar

from koda_validate import (
    Invalid,
    TypeErr,
    Valid,
    ValidationResult,
    Validator,
)

T = TypeVar("T")
H = TypeVar("H", bound=Hashable)

_LOGGER = logging.getLogger(__name__)


class ValidatePath(Validator[Path]):
    def __call__(self, val: object) -> ValidationResult[Path]:
        if isinstance(val, str):
            return Valid(Path(val).expanduser())
        elif isinstance(val, Path):
            return Valid(val.expanduser())
        else:
            return Invalid(TypeErr(Path), val, self)


def abort(msg: str, *values: object) -> NoReturn:
    _LOGGER.error(msg, *values)
    sys.exit(1)


def aggregate(items: Iterable[T], key: Callable[[T], H]) -> dict[H, list[T]]:
    result: dict[H, list[T]] = {}
    for it in items:
        itk = key(it)
        try:
            result[itk].append(it)
        except KeyError:
            result[itk] = [it]

    return result
