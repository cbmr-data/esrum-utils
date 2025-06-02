from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import Annotated, Any, TypeVar

import tomli
from koda import Just, Maybe, nothing
from koda_validate import (
    CoercionErr,
    DataclassValidator,
    Invalid,
    MapValidator,
    Valid,
    Validator,
    coercer,
)
from koda_validate.typehints import get_typehint_validator

ModelType = TypeVar("ModelType")

_LOG = logging.getLogger("config")


@dataclasses.dataclass
class Slack:
    urls: list[str] = dataclasses.field(default_factory=list[str])


@dataclasses.dataclass
class Kerberos:
    username: str | None = None
    keytab: str | None = None


@dataclasses.dataclass
class LDAP:
    uri: str
    searchbase: str
    sensitive_groups: list[str] = dataclasses.field(default_factory=list[str])
    mandatory_groups: list[str] = dataclasses.field(default_factory=list[str])
    groups: list[str] = dataclasses.field(default_factory=list[str])


@dataclasses.dataclass
class Sacct:
    ldap_group: str
    cluster: str
    account: str


@dataclasses.dataclass
class Config:
    database: Annotated[Path, PathValidator()]
    ldap: LDAP
    kerberos: Kerberos = dataclasses.field(default_factory=Kerberos)
    slack: Slack = dataclasses.field(default_factory=Slack)
    sacct: Sacct | None = dataclasses.field(default=None)

    @classmethod
    def load(cls, filepath: Path) -> Config | None:
        text = filepath.read_text()

        validator = custom_resolver(Config)
        db = validator(tomli.loads(text))
        if not isinstance(db, Valid):
            _LOG.error("Configuration file is invalid: %s", db.err_type)
            return None

        return db.val


def custom_resolver(annotations: type[ModelType]) -> Validator[ModelType]:
    if dataclasses.is_dataclass(annotations):
        return DataclassValidator(
            annotations,
            fail_on_unknown_keys=True,
            typehint_resolver=custom_resolver,
        )

    validator = get_typehint_validator(annotations)
    if isinstance(validator, MapValidator) and validator.coerce is None:
        validator.coerce = coerce_none_to_dict

    return validator  # pyright: ignore [reportUnknownVariableType,reportReturnType]


@coercer(type(None), dict[Any, Any])
def coerce_none_to_dict(val: object) -> Maybe[dict[Any, Any]]:
    if isinstance(val, dict):
        return Just(val)  # pyright: ignore[reportUnknownArgumentType]
    elif val is None:
        return Just({})
    return nothing


class PathValidator(Validator[Path]):
    def __call__(self, val: object) -> Valid[Path] | Invalid:
        if isinstance(val, Path) and val:
            return Valid(val)
        elif isinstance(val, str) and val:
            return Valid(Path(val))

        return Invalid(CoercionErr({str, Path}, Path), val, self)
