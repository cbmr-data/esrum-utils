from __future__ import annotations

import dataclasses
import logging
import os
from pathlib import Path
from typing import Annotated, TypeVar, cast

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


class ConfigError(RuntimeError):
    pass


@dataclasses.dataclass
class Slack:
    webhook_url: str | None = None
    webhook_env: str | None = None
    webhook_file: str | None = None

    def gather_webhooks(self) -> list[str]:
        webhooks: list[str] = []
        if self.webhook_url is not None:
            webhooks.append(self._validate_url("webhook_url", self.webhook_url))

        if self.webhook_env is not None:
            webhook = os.environ.get(self.webhook_env)
            webhooks.append(self._validate_url("webhook_env", webhook))

        if self.webhook_file is not None:
            try:
                webhook = Path(self.webhook_file).read_text().strip()
            except FileNotFoundError:
                raise ConfigError(f"Slack webhook file '{self.webhook_file}' not found")
            except PermissionError:
                raise ConfigError(f"Could not access webhooks in '{self.webhook_file}'")

            webhooks.append(self._validate_url("webhook_file", webhook))

        return webhooks

    @staticmethod
    def _validate_url(desc: str, url: str | None) -> str:
        if url is None:
            raise ConfigError("Slack webhook [slack.webhook_env] is not not set")

        # Slack url and local (test) URLs are allowed
        elif not url.startswith(("https://hooks.slack.com/", "http://localhost:")):
            raise ConfigError(
                f"Slack webhook from [slack.{desc}] is not valid; URLs must match "
                f"either  'https://hooks.slack.com/*' or 'http://localhost:*': {url!r}"
            )

        return url


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
    sensitive_users: dict[str, str] = dataclasses.field(default_factory=dict[str, str])


@dataclasses.dataclass
class Sacct:
    ldap_group: str
    cluster: str
    account: str

    # Command to execute when a member has been added from the LDAP group
    add_member: list[str] = dataclasses.field(default_factory=list[str])
    # Command to execute when a member has been removed from the LDAP group
    remove_member: list[str] = dataclasses.field(default_factory=list[str])


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
        validator = DataclassValidator(
            annotations,
            fail_on_unknown_keys=True,
            typehint_resolver=custom_resolver,
        )

        return cast("Validator[ModelType]", validator)

    validator = get_typehint_validator(annotations)
    if isinstance(validator, MapValidator) and validator.coerce is None:
        validator.coerce = coerce_none_to_dict

    return validator


@coercer(type(None), dict[object, object])
def coerce_none_to_dict(val: object) -> Maybe[dict[object, object]]:
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
