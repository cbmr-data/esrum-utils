from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

import tomli
from koda_validate import (
    DataclassValidator,
    Valid,
)

from .utilities import ValidatePath, abort


@dataclass
class Config:
    database: Annotated[Path, ValidatePath()]
    email_recipients: list[str] = field(default_factory=list)
    slack_webhooks: list[str] = field(default_factory=list)
    process_groups: dict[str, list[str]] = field(default_factory=dict)
    smtp_server: str | None = None

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: object = tomli.load(handle)

        validator = DataclassValidator(Config)
        result = validator(toml)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val
