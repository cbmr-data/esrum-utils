# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import tomli
from koda_validate import DataclassValidator, Valid

from monitor_sinfo._utilities import abort


@dataclass
class Config:
    smtp_server: str
    email_recipients: list[str]
    slack_webhooks: list[str]

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: object = tomli.load(handle)

        validator = DataclassValidator(Config)
        result = validator(toml)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val
