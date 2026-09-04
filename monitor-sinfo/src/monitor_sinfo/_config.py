# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import tomli
from koda_validate import DataclassValidator, Valid

from monitor_sinfo._utilities import abort


class ConfigError(RuntimeError):
    pass


@dataclass
class SlackConfig:
    webhook_url: str | None = None
    webhook_file: str | None = None
    webhook_env: str | None = None

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


@dataclass
class Config:
    smtp_server: str
    email_recipients: list[str]
    slack: SlackConfig = field(default_factory=SlackConfig)

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: object = tomli.load(handle)

        validator = DataclassValidator(Config)
        result = validator(toml)
        if not isinstance(result, Valid):
            abort("Error parsing TOML file: %s", result.err_type)

        return result.val
