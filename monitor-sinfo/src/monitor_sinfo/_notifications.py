# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import datetime
from typing import Literal, TypeAlias

import requests
from typing_extensions import override

from monitor_sinfo._cli import Args
from monitor_sinfo._config import Config
from monitor_sinfo._sinfo import ChangeType, Status, StatusChange
from monitor_sinfo._utilities import JSON


class Notifier:
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        dry_run: bool,
    ) -> bool:
        raise NotImplementedError

    def format_update(self, name: str, update: StatusChange) -> str:
        name = self._highlight(name)
        state = self._highlight(str(update.new))
        last_state = f", was {update.old}" if update.old else ""
        reason = f", reason is {update.new.reason!r}" if update.new.reason else ""

        if update.change == ChangeType.Added:
            return f"Added node {name} with state {state}{reason}"
        elif update.change == ChangeType.Removed:
            return f"Removed node {name}"
        elif update.change == ChangeType.Trivial:
            return f"Node {name} state changed to {state}{last_state}"
        else:
            return (
                f"Node {name} became {update.change} "
                f"with state {state}{last_state}{reason}"
            )

    def _highlight(self, value: str) -> str:
        return value


class LogNotifier(Notifier):
    @override
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        dry_run: bool,
    ) -> bool:
        logger = logging.getLogger(__name__)
        for key, update in sorted(updates.items()):
            message = self.format_update(key, update)

            if (
                update.new.is_bad_state
                or not update.new.is_available
                or update.change == ChangeType.Removed
            ):
                logger.warning("%s", message)
            else:
                logger.info("%s", message)

        return True


class EmailNotifier(Notifier):
    def __init__(
        self,
        *,
        smtpserver: str,
        recipients: list[str],
        verbose: bool,
    ) -> None:
        self._smtpserver = smtpserver
        self._recipients = list(recipients)
        self._verbose = verbose

    @override
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        dry_run: bool,
    ) -> bool:
        message: list[str] = []
        for key, update in sorted(updates.items()):
            if update.change != ChangeType.Trivial or self._verbose:
                message.append(f"{len(message) + 1}. {self.format_update(key, update)}")

        if message:
            if dry_run:
                logger = logging.getLogger(__name__)
                logger.info(
                    "Would email %i recipients: %r", len(self._recipients), message
                )
            else:
                return self._send_message("\n".join(message))

        return True

    def _send_message(self, message: str) -> bool:
        logger = logging.getLogger(__name__)
        logger.debug("Sending email to %i recipients", len(self._recipients))
        try:
            proc = subprocess.Popen(
                [
                    "/usr/bin/mail",
                    "-S",
                    f"smtp={self._smtpserver}",
                    "-s",
                    "Esrum: Changes to node status",
                    *self._recipients,
                ],
                stdin=subprocess.PIPE,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )

            proc.communicate(input=message.encode("utf-8"))
        except OSError as error:
            logger.error("Error sending email notification: %s", error)
            return False

        return not proc.returncode


SlackTypes: TypeAlias = Literal[
    "rich_text", "rich_text_section", "rich_text_list", "emoji", "text"
]


class SlackBlock:
    def __init__(
        self,
        typ: SlackTypes,
        text: str | None = None,
        *,
        bold: bool = False,
        italic: bool = False,
    ) -> None:
        self._type: SlackTypes = typ
        self._children: list[SlackBlock] = []
        self._text = text
        self._bold = bold
        self._italic = italic

        if text is None and not typ.startswith("rich_"):
            raise ValueError("text required for emoji/text fields")
        elif text is not None and typ.startswith("rich_"):
            raise ValueError("text not allowed for rich text fields")

    def add_text(
        self,
        text: str,
        *,
        bold: bool = False,
        italic: bool = False,
    ) -> SlackBlock:
        self.add_element("text", text, bold=bold, italic=italic)
        return self

    def add_element(
        self,
        typ: SlackTypes,
        text: str | None = None,
        *,
        bold: bool = False,
        italic: bool = False,
    ) -> SlackBlock:
        if not self._type.startswith("rich_"):
            raise ValueError(typ)

        self._children.append(SlackBlock(typ, text, bold=bold, italic=italic))
        return self._children[-1]

    def to_json(self) -> JSON:
        if self._type == "text":
            if self._text is None:
                raise AssertionError("impossible situation: text object without text")

            out: JSON = {"type": "text", "text": self._text}

            style: JSON = {}
            for key, enabled in (("bold", self._bold), ("italic", self._italic)):
                if enabled:
                    style[key] = True

            if style:
                out["style"] = style

            return out
        elif self._type == "emoji":
            if self._text is None:
                raise AssertionError("impossible situation: emoji object without text")

            return {"type": "emoji", "name": self._text}
        elif self._type == "rich_text_list":
            return {
                "type": "rich_text_list",
                "style": "bullet",
                "elements": [it.to_json() for it in self._children],
            }

        return {
            "type": self._type,
            "elements": [it.to_json() for it in self._children],
        }


class SlackNotifier(Notifier):
    def __init__(self, *, webhooks: list[str], timeout: float, verbose: bool) -> None:
        self._webhooks = list(webhooks)
        self._timeout = timeout
        self._verbose = verbose

    @override
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        dry_run: bool,
    ) -> bool:
        block = SlackBlock("rich_text")
        block.add_element("rich_text_section").add_text(
            "Node status update for {}:\n\n".format(
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

        any_updates = False
        for key, update in sorted(updates.items()):
            change = update.change

            if change != ChangeType.Trivial or self._verbose:
                any_updates = True
                item = block.add_element("rich_text_list").add_element(
                    "rich_text_section"
                )

                if change in (ChangeType.Available, ChangeType.Added):
                    item.add_element("emoji", text="green_heart")
                    item.add_text(" ")
                elif change in (ChangeType.Unavailable, ChangeType.Removed):
                    item.add_element("emoji", text="broken_heart")
                    item.add_text(" ")

                item.add_text(key, bold=True)
                if change in (ChangeType.Added, ChangeType.Removed):
                    item.add_text(" was ")
                    item.add_text(str(change), bold=True)
                elif change in (ChangeType.Available, ChangeType.Unavailable):
                    item.add_text(" is ")
                    item.add_text(str(change), bold=True)
                    item.add_text(": Went from ")
                else:
                    item.add_text(" went from ")

                if change not in (ChangeType.Added, ChangeType.Removed):
                    if update.old is None:
                        raise AssertionError(
                            "impossible situation: state changed, but has no last state"
                        )

                    item.add_text(str(update.old), italic=True)
                    item.add_text(" to ")
                    item.add_text(str(update.new), italic=True)

                if update.new.reason:
                    item.add_text(" with reason ")
                    item.add_text(update.new.reason, italic=True)

        unavailable_nodes = 0
        for status in nodes.values():
            if status.is_bad_state:
                unavailable_nodes += 1

        summary = block.add_element("rich_text_section")
        summary.add_text(
            "\nSummary: {} node{} are available".format(
                len(nodes) - unavailable_nodes,
                "" if len(nodes) - unavailable_nodes == 1 else "s",
            )
        )

        if unavailable_nodes:
            summary.add_text(
                " and {} node{} are unavailable".format(
                    unavailable_nodes,
                    "" if unavailable_nodes == 1 else "s",
                )
            )

        if any_updates:
            if dry_run:
                logger = logging.getLogger(__name__)
                logger.info(
                    "Would send data to webhooks: %s",
                    json.dumps({"blocks": [block.to_json()]}),
                )
            else:
                return self._send_message(block)

        return True

    def _highlight(self, value: str) -> str:
        return f"*{value}*"

    def _send_message(self, block: SlackBlock) -> bool:
        logger = logging.getLogger(__name__)
        data = {"blocks": [block.to_json()]}
        any_errors = False
        for url in self._webhooks:
            logger.debug("Sending blocks to slack at %r", url)
            try:
                result = requests.post(
                    url,
                    data=json.dumps(data),
                    headers={
                        "content-type": "application/json",
                    },
                    timeout=self._timeout,
                )
            except requests.exceptions.RequestException as error:
                logger.error("Request to slack webhook %r failed: %s", url, error)
                any_errors = True
                continue

            if result.status_code != 200:
                logger.error(
                    "Request to slack webhook %r failed with %s",
                    url,
                    result.status_code,
                )
                any_errors = True

        return not any_errors


def setup_notifications(args: Args) -> list[Notifier]:
    logger = logging.getLogger(__name__)
    logger.info("Loading TOML config from %r", str(args.config))
    config = Config.load(args.config)

    notifiers: list[Notifier] = [LogNotifier()]
    if config.email_recipients:
        logger.debug("adding email recipients %s", config.email_recipients)
        notifiers.append(
            EmailNotifier(
                smtpserver=config.smtp_server,
                recipients=config.email_recipients,
                verbose=args.verbose,
            )
        )

    if config.slack_webhooks:
        logger.debug("adding slack webhook %s", config.slack_webhooks)
        notifiers.append(
            SlackNotifier(
                webhooks=config.slack_webhooks,
                timeout=args.slack_timeout,
                verbose=args.verbose,
            )
        )

    return notifiers
