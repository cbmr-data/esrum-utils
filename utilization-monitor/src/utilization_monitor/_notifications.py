from __future__ import annotations

import enum
import json
import logging
import subprocess
import sys
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from typing import Literal, TypeAlias, Union

import requests
from koda_validate import DataclassValidator
from typing_extensions import override

JSON: TypeAlias = dict[
    str, Union[float, str, bool, "JSON", list[Union[float, str, bool, "JSON"]]]
]


_LOGGER = logging.getLogger(__name__)
_debug = _LOGGER.debug
_error = _LOGGER.error
_info = _LOGGER.info
_warning = _LOGGER.warning


class ChangeType(enum.Enum):
    Added = "added"
    Removed = "removed"
    Trivial = "trivial"
    Available = "available"
    Unavailable = "unavailable"

    def __str__(self) -> str:
        return self.value


@dataclass
class Status:
    state: str
    reason: str | None


@dataclass
class StatusChange(Status):
    change: ChangeType
    last_state: str | None


@dataclass
class StatusDB:
    timestamp: datetime
    nodes: dict[str, Status]


StatusDBValidator = DataclassValidator(StatusDB)


class KodaJSONEncoder(json.JSONEncoder):
    """Json encoder that generates Koda compatible JSON"""

    @override
    def default(self, o: object) -> object:
        if isinstance(o, datetime):
            return o.isoformat()
        elif is_dataclass(o):
            return asdict(o)  # pyright: ignore[reportArgumentType]

        return super().default(o)


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
        state = self._highlight(update.state)
        last_state = f", was {update.last_state}" if update.last_state else ""
        reason = f", reason is {update.reason!r}" if update.reason else ""

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
        for key, update in sorted(updates.items()):
            message = self.format_update(key, update)

            if update.is_bad_state or update.change == ChangeType.Removed:
                _warning("%s", message)
            else:
                _info("%s", message)

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
                _info("Would email %i recipients: %r", len(self._recipients), message)
            else:
                return self._send_message("\n".join(message))

        return True

    def _send_message(self, message: str) -> bool:
        _debug("Sending email to %i recipients", len(self._recipients))
        try:
            proc = subprocess.Popen(
                [
                    "/usr/bin/mail",
                    "-S",
                    f"smtp={self._smtpserver}",
                    "-s",
                    "Changes to node status",
                    *self._recipients,
                ],
                stdin=subprocess.PIPE,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )

            proc.communicate(input=message.encode("utf-8"))
        except OSError as error:
            _error("Error sending email notification: %s", error)
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
            assert self._text is not None
            out: JSON = {"type": "text", "text": self._text}

            style: JSON = {}
            for key, enabled in (("bold", self._bold), ("italic", self._italic)):
                if enabled:
                    style[key] = True

            if style:
                out["style"] = style

            return out
        elif self._type == "emoji":
            assert self._text is not None
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
                    assert update.last_state is not None
                    item.add_text(update.last_state, italic=True)
                    item.add_text(" to ")
                    item.add_text(update.state, italic=True)

                if update.reason:
                    item.add_text(" with reason ")
                    item.add_text(update.reason, italic=True)

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
                _info(
                    "Would send data to webhooks: %s",
                    json.dumps({"blocks": [block.to_json()]}),
                )
            else:
                return self._send_message(block)

        return True

    def _highlight(self, value: str) -> str:
        return f"*{value}*"

    def _send_message(self, block: SlackBlock) -> bool:
        data = {"blocks": [block.to_json()]}
        any_errors = False
        for url in self._webhooks:
            _debug("Sending blocks to slack at %r", url)
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
                _error("Request to slack webhook %r failed: %s", url, error)
                any_errors = True
                continue

            if result.status_code != 200:
                _error(
                    "Request to slack webhook %r failed with %s",
                    url,
                    result.status_code,
                )
                any_errors = True

        return not any_errors
