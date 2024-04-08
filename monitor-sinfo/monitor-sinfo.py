#!/usr/bin/env python3
from __future__ import annotations

import argparse
import enum
import functools
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, TypeAlias, Union, cast

import coloredlogs
import requests
import tomli

JSON: TypeAlias = Dict[
    str, Union[float, str, bool, "JSON", List[Union[float, str, bool, "JSON"]]]
]

_BAD_STATES = {"down", "drain", "drng", "fail", "failg", "pow_dn", "unk"}

_LOG = logging.getLogger("monitor-sinfo")

_debug = _LOG.debug
_error = _LOG.error
_info = _LOG.info
_warning = _LOG.warning
_log = _LOG.log


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

    @property
    def is_bad_state(self) -> bool:
        return self.state in _BAD_STATES


@dataclass
class StatusChange(Status):
    change: ChangeType
    last_state: str | None


class Notifier:
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        first_loop: bool,
        dry_run: bool,
    ) -> bool:
        raise NotImplementedError

    def format_update(self, name: str, update: StatusChange) -> str:
        name = self._highlight(name)
        state = self._highlight(update.state)
        last_state = f", was {update.last_state}" if update.last_state else ""
        reason = f", reason is {update.reason}" if update.reason else ""

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
    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        first_loop: bool,
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

    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        first_loop: bool,
        dry_run: bool,
    ) -> bool:
        if first_loop:
            return True

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
    def __init__(self, *, webhooks: list[str], verbose: bool) -> None:
        self._webhooks = list(webhooks)
        self._verbose = verbose

    def send_notification(
        self,
        *,
        nodes: dict[str, Status],
        updates: dict[str, StatusChange],
        first_loop: bool,
        dry_run: bool,
    ) -> bool:
        if first_loop:
            return True

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
                    item.add_text(" Node ")
                elif change in (ChangeType.Unavailable, ChangeType.Removed):
                    item.add_element("emoji", text="broken_heart")
                    item.add_text(" Node ")
                else:
                    item.add_text("Node ")

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
            result = requests.post(
                url,
                data=json.dumps(data),
                headers={
                    "content-type": "application/json",
                },
            )

            if result.status_code != 200:
                _error(
                    "Request to slack webhook %r failed with %s",
                    url,
                    result.status_code,
                )
                any_errors = True

        return not any_errors


@dataclass
class Config:
    smtp_server: str
    email_recipients: list[str]
    slack_webhooks: list[str]

    @staticmethod
    def load(filepath: Path) -> Config:
        with filepath.open("rb") as handle:
            toml: Any = tomli.load(handle)

        smtp_server = toml.get("smtp-server")
        if not isinstance(smtp_server, str):
            _error("`smtp-server` not set/not a string in TOML file")
            sys.exit(1)

        emails = Config._get(toml, "email-recipients")
        slack = Config._get(toml, "slack-webhooks")

        return Config(
            smtp_server=smtp_server,
            email_recipients=emails,
            slack_webhooks=slack,
        )

    @staticmethod
    def _get(toml: dict[str, Any], key: str) -> list[str]:
        values: Any = toml.get(key)
        if values is None:
            _warning("Key %r not specified in toml file", key)
            return []

        if isinstance(values, str):
            return [values]
        elif not isinstance(values, list):
            _error("%r in toml file is not a string or a list", key)
            sys.exit(1)

        for value in cast(list[object], values):
            if not isinstance(value, str):
                _error("Value %r for key %r in toml file is not a string", value, key)
                sys.exit(1)

        return cast(list[str], values)


def collect_node_status(sinfo: str) -> dict[str, Status] | None:
    _debug("Running sinfo")
    proc = subprocess.run(
        [sinfo, "--Node", "--format=%N|%t|%E"],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        text=True,
    )

    if proc.returncode:
        _error("sinfo exited with code %i: %s", proc.returncode, proc.stderr)
        return None

    lines = proc.stdout.splitlines()
    header = lines[0].split("|")

    result: dict[str, Status] = {}
    for line in lines[1:]:
        row = dict(zip(header, line.split("|")))
        reason: str | None = row["REASON"]
        if reason == "none":
            reason = None

        name = row["NODELIST"]
        state = row["STATE"].strip("*")
        result[name] = Status(state=state, reason=reason)

    return result


def main_loop(
    sinfo_exe: str, interval: float
) -> Iterator[tuple[dict[str, Status], dict[str, StatusChange]]]:
    nodes: dict[str, Status] = {}
    while True:
        sinfo = collect_node_status(sinfo_exe)
        if sinfo is None:
            _debug("Longer sleep for %i seconds", interval * 5)
            time.sleep(interval * 5)
            continue

        updates: dict[str, StatusChange] = {}
        for key, node in sorted(sinfo.items()):
            if key not in nodes:
                updates[key] = StatusChange(
                    state=node.state,
                    last_state=None,
                    reason=node.reason,
                    change=ChangeType.Added,
                )
            elif nodes[key].state != node.state:
                if node.is_bad_state:
                    change = ChangeType.Unavailable
                elif nodes[key].is_bad_state:
                    change = ChangeType.Available
                else:
                    change = ChangeType.Trivial

                updates[key] = StatusChange(
                    change=change,
                    state=node.state,
                    last_state=nodes[key].state,
                    reason=node.reason,
                )

        for key in set(nodes) - set(sinfo):
            updates[key] = StatusChange(
                change=ChangeType.Removed,
                state="unk",
                last_state=None,
                reason=None,
            )

        if updates:
            yield dict(sinfo), updates
        else:
            _debug("No updates")

        _debug("Sleeping %i seconds", interval)
        time.sleep(interval)
        nodes = sinfo


@dataclass
class Args:
    config: Path
    sinfo: str
    interval: float
    verbose: bool
    dry_run: bool
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        )
    )

    parser.add_argument("config", type=Path)
    parser.add_argument(
        "--sinfo",
        type=str,
        default="/usr/bin/sinfo",
        help="Path to/name of sinfo executable",
    )
    parser.add_argument(
        "--interval",
        default=1,
        type=float,
        help="Check node state every N minutes",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Send updates on all state changes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log updates that would be sent instead of sending them",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Verbosity level for console logging",
    )

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    args.interval *= 60

    coloredlogs.install(
        fmt="%(asctime)s %(levelname)s %(message)s",
        level=args.log_level,
    )

    _info("Loading TOML config from %r", str(args.config))
    config = Config.load(args.config)

    notifiers: list[Notifier] = [LogNotifier()]
    if config.email_recipients:
        _debug("adding email recipients %s", config.email_recipients)
        notifiers.append(
            EmailNotifier(
                smtpserver=config.smtp_server,
                recipients=config.email_recipients,
                verbose=args.verbose,
            )
        )

    if config.slack_webhooks:
        _debug("adding slack webhook %s", config.slack_webhooks)
        notifiers.append(
            SlackNotifier(
                webhooks=config.slack_webhooks,
                verbose=args.verbose,
            )
        )

    first_loop = True
    for nodes, updates in main_loop(sinfo_exe=args.sinfo, interval=args.interval):
        if first_loop:
            for update in updates.values():
                if update.is_bad_state:
                    update.change = ChangeType.Unavailable

        for notifier in notifiers:
            notifier.send_notification(
                nodes=nodes,
                updates=updates,
                first_loop=first_loop,
                dry_run=args.dry_run,
            )

        first_loop = False

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
