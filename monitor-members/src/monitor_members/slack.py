from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import TypeAlias, Union

import requests

from monitor_members.database import ChangeType, GroupChange

JSON: TypeAlias = dict[
    str,
    Union[float, str, bool, "JSON", list[Union[float, str, bool, "JSON"]]],
]


class SlackNotifier:
    def __init__(self, *, webhooks: list[str], timeout: float, verbose: bool) -> None:
        self._log = logging.getLogger("slack")
        self._webhooks = list(webhooks)
        self._timeout = timeout
        self._verbose = verbose

    def send_ldap_notification(
        self,
        *,
        displaynames: dict[str, str | None],
        changes: Iterable[GroupChange],
    ) -> bool:
        elements: list[float | str | bool | JSON] = []

        # changes grouped by user
        user_updates: dict[str, list[GroupChange]] = defaultdict(list)
        for change in changes:
            user_updates[change.user].append(change)

        def _sort_key(it: tuple[str, list[GroupChange]]) -> tuple[int, str]:
            return (sum(-1 for change in it[1] if change.warning), it[0])

        for user, updates in sorted(user_updates.items(), key=_sort_key):
            elements.append(
                self._add_user(
                    username=user,
                    displayname=displaynames[user],
                    updates=updates,
                )
            )

        blocks: list[JSON] = [
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "Changes to LDAP groups at {}:\n\n".format(
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                ),
                            }
                        ],
                    },
                    {
                        "type": "rich_text_list",
                        "style": "bullet",
                        "indent": 0,
                        "elements": elements,
                    },
                ],
            },
        ]

        return self._send_message(blocks)

    @classmethod
    def _add_user(
        cls,
        username: str,
        displayname: str | None,
        updates: list[GroupChange],
    ) -> JSON:
        updates.sort(key=lambda it: (not it.warning, it.group))
        elements: list[float | str | bool | JSON] = []

        if any(it.warning for it in updates):
            elements.append({"type": "emoji", "name": "warning"})
            elements.append({"type": "text", "text": " "})

        if displayname is not None:
            elements.append({"type": "text", "text": f"{displayname}"})
            elements.append(
                {"type": "text", "text": f" ({username})", "style": {"italic": True}}
            )
        else:
            elements.append({"type": "text", "text": f"{username}"})

        if additions := [it for it in updates if it.change == ChangeType.ADD]:
            elements.append({"type": "text", "text": " added to"})
            elements.extend(cls.add_section(additions))

        if removals := [it for it in updates if it.change == ChangeType.DEL]:
            action = ", and removed from" if additions else " removed from"
            elements.append({"type": "text", "text": action})
            elements.extend(cls.add_section(removals))

        return {
            "type": "rich_text_section",
            "elements": elements,
        }

    @classmethod
    def add_section(cls, updates: list[GroupChange]) -> list[JSON]:
        elements: list[JSON] = []
        warnings = [it for it in updates if it.warning]
        updates = [it for it in updates if not it.warning]

        for idx, it in enumerate(warnings):
            if idx:
                elements.append({"type": "text", "text": ","})

            elements.append(
                {"type": "text", "text": f" {it.group}", "style": {"bold": True}}
            )

        if updates:
            labels: list[str] = []
            if warnings:
                labels.append("")
            else:
                elements.append({"type": "text", "text": " "})

            labels.extend(it.group for it in updates)
            elements.append({"type": "text", "text": ", ".join(labels)})

        return elements

    def _send_message(self, blocks: list[JSON]) -> bool:
        data = {"blocks": blocks}
        any_errors = False
        for url in self._webhooks:
            self._log.debug("sending blocks to slack at %r", url)
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
                self._log.error("request to slack webhook %r failed: %s", url, error)
                any_errors = True
                continue

            if result.status_code != 200:
                self._log.error(
                    "request to slack webhook %r failed with %s",
                    url,
                    result.status_code,
                )
                any_errors = True

        return not any_errors
