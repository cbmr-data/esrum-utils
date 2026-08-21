from __future__ import annotations

import fnmatch
import json
import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from itertools import groupby
from typing import TypeAlias, Union

import requests

from monitor_members.common import pretty_list_t
from monitor_members.database import ChangeType, GroupChange

JSONList: TypeAlias = list[Union[float, str, bool, "JSON"]]
JSON: TypeAlias = dict[
    str,
    Union[float, str, bool, "JSON", JSONList],
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
        sensitive_users: dict[str, str],
    ) -> bool:
        if not self._webhooks:
            self._log.warning("Slack LDAP update not sent; no webhooks configured")
            return False

        elements: JSONList = []

        # changes grouped by user
        user_updates: dict[str, list[GroupChange]] = defaultdict(list)
        for change in changes:
            user_updates[change.user].append(change)

        def _sort_key(it: tuple[str, list[GroupChange]]) -> tuple[int, str]:
            return (sum(-1 for change in it[1] if change.warning), it[0])

        for user, updates in sorted(user_updates.items(), key=_sort_key):
            messages = [
                message
                for glob, message in sensitive_users.items()
                if fnmatch.fnmatch(name=user.lower(), pat=glob.lower())
            ]
            alert = "; ".join(messages) if messages else None
            if alert is not None:
                self._log.warning("sensitive user %r: %s", user, alert)

            elements.append(
                self._describe_user_changes(
                    username=user,
                    displayname=displaynames[user],
                    updates=updates,
                    alert=alert is not None,
                )
            )

            if alert is not None:
                elements.append(self._add_user_alert(alert))

        return self._send_message([{"type": "rich_text", "elements": elements}])

    def send_error_message(self, *, what: str, stderr: str = "") -> bool:
        if not self._webhooks:
            self._log.warning("Error message not sent; no webhooks configured")
            return False

        if stderr := stderr.strip():
            stderr = f":\n```\n{stderr}\n```"

        return self._send_message(
            [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":warning: {what}{stderr}",
                    },
                }
            ]
        )

    @staticmethod
    def _rich_text_list(elements: JSONList, *, indent: int = 0) -> JSON:
        return {
            "type": "rich_text_list",
            "style": "bullet",
            "indent": indent,
            "elements": elements,
        }

    def _describe_user_changes(
        self,
        *,
        username: str,
        displayname: str | None,
        updates: list[GroupChange],
        alert: bool,
    ) -> JSON:
        updates.sort(key=lambda it: (not it.warning, it.group))
        elements: list[float | str | bool | JSON] = []

        if alert:
            elements.append({"type": "emoji", "name": "octagonal_sign"})
            elements.append({"type": "text", "text": " "})
        elif any(it.warning for it in updates):
            elements.append({"type": "emoji", "name": "warning"})
            elements.append({"type": "text", "text": " "})

        if displayname is not None:
            elements.append({"type": "text", "text": f"{displayname} "})
            elements.append(
                {"type": "text", "text": f" ({username})", "style": {"italic": True}}
            )
        else:
            elements.append({"type": "text", "text": f"{username} "})

        elements.extend(self.add_change_section(username=username, updates=updates))

        return self._rich_text_list(
            [
                {
                    "type": "rich_text_section",
                    "elements": elements,
                }
            ]
        )

    def _add_user_alert(self, alert: str) -> JSON:
        elements: JSONList = [
            {"type": "text", "text": "NOTICE: ", "style": {"bold": True}}
        ]

        pattern = r"(<@[A-Za-z0-9]+>)"
        for chunk in re.split(pattern, alert):
            if re.match(pattern, chunk):
                elements.append({"type": "user", "user_id": chunk[2:-1]})
            elif chunk:
                elements.append({"type": "text", "text": chunk})

        return self._rich_text_list(
            [
                {
                    "type": "rich_text_section",
                    "elements": elements,
                }
            ],
            indent=1,
        )

    def add_change_section(
        self,
        *,
        username: str,
        updates: Iterable[GroupChange],
    ) -> Iterator[JSON]:
        for idx, (changes, values) in enumerate(
            groupby(updates, key=lambda it: it.changes)
        ):
            if idx:
                yield {"type": "text", "text": ";"}

            summary = self._summarize_changes(username=username, changes=changes)
            yield {"type": "text", "text": f" {summary} "}

            for it in pretty_list_t(tuple(values)):
                if isinstance(it, GroupChange):
                    if it.warning_sensitive:
                        style: JSON = {"style": {"code": True}}
                    elif it.warning_mandatory:
                        style = {"style": {"bold": True}}
                    else:
                        style = {}

                    yield {"type": "text", "text": it.group, **style}
                else:
                    yield {"type": "text", "text": it}

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
                self._log.error("for request %s", data)
                any_errors = True

        return not any_errors

    def _summarize_changes(
        self,
        *,
        username: str,
        changes: Sequence[ChangeType],
    ) -> str:
        if not changes:
            raise ValueError(changes)

        if len(changes) == 1:
            for value in changes:
                if value == ChangeType.ADD:
                    return "added to"
                elif value == ChangeType.DEL:
                    return "removed from"
                else:
                    raise NotImplementedError(value)

        idx = 0  # enumerate is not used, due to use of `continue`
        result: list[str] = []
        previous: ChangeType | None = None
        for value in changes:
            if value == previous:
                # This normally shouldn't happen
                self._log.warning("repeated changes found for user %r", username)
                continue

            if len(changes) > 1 and idx + 1 == len(changes):
                result.append(", and ")
            elif idx:
                result.append(", ")

            again = " again" if previous not in (None, value) else ""
            if value == ChangeType.ADD:
                result.append(f"added{again} to")
            elif value == ChangeType.DEL:
                result.append(f"removed{again} from")
            else:
                raise NotImplementedError(value)

            previous = value
            idx += 1

        return "".join(result)
