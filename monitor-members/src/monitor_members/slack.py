from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Literal, TypeAlias, Union

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
        blocks: list[JSON] = [
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "Changes to LDAP groups for {}:\n\n".format(
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                ),
                            }
                        ],
                    }
                ],
            },
        ]

        # changes grouped by user
        user_updates: dict[str, list[GroupChange]] = defaultdict(list)
        for change in changes:
            user_updates[change.user].append(change)

        def _sort_key(it: tuple[str, list[GroupChange]]) -> tuple[int, str]:
            return (sum(-1 for change in it[1] if change.warning), it[0])

        for user, updates in sorted(user_updates.items(), key=_sort_key):
            updates.sort(key=lambda it: (not it.warning, it.group))

            if additions := [it for it in updates if it.change == ChangeType.ADD]:
                blocks.append(
                    self.add_section(
                        username=user,
                        displayname=displaynames[user],
                        action="added to",
                        changes=additions,
                    )
                )

            if removals := [it for it in updates if it.change == ChangeType.DEL]:
                blocks.append(
                    self.add_section(
                        username=user,
                        displayname=displaynames[user],
                        action="removed from",
                        changes=removals,
                    )
                )

        return self._send_message(blocks)

    @staticmethod
    def add_section(
        username: str,
        displayname: str | None,
        action: Literal["added to", "removed from"],
        changes: list[GroupChange],
    ) -> JSON:
        displayname = f"(_{displayname}_) " if displayname else ""

        important_groups: list[str] = []
        other_groups: list[str] = []

        for it in changes:
            if it.warning:
                important_groups.append(it.group)
            else:
                other_groups.append(it.group)

        summary: list[str] = []
        if important_groups:
            summary.append(":warning: *")
            summary.append(", ".join(important_groups))
            summary.append("* :warning: ")

        if other_groups:
            summary.append(", ".join(other_groups))

        return {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*{username}* {displayname}{action} ",
                },
                {
                    "type": "mrkdwn",
                    "text": "".join(summary),
                },
            ],
        }

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
