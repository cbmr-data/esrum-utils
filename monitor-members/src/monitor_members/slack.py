from __future__ import annotations

import dataclasses
import fnmatch
import json
import logging
import re
from collections.abc import Iterable, Sequence
from itertools import groupby
from typing import ClassVar, Literal

import requests

from monitor_members.common import pretty_list_t
from monitor_members.database import ChangeType, GroupChange, ProfileChange, UserChange
from monitor_members.groups import GroupType

type JSONList = list[float | str | bool | JSON]
type JSON = dict[
    str,
    float | str | bool | JSON | JSONList,
]


class SlackNotifier:
    def __init__(self, *, webhooks: list[str], timeout: float, verbose: bool) -> None:
        self._log = logging.getLogger("slack")
        self._webhooks = list(webhooks)
        self._timeout = timeout
        self._verbose = verbose

    def send_ldap_notification(
        self,
        changes: Iterable[UserChange],
        *,
        sensitive_users: dict[str, str],
        primary_group: str | None,
    ) -> bool:
        if not self._webhooks:
            self._log.warning("Slack LDAP update not sent; no webhooks configured")
            return False

        elements: list[RichTextSection | RichTextList] = []

        def _sort_key(it: UserChange) -> tuple[int, str]:
            return (
                sum(-1 for change in it.group_changes if change.warning),
                it.display_name,
            )

        for it in sorted(changes, key=_sort_key):
            elements.extend(
                self._describe_user_changes(
                    changes=it,
                    sensitive_users=sensitive_users,
                    primary_group=primary_group,
                )
            )

        return self._send_message([RichText(elements=elements)])

    def send_error_message(self, *, what: str, stderr: str = "") -> bool:
        if not self._webhooks:
            self._log.warning("Error message not sent; no webhooks configured")
            return False

        if stderr := stderr.strip():
            stderr = f":\n```\n{stderr}\n```"

        return self._send_message(
            [
                RichText(
                    [
                        RichTextSection([Emoji("warning"), Text(f" {what}{stderr}")]),
                    ]
                ),
            ]
        )

    def _describe_user_changes(
        self,
        changes: UserChange,
        sensitive_users: dict[str, str],
        primary_group: str | None,
    ) -> Iterable[RichTextList]:
        initial_indent = 0
        # These changes are always added to other updates
        alerts = self._add_user_alerts(
            changes=changes,
            sensitive_users=sensitive_users,
            primary_group=primary_group,
        )

        if alerts or changes.group_changes and changes.profile_changes:
            initial_indent = 1
            yield RichTextList([RichTextSection(self._add_user_names(changes))])

        if changes.group_changes:
            changes.group_changes.sort(key=lambda it: (not it.warning, it.group))
            warning = any(it.warning for it in changes.group_changes)
            header: list[Text | Emoji | User] = []
            if initial_indent:
                if warning:
                    header += [Emoji("warning"), Text(" ")]
            else:
                header = self._add_user_names(changes, warning=warning)

            yield from self._add_group_changes_section(
                username=changes.username,
                updates=changes.group_changes,
                header=header,
                indent=initial_indent,
            )

        for change in changes.profile_changes:
            yield from self._add_profile_changes_section(
                change=change,
                header=[] if initial_indent else self._add_user_names(changes),
                display_name=changes.display_name,
                indent=initial_indent,
            )

        yield from alerts

    def _add_user_names(
        self,
        changes: UserChange,
        warning: bool = False,
    ) -> list[Text | Emoji | User]:
        elements: list[Text | Emoji | User] = []
        if warning:
            elements.append(Emoji("warning"))
            elements.append(Text(" "))

        if changes.display_name != changes.username:
            elements.append(Text(f"{changes.display_name} "))
            elements.append(Text(f"({changes.username})", italic=True))
        else:
            elements.append(Text(f"{changes.username} "))

        return elements

    def _add_user_alerts(
        self,
        changes: UserChange,
        sensitive_users: dict[str, str],
        primary_group: str | None,
    ) -> list[RichTextList]:
        messages = [
            message
            for glob, message in sensitive_users.items()
            if fnmatch.fnmatch(name=changes.username.lower(), pat=glob.lower())
        ]

        alerts: list[RichTextList] = []
        if messages:
            alert = "; ".join(messages)
            self._log.warning("sensitive user %r: %s", changes.username, alert)

            elements: list[Text | Emoji | User] = []

            pattern = r"(<@[A-Za-z0-9]+>)"
            for chunk in re.split(pattern, alert):
                if re.match(pattern, chunk):
                    elements.append(User(user_id=chunk[2:-1]))
                elif chunk:
                    elements.append(Text(chunk))

            alerts.append(self._add_user_alert(elements))

        if changes.orphan_memberships and primary_group is not None:
            elements = [
                Text(f"is not in {primary_group}, but "),
                Text("is", italic=True),
                Text(" in "),
            ]

            for idx, (group, type) in enumerate(changes.orphan_memberships.items()):
                if idx > 0 and idx + 1 == len(changes.orphan_memberships):
                    elements.append(Text(", and "))
                elif idx:
                    elements.append(Text(", "))

                elements.append(self._format_group_name(group, type, True))

            alerts.append(self._add_user_alert(elements))

        return alerts

    @staticmethod
    def _add_user_alert(text: Iterable[Text | Emoji | User]) -> RichTextList:
        text = [Emoji("octagonal_sign"), Text(" "), *text]
        return RichTextList([RichTextSection(text)], indent=1)

    def _add_group_changes_section(
        self,
        username: str,
        updates: list[GroupChange],
        header: Iterable[Text | Emoji | User],
        indent: int = 0,
    ) -> Iterable[RichTextList]:
        text: list[Text | Emoji | User] = list(header)
        for idx, (changes, values) in enumerate(
            groupby(updates, key=lambda it: it.changes)
        ):
            if idx:
                text.append(Text(";"))

            summary = self._summarize_changes(username=username, changes=changes)
            text.append(Text(f" {summary} "))

            for it in pretty_list_t(tuple(values)):
                if isinstance(it, GroupChange):
                    mark_mandatory = it.changes[-1] == ChangeType.DEL
                    name = self._format_group_name(
                        it.group, it.group_type, mark_mandatory
                    )
                    text.append(name)
                else:
                    text.append(Text(it))

        yield RichTextList([RichTextSection(text)], indent=indent)

    def _add_profile_changes_section(
        self,
        display_name: str,
        change: ProfileChange,
        header: Iterable[Text | Emoji | User],
        indent: int = 0,
    ) -> Iterable[RichTextList]:
        yield RichTextList(
            [RichTextSection([*header, Text(" has profile changes:")])], indent=indent
        )

        for key, values in change.changes.items():
            # Don't display the name twice if unchanged
            if values.old != values.new or key != "display_name":
                if key == "display_name":
                    key = "name"

                text: list[Text | Emoji | User] = [
                    Text(key.title(), bold=(values.old != values.new)),
                    Text(f": {values.old}"),
                ]

                if values.old != values.new:
                    text.append(Text(" ➡ "))
                    text.append(Text(values.new, bold=True))

                yield RichTextList([RichTextSection(text)], indent=indent + 1)

    def _send_message(self, blocks: list[RichText]) -> bool:
        data = {"blocks": [it.to_json() for it in blocks]}
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

    @staticmethod
    def _format_group_name(
        name: str,
        group_type: GroupType,
        mark_mandatory: bool = False,
    ) -> Text:
        bold = code = False
        if group_type == GroupType.SENSITIVE:
            code = True
        elif group_type == GroupType.MANDATORY and mark_mandatory:
            bold = True

        return Text(name, bold=bold, code=code)


@dataclasses.dataclass
class RichText:
    type: ClassVar[str] = "rich_text"
    elements: list[RichTextSection | RichTextList] = dataclasses.field(
        default_factory=list
    )

    def to_json(self) -> JSON:
        return {"type": self.type, "elements": [it.to_json() for it in self.elements]}


@dataclasses.dataclass
class RichTextList:
    type: ClassVar[str] = "rich_text_list"
    elements: list[RichTextSection] = dataclasses.field(default_factory=list)
    style: Literal["bullet", "ordered", "check"] = "bullet"
    indent: int = 0

    def to_json(self) -> JSON:
        return {
            "type": self.type,
            "style": self.style,
            "indent": self.indent,
            "elements": [it.to_json() for it in self.elements],
        }


@dataclasses.dataclass
class RichTextSection:
    type: ClassVar[str] = "rich_text_section"
    elements: list[Text | Emoji | User] = dataclasses.field(default_factory=list)

    def to_json(self) -> JSON:
        return {"type": self.type, "elements": [it.to_json() for it in self.elements]}


@dataclasses.dataclass
class Text:
    type: ClassVar[str] = "text"
    text: str
    italic: bool = False
    bold: bool = False
    code: bool = False

    def to_json(self) -> JSON:
        italic = self.italic
        text = self.text
        # Slack does not allow empty Text fields
        if not text:
            italic = True
            text = "N/A"

        value: JSON = {"type": self.type, "text": text}
        if self.bold or self.code:
            value["style"] = {
                "italic": italic,
                "bold": self.bold,
                "code": self.code,
            }
        return value


@dataclasses.dataclass
class Emoji:
    type: ClassVar[str] = "emoji"
    name: str

    def to_json(self) -> JSON:
        return {"type": self.type, "name": self.name}


@dataclasses.dataclass
class User:
    type: ClassVar[str] = "user"
    user_id: str

    def to_json(self) -> JSON:
        return {"type": self.type, "user_id": self.user_id}
