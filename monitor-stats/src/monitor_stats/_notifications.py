# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import TypeAlias, Union

import requests

from monitor_stats._monitor import BlacklistedProcess, Summary
from monitor_stats._utils import format_time, get_username

JSONValue: TypeAlias = Union[float, str, bool, "JSON"]
JSON: TypeAlias = dict[str, JSONValue] | list[JSONValue]


class SlackNotifier:
    def __init__(self, *, webhooks: list[str], timeout: float, host: str) -> None:
        self._log = logging.getLogger("slack")
        self._webhooks = list(webhooks)
        self._timeout = timeout
        self._host = host

    def notify(self, summary: Summary) -> bool:
        if not self._webhooks:
            self._log.warning("Slack LDAP update not sent; no webhooks configured")
            return False
        elif not (summary.system or summary.blacklisted):
            return False

        alerts: list[JSONValue] = []

        if summary.system:
            alerts.extend(
                self._add_entry(
                    f"Resource usage at {self._host} exceeds thresholds",
                    [
                        self._add_metrics(key, value, extra=summary.extras.get(key))
                        for key, value in summary.system.items()
                    ],
                )
            )

            if procs := summary.top_processes():
                alerts.extend(
                    self._add_entry(
                        "Top most resource intensive processes are",
                        [
                            self._add_process(
                                uid=it.uid,
                                pids=[it.pid],
                                cmdline=it.cmd,
                                cpu_mem=(it.cpu, it.mem),
                            )
                            for it in sorted(procs, key=lambda it: -max(it.cpu, it.mem))
                        ],
                        warning=False,
                    )
                )

        if summary.blacklisted:
            alerts.extend(
                self._add_entry(
                    f"Blacklisted process is running on {self._host}",
                    [
                        self._add_process(
                            uid=it.uid,
                            pids=it.pids,
                            cmdline=it.cmd,
                            runtime=it.runtime,
                        )
                        for it in BlacklistedProcess.merge(summary.blacklisted)
                    ],
                )
            )

        blocks: list[JSON] = [
            {
                "type": "rich_text",
                "elements": alerts,
            },
        ]

        return self._send_message(blocks)

    @classmethod
    def _add_entry(
        cls,
        message: str,
        elements: JSON,
        *,
        warning: bool = True,
    ) -> Iterable[JSON]:
        text: list[JSONValue] = []

        if warning:
            text.append({"type": "emoji", "name": "warning"})

        text.append({"type": "text", "text": f"{message} "})

        yield {"type": "rich_text_section", "elements": text}

        yield {
            "type": "rich_text_list",
            "style": "bullet",
            "indent": 0,
            "elements": elements,
        }

    @classmethod
    def _add_metrics(cls, name: str, value: float, extra: str | None = None) -> JSON:
        elems: list[JSONValue] = [
            {"type": "text", "text": f" {name} is currently at {value:.2f}"},
        ]

        if extra is not None:
            elems.append({"type": "text", "text": f"; {extra}"})

        return {"type": "rich_text_section", "elements": elems}

    @classmethod
    def _add_process(
        cls,
        *,
        uid: int,
        pids: list[int],
        cmdline: str,
        runtime: float | None = None,
        cpu_mem: tuple[float, float] | None = None,
    ) -> JSON:
        elements: list[JSONValue] = []
        username = get_username(uid)
        if len(pids) > 1:
            head = ", ".join(map(str, pids[:-1]))
            pidlist = f"{head}, and {pids[-1]}"
            elements.append({"type": "text", "text": f"Processes {pidlist} ("})
        else:
            (pid,) = pids
            elements.append({"type": "text", "text": f"Process {pid} ("})

        elements.append({"type": "text", "style": {"italic": True}, "text": username})
        elements.append({"type": "text", "text": ")"})

        if runtime is not None:
            elements.append(
                {
                    "type": "text",
                    "text": f" running for {format_time(runtime)}",
                },
            )

        if cpu_mem is not None:
            cpu, mem = cpu_mem
            elements.append(
                {
                    "type": "text",
                    "text": f" using {cpu:.1f} CPUs and {mem:.1f}% memory",
                },
            )

        if len(cmdline) > 200:
            cmdline = cmdline[:195] + "[...]"

        elements.append({"type": "text", "text": ": "})
        elements.append({"type": "text", "style": {"code": True}, "text": cmdline})

        return {
            "type": "rich_text_section",
            "elements": elements,
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
                self._log.error("for request %s", data)
                any_errors = True

        return not any_errors
