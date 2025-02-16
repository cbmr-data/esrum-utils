from __future__ import annotations

import logging

from monitor_members.common import run_subprocess


class LDAP:
    __slots__ = [
        "_log",
        "_searchbase",
        "_uri",
    ]

    _log: logging.Logger
    _uri: str
    _searchbase: str

    def __init__(self, *, uri: str, searchbase: str) -> None:
        self._log = logging.getLogger("ldap")
        self._uri = uri
        self._searchbase = searchbase

    def display_name(self, name: str) -> str | None:
        if (lines := self._get(name, "displayName")) is not None:
            for line in lines:
                if line := line.strip():
                    return line

    def members(self, name: str) -> set[str] | None:
        if (lines := self._get(name, "member")) is not None:
            members: set[str] = set()
            for line in lines:
                for field in line.split(","):
                    if (field := field.strip()) and field.startswith("CN="):
                        members.add(field[3:])

            return members

    def _get(self, key: str, attr: str) -> list[str] | None:
        self._log.debug("fetching %r for LDAP key %r", attr, key)
        proc = run_subprocess(
            self._log,
            [
                "ldapsearch",
                "-o",
                "ldif-wrap=no",
                "-LLL",
                "-Q",
                "-H",
                self._uri,
                "-b",
                self._searchbase,
                "--",
                f"(cn={key})",
                attr,
            ],
        )

        if not proc:
            self._log.error("failed to fetch %r for LDAP key %r", attr, key)
            proc.log_stderr(self._log)
            return None

        attr = f"{attr}: "
        return [
            line[len(attr) :].strip()
            for line in proc.stdout.splitlines()
            if line.startswith(attr)
        ]
