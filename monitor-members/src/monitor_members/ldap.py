from __future__ import annotations

import base64
import logging

from monitor_members.common import run_subprocess


class LDAP:
    __slots__ = [
        "_ldapsearch_exe",
        "_log",
        "_searchbase",
        "_uri",
    ]

    _log: logging.Logger
    _uri: str
    _searchbase: str

    def __init__(
        self,
        *,
        ldapsearch_exe: str = "ldapsearch",
        uri: str,
        searchbase: str,
    ) -> None:
        self._log = logging.getLogger("ldap")
        self._uri = uri
        self._searchbase = searchbase
        self._ldapsearch_exe = ldapsearch_exe

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
                self._ldapsearch_exe,
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

        lines: list[str] = []

        attr_key_b64 = f"{attr}:: "
        attr_key = f"{attr}: "
        for line in proc.stdout.splitlines():
            if line.startswith(attr_key):
                lines.append(line[len(attr_key) :].strip())
            elif line.startswith(attr_key_b64):
                value = line[len(attr_key_b64) :].strip()
                lines.append(base64.b64decode(value).decode("utf-8", errors="replace"))

        return lines
