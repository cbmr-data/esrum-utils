from __future__ import annotations

import base64
import contextlib
import dataclasses
import itertools
import logging

from monitor_members.common import run_subprocess


@dataclasses.dataclass
class LDAPUser:
    cn: str
    display_name: str
    department: str
    division: str
    title: str


@dataclasses.dataclass
class LDAPGroup:
    cn: str
    members: list[str]


class LDAP:
    __slots__ = [
        "_batch_size",
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
        batch_size: int = 10,
    ) -> None:
        self._log = logging.getLogger("ldap")
        self._uri = uri
        self._searchbase = searchbase
        self._ldapsearch_exe = ldapsearch_exe
        self._batch_size = batch_size

    def get_users(self, cns: list[str]) -> dict[str, LDAPUser] | None:
        matches = self._batch_query(
            cns, ("displayName", "department", "division", "title")
        )
        if matches is None:
            return None

        results: dict[str, LDAPUser] = {}
        for cn, result in matches.items():
            results[cn] = LDAPUser(
                cn=cn,
                display_name=self._get_single_value(cn, "displayName", result),
                department=self._get_single_value(cn, "department", result),
                division=self._get_single_value(cn, "division", result),
                title=self._get_single_value(cn, "title", result),
            )

        return results

    def get_groups(self, cns: list[str]) -> dict[str, LDAPGroup] | None:
        matches = self._batch_query(cns, ("member",))
        if matches is None:
            return None

        results: dict[str, LDAPGroup] = {}
        for cn, result in matches.items():
            members: list[str] = []
            for value in self._get_multiple_values("member", result):
                if member := self._parse_dn(value):
                    members.append(member)
                else:
                    self._log.error("Could not parse member for %r: %r", cn, value)
                    return None

            results[cn] = LDAPGroup(cn=cn, members=members)

        return results

    def _batch_query(
        self,
        cns: list[str] | tuple[str, ...],
        attrs: list[str] | tuple[str, ...],
    ) -> dict[str, dict[str, str | list[str]]] | None:
        sorted_cns = sorted(set(cns))
        sorted_attrs = sorted(set(attrs))

        results: dict[str, dict[str, str | list[str]]] = {}
        for batch in itertools.batched(sorted_cns, n=self._batch_size):
            result = self._query(batch, sorted_attrs)
            if result is None:
                return None

            results.update(result)

        return results

    def _query(
        self,
        cns: list[str] | tuple[str, ...],
        attrs: list[str] | tuple[str, ...],
    ) -> dict[str, dict[str, str | list[str]]] | None:
        query = "".join(f"(cn={cn})" for cn in cns)
        if len(cns) > 1:
            query = f"(|{query})"

        self._log.debug("fetching %r for LDAP key %r", attrs, query)
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
                query,
                *attrs,
            ],
        )

        if not proc:
            self._log.error("failed to get %r for LDAP query %r", attrs, query)
            proc.log_stderr(self._log)
            return None

        results: dict[str, dict[str, str | list[str]]] = {}
        current_fields: dict[str, str | list[str]] = {}

        def _add_current_fields() -> bool:
            if current_fields:
                dn = current_fields.get("dn")
                if not dn:
                    self._log.error("found records before DN: %r", current_fields)
                    return False

                cn = self._parse_dn(dn)
                if cn is None:
                    self._log.error("invalid DN found %r", dn)
                    return False

                if cn in results:
                    self._log.error("duplicate CN %r in query results", cn)
                    return False

                results[cn] = dict(current_fields)
                current_fields.clear()

            return True

        for line in proc.stdout.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = self._parse_field(line)
                existing_value = current_fields.get(key)
                if existing_value is None:
                    current_fields[key] = value
                elif isinstance(existing_value, list):
                    existing_value.append(value)
                else:
                    current_fields[key] = [existing_value, value]
            elif not _add_current_fields():
                return None

        return results if _add_current_fields else None

    @staticmethod
    def _parse_field(line: str) -> tuple[str, str]:
        key, value = line.split(None, 1)
        if key.endswith("::"):
            with contextlib.suppress(UnicodeDecodeError):
                value = base64.b64decode(value).decode()

        return key.rstrip(":"), value

    @staticmethod
    def _parse_dn(values: str | list[str]) -> str | None:
        if not isinstance(values, list):
            values = [values]

        for value_pairs in values:
            for key_value in value_pairs.split(","):
                if key_value.startswith("CN="):
                    _, value = key_value.split("=", 1)
                    # normalize to lowercase for simplicity
                    return value.lower()

        return None

    def _get_single_value(
        self,
        cn: str,
        key: str,
        properties: dict[str, str | list[str]],
        default: str = "",
    ) -> str:
        values = properties.get(key)
        if values is None:
            return default
        elif isinstance(values, str):
            return values

        value_set = set(values)
        if len(value_set) == 1:
            (values,) = value_set
            return values

        self._log.warning("Record %r has multiple %r values: %r", cn, key, values)
        return values[0]  # arbitrary value

    @staticmethod
    def _get_multiple_values(
        key: str,
        properties: dict[str, str | list[str]],
    ) -> list[str]:
        values = properties.get(key)
        if isinstance(values, str):
            return [values]
        elif values:
            return values
        else:
            return []
