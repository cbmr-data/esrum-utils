from __future__ import annotations

import logging

from monitor_members.common import run_subprocess


class Sacctmgr:
    __slots__ = ["_account", "_cluster", "_log"]

    def __init__(self, cluster: str, account: str) -> None:
        self._cluster = cluster.lower()
        self._account = account.lower()
        self._log = logging.getLogger("sacct")

    def get_associations(self) -> set[str] | None:
        proc = run_subprocess(
            self._log,
            ["sacctmgr", "list", "Association", "--parsable2"],
        )

        if not proc:
            self._log.error("failed to get sacct users")
            proc.log_stderr(self._log)
            return None

        members: set[str] = set()
        header: list[str] | None = None
        for line in proc.stdout.splitlines():
            if line := line.strip():
                if header is None:
                    header = line.split("|")
                    continue

                row = dict(zip(header, line.split("|"), strict=True))
                if (
                    row["User"]
                    and row["Cluster"].lower() == self._cluster
                    and row["Account"].lower() == self._account
                ):
                    members.add(row["User"])

        return members
