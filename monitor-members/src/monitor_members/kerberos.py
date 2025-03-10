from __future__ import annotations

import logging

from monitor_members.common import quote, run_subprocess

__all__ = [
    "Kerberos",
]


class Kerberos:
    def __init__(
        self,
        *,
        keytab: str | None = None,
        username: str | None = None,
        kinit_exe: str = "kinit",
    ) -> None:
        self._log = logging.getLogger("kerberos")
        self._keytab = keytab
        self._username = username
        self._kinit_exe = kinit_exe

    def refresh(self) -> bool:
        """Attempts to refresh the current ticket, if any, or create a new ticket using
        the supplied keytab. Returns true if either succeeds."""

        # Attempt to renew existing ticket (if any)
        if not (proc := run_subprocess(self._log, [self._kinit_exe, "-R"])):
            self._log.warning("could not refresh existing kerberos tickets (if any)")
            proc.log_stderr(self._log, level=logging.WARNING)

            if self._keytab is not None and self._username is not None:
                command = [self._kinit_exe, self._username, "-k", "-t", self._keytab]
                if not (proc := run_subprocess(self._log, command)):
                    self._log.error("failed to generate kerberos ticket:")
                    proc.log_stderr(self._log)
                    return False

                self._log.info("generated new kerberos ticket")
                return True
            else:
                self._log.warning("keytab and username required to generate ticket")
                return False

        self._log.info("refreshed existing kerberos ticket")

        return True

    def _log_stderr(self, executable: str, stderr: str) -> None:
        for line in stderr.splitlines():
            if line := line.rstrip():
                self._log.error("%s: %s", quote(executable), line)
