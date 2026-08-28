from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import datetime, timedelta

from monitor_members.common import quote, run_subprocess
from monitor_members.slack import SlackNotifier

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
        self._authenticated = False

    def refresh(
        self,
        *,
        notifier: SlackNotifier | None = None,
        what: str = "Refreshing kerberos ticket",
    ) -> bool:
        """Attempts to refresh the current ticket, if any, or create a new ticket using
        the supplied keytab. Returns true if either succeeds."""

        was_authenticated = self._authenticated
        self._authenticated = False

        # Attempt to renew existing ticket (if any)
        if not (proc := run_subprocess(self._log, [self._kinit_exe, "-R"])):
            self._log.warning("could not refresh existing kerberos tickets (if any)")
            proc.log_stderr(self._log, level=logging.WARNING)

            if self._keytab is not None and self._username is not None:
                command = [self._kinit_exe, self._username, "-k", "-t", self._keytab]
                if not (proc := run_subprocess(self._log, command)):
                    self._log.error("failed to generate kerberos ticket:")
                    proc.log_stderr(self._log)

                    if was_authenticated and notifier:
                        notifier.send_error_message(
                            what=f"{what}: Failed to generate ticket from keytab",
                            stderr=proc.stderr,
                        )

                    return False

                self._log.info("generated new kerberos ticket")
                self._authenticated = True
                return True
            else:
                self._log.warning("keytab and username required to generate ticket")
                if was_authenticated and notifier:
                    notifier.send_error_message(
                        what=f"{what}: Could not refresh kerberos ticket and no "
                        "keytab file was provided/configured",
                    )

                return False

        self._log.info("refreshed existing kerberos ticket")
        self._authenticated = True
        return True

    def authenticated_loop(
        self,
        *,
        interval: float,
        notifier: SlackNotifier | None = None,
        what: str = "Refreshing kerberos ticket",
    ) -> Iterator[None]:
        while True:
            if self.refresh(notifier=notifier, what=what):
                yield None
            else:
                self._log.warning("unable to check group memberships")

            if interval <= 0:
                break

            try:
                wake_at = datetime.now() + timedelta(seconds=interval)
                self._log.info("Next loop at %s", wake_at)
                time.sleep(interval)
            except KeyboardInterrupt:
                break

    def _log_stderr(self, executable: str, stderr: str) -> None:
        for line in stderr.splitlines():
            if line := line.rstrip():
                self._log.error("%s: %s", quote(executable), line)

    def __bool__(self) -> bool:
        return self._authenticated
