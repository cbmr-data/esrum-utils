import logging
import subprocess


class Mailer:
    def __init__(self, *, smtpserver: str, recipients: list[str]) -> None:
        self._smtpserver = smtpserver
        self._recipients = tuple(recipients)
        if not self._recipients:
            raise ValueError("no recipients specified")

    def __call__(self, message: str, *, dry_run: bool = False) -> bool:
        logger = logging.getLogger(__name__)
        if dry_run:
            logger.info("Would email %i recipients: %r", len(self._recipients), message)
            return True

        logger.debug("Sending email to %i recipients", len(self._recipients))
        try:
            proc = subprocess.Popen(
                [
                    "/usr/bin/mail",
                    "-S",
                    f"smtp={self._smtpserver}",
                    "-s",
                    "Esrum: Changes to node status",
                    *self._recipients,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            stdout, _ = proc.communicate(input=message.encode("utf-8"))
        except OSError as error:
            logger.error("Error sending email notification: %s:", error)
            return False

        if proc.returncode:
            logger.error("Error sending email notification (%s):", proc.returncode)
            for line in stdout.splitlines():
                logger.error("    > %s", line.rstrip())

        return not proc.returncode
