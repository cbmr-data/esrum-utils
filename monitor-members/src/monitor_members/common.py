from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
import sys
from collections.abc import Iterator, Sequence
from functools import wraps
from pathlib import Path
from typing import Callable, Literal, NoReturn, TypeVar

import coloredlogs

T = TypeVar("T")


def main_func(func: Callable[[T], int]) -> Callable[[T], None]:
    # Ensure that tap finds the correct annotations
    @wraps(func)
    def _wrapper(arg: T) -> None:
        sys.exit(func(arg))

    return _wrapper


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def abort(*args: object) -> NoReturn:
    eprint("ERROR: ", *args)
    sys.exit(1)


def quote(*values: object) -> str:
    return " ".join(shlex.quote(str(value)) for value in values)


def which(name: str) -> str:
    if (executable := shutil.which(name)) is not None:
        return executable

    return name


def parse_duration(value: str) -> float:
    mult = 1

    value = value.lower()
    for key, seconds in (("d", 24 * 60 * 60), ("h", 60 * 60), ("m", 60), ("s", 1)):
        if value.endswith(key):
            value = value[:-1]
            mult = seconds
            break

    return float(value) * mult


def setup_logging(
    name: str,
    *,
    log_level: Literal["ERROR", "WARNING", "INFO", "DEBUG"],
    log_sql: bool = False,
) -> logging.Logger:
    coloredlogs.install(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        level=log_level,
        milliseconds=True,
    )

    if log_sql:
        # Echo SQL alchemy commands to log
        logging.getLogger("sqlalchemy.engine").setLevel(log_level)

    return logging.getLogger(name)


class CommandOutput:
    def __init__(
        self,
        *,
        command: tuple[str | Path, ...],
        returncode: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def log_stderr(self, log: logging.Logger, *, level: int = logging.ERROR) -> None:
        executable = quote(self.command[0])
        log.log(level, "%s terminated with returncode %i", executable, self.returncode)

        if stderr := self.stderr.rstrip():
            for line in stderr.splitlines():
                if line := line.rstrip():
                    log.log(level, "%s: %s", executable, line)

    def __bool__(self) -> bool:
        return self.returncode == 0


def run_subprocess(
    log: logging.Logger,
    command: Sequence[str] | Sequence[str | Path],
) -> CommandOutput:
    log.debug("Running command %s", command)
    if not command:
        raise ValueError(command)

    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        encoding="utf-8",
        shell=False,
    )

    try:
        stdout, stderr = proc.communicate()
    except OSError:
        log.exception("error while calling %s", quote(command[0]))
        raise

    return CommandOutput(
        command=tuple(command),
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )


def pretty_list(items: Sequence[object]) -> str:
    return "".join(str(v) for v in pretty_list_t(items))


def pretty_list_t(items: Sequence[T]) -> Iterator[str | T]:
    if len(items) == 0:
        yield "N/A"
    elif len(items) == 1:
        yield from items
    elif len(items) == 2:
        yield items[0]
        yield " and "
        yield items[1]
    else:
        for idx, item in enumerate(items):
            if idx + 1 == len(items):
                yield ", and "
            elif idx:
                yield ", "

            yield item
