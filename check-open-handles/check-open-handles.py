#!/usr/bin/python3.11
# pyright: strict
from __future__ import annotations

import argparse
import functools
import shlex
import sys
from pathlib import Path

AUTOFS = {Path("/maps/projects"), Path("/maps/datasets")}
NFSDIRS = {"groupdir", "hdir", "sdir"}


class Commandline:
    def __init__(self, it: Path) -> None:
        self._path = it
        self._cmdline = None

    def __str__(self) -> str:
        if self._cmdline is None:
            try:
                self._cmdline = " ".join(
                    (self._path / "cmdline").read_text().split("\0")
                ).strip()
            except OSError:
                self._cmdline = ""

        return self._cmdline


class Owner:
    def __init__(self, it: Path) -> None:
        self._path = it
        self._owner = None

    def __str__(self) -> str:
        if self._owner is None:
            try:
                self._owner = self._path.owner()
            except OSError:
                self._owner = "unknown"

        return self._owner


def quote(value: str | Path | Commandline) -> str:
    return shlex.quote(str(value))


def evaluate(it: Path) -> str:
    try:
        return "OK" if it.exists() else "BAD"
    except PermissionError:
        return "PRM"
    except OSError:
        return "UNK"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument("files", nargs="*", type=Path)
    parser.add_argument("--verbose", help="Print all candidates")

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    dirs = args.files or Path("/proc").iterdir()
    for it in dirs:
        if it.name.isdigit() and it.is_dir():
            cmdline = Commandline(it)
            owner = Owner(it)

            try:
                cwd = (it / "cwd").resolve()
            except OSError as error:
                print(it.name, "ERR", error, file=sys.stderr)
                continue

            # Crude check since we may not be able to resolve paths to /maps
            if cwd in AUTOFS or NFSDIRS.intersection(cwd.parts):
                print(
                    owner,
                    it.name,
                    "CWD",
                    quote(cwd),
                    quote(cmdline),
                    sep="\t",
                )

            try:
                fds = list((it / "fd").iterdir())
            except OSError:
                continue

            for fd in fds:
                try:
                    cwd = fd.resolve()
                except OSError:
                    continue

                # Crude check since we may not be able to resolve paths to /maps
                if cwd in AUTOFS or NFSDIRS.intersection(fd.parts):
                    print(
                        owner,
                        it.name,
                        f"fd={fd.name}",
                        quote(cwd),
                        quote(cmdline),
                        sep="\t",
                    )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
