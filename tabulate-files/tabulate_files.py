#!/usr/bin/env python3
from __future__ import annotations

import argparse
import grp
import os
import pwd
import sys
from functools import lru_cache
from pathlib import Path


def escape(value: object) -> str:
    # Given the simple output format, it is necessary to escape some characters
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")


def output(*args: object) -> None:
    line = "\t".join(escape(it) for it in args)
    sys.stdout.buffer.write(line.encode("utf-8", errors="replace"))
    sys.stdout.buffer.write(b"\n")


@lru_cache
def username(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


@lru_cache
def groupname(gid: int) -> str:
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return str(gid)


def print_file_info(it: Path | os.DirEntry[str]) -> None:
    st = it.lstat() if isinstance(it, Path) else it.stat(follow_symlinks=False)

    output(
        f"{st.st_mode:o}",
        username(st.st_uid),
        groupname(st.st_gid),
        st.st_size,
        st.st_mtime_ns,
        os.fspath(it),
        # lint disabled, since `it` is only a Path some of the time
        os.readlink(it) if it.is_symlink() else "",  # noqa: PTH115
    )


def walk(root: Path | os.DirEntry[str]) -> None:
    print_file_info(root)

    try:
        for it in sorted(os.scandir(root), key=lambda it: it.name):
            if it.is_dir(follow_symlinks=False):
                walk(it)
            else:
                print_file_info(it)
    except PermissionError as error:
        print(error, file=sys.stderr)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="+", type=Path)

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    output("Mode", "User", "Group", "Size", "MTimeNS", "Path", "Link")
    try:
        for root in args.root:
            walk(root)
    except BrokenPipeError:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
