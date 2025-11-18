#!/usr/bin/env python3
# pyright: strict
from __future__ import annotations

import argparse
import functools
import os
import pwd
import shlex
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MIN_UID = 65535

DEFAULT_ROOTS = (
    Path("/tmp").resolve(),  # noqa: S108
    Path("/scratch").resolve(),
    Path("/scratch/tmp").resolve(),
)

DEFAULT_PATH_SKIPLIST = (
    Path("/scratch/containers").resolve(),
    Path("/scratch/rstudio").resolve(),
    Path("/scratch/rstudio-proj").resolve(),
)


@functools.cache
def username(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def escape(it: Path) -> str:
    return shlex.quote(str(it))


def wipe_folder(
    *,
    root: Path,
    min_uid: int,
    max_time: float,
    uid_skiplist: frozenset[int],
    path_skiplist: frozenset[Path],
    commit: bool,
) -> bool:
    unhandled_entries = 0
    if root in path_skiplist:
        print("SKIP PROTECTED-PATH", escape(root))
        return False

    def print_with_owner(msg: str, it: Path, stats: os.stat_result) -> None:
        print(msg, "@", username(stats.st_uid), escape(it))

    try:
        for it in sorted(root.iterdir()):
            unhandled_entries += 1
            if it in path_skiplist:
                print("SKIP PROTECTED-PATH", escape(it))
                continue

            try:
                stats = os.lstat(it)
            except FileNotFoundError:
                print("SKIP FILE-NOT-FOUND", escape(it))
                continue
            except PermissionError:
                print("SKIP PERMISSION", escape(it))
                continue

            if stats.st_uid < min_uid:
                print_with_owner("SKIP LOCAL-USER", it, stats)
            elif stats.st_uid in uid_skiplist:
                print_with_owner("SKIP ACTIVE-USER", it, stats)
            elif max(stats.st_atime, stats.st_ctime, stats.st_mtime) >= max_time:
                print_with_owner("SKIP NEW-FILE-OR-FOLDER", it, stats)
            elif stat.S_ISDIR(stats.st_mode):
                if wipe_folder(
                    root=it,
                    min_uid=min_uid,
                    max_time=max_time,
                    uid_skiplist=uid_skiplist,
                    path_skiplist=path_skiplist,
                    commit=commit,
                ):
                    unhandled_entries -= 1
            else:
                print_with_owner("DEL FILE", it, stats)
                if commit:
                    try:
                        it.unlink()
                        unhandled_entries -= 1
                    except OSError as error:
                        print("FAIL DELETE-FILE @", username(stats.st_uid), error)
    except PermissionError:
        print("SKIP PERMISSION", escape(root))
        return False

    if commit and unhandled_entries == 0:
        print("DEL FOLDER", escape(root))
        try:
            root.rmdir()
            return True
        except OSError as error:
            print("FAIL DELETE-FOLDER", error)

    return False


def collect_uids(min_uid: int) -> frozenset[int]:
    uids: set[int] = set()
    for it in Path("/proc").iterdir():
        if it.name.isdigit():
            try:
                stats = it.stat()
                if stats.st_uid >= min_uid:
                    uids.add(stats.st_uid)
            except FileNotFoundError:
                pass

    return frozenset(uids)


@dataclass
class Args:
    roots: list[Path]
    commit: bool
    min_uid: int
    force_delete_uid: list[int]
    min_age: float


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "roots",
        nargs="*",
        type=Path,
        help="Locations to clean; defaults to /tmp, /scratch, and /scratch/tmp if not "
        "set manually",
    )

    parser.add_argument(
        "--min-uid",
        type=int,
        default=DEFAULT_MIN_UID,
        help="User IDs below this value are ignored. This is intended to filter system "
        "processes and local users",
    )

    parser.add_argument(
        "--min-age",
        metavar="N",
        type=float,
        default=24.0,
        help="Skip files that have been created/modified/accessed in the last N hours",
    )

    parser.add_argument(
        "--force-delete-uid",
        type=int,
        default=[],
        action="append",
        help="Always delete the files for the specified UID",
    )

    parser.add_argument(
        "--commit",
        default=False,
        action="store_true",
        help="Actually delete/files and folders",
    )

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if not args.roots:
        args.roots = list(DEFAULT_ROOTS)

    # Files/folders created/modified/accessed after this time are ignored
    max_time = time.time() - args.min_age

    active_users = collect_uids(args.min_uid) - frozenset(args.force_delete_uid)
    for uid in sorted(active_users):
        print("SKIP USERNAME", username(uid))

    for root in args.roots:
        root = root.resolve()
        if root.exists():
            wipe_folder(
                root=root,
                min_uid=args.min_uid,
                max_time=max_time,
                uid_skiplist=active_users,
                path_skiplist=frozenset(DEFAULT_PATH_SKIPLIST),
                commit=args.commit,
            )
        else:
            print("SKIP FILE-NOT-FOUND", root)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
