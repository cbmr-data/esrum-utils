#!/usr/bin/env python3
from __future__ import annotations

import argparse
import functools
import grp
import os
import pwd
import shlex
import stat
import sys
from pathlib import Path
from typing import Iterable, NoReturn, TypeVar

T = TypeVar("T")


def tqdm(seq: Iterable[T]) -> Iterable[T]:
    if sys.stderr.isatty():
        try:
            import tqdm

            return tqdm.tqdm(seq, unit=" entry", unit_scale=True)
        except ImportError:
            pass

    return seq


def warning(*args: object) -> None:
    print("WARNING:", *args, file=sys.stderr)


def abort(*args: object) -> NoReturn:
    print("ERROR:", *args, file=sys.stderr)
    sys.exit(1)


def quote(value: object) -> str:
    return shlex.quote(str(value))


def get_group_id(name: str) -> int:
    try:
        groupinfo = grp.getgrnam(name)
    except KeyError:
        abort(f"Group with name {name} not found!")

    return groupinfo.gr_gid


def get_group_name(gid: int) -> str:
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return str(gid)


def get_user_name(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
        description="""This script is used to fix permissions and group ownership so
        only the owner has write access (if already set), and so that "group" members
        and "others" only have read-access. This corresponds to the default permissions
        in project folders. Group members can optionally be given write access to
        already writable files/folders and others may be restricted.
        """,
    )
    parser.add_argument(
        "group",
        help="Owning group for all folders/files",
    )
    parser.add_argument(
        "root",
        nargs="+",
        type=Path,
        help="One or more paths for which group-ownership/permissions are to be fixed",
    )
    parser.add_argument(
        "--ignore-missing-permissions",
        action="store_true",
        help="By default this script will ensure that files are readable and that "
        "folders are readable and executable, for both the owner and group. Use this "
        "to disable setting this permissions",
    )
    parser.add_argument(
        "--no-group-bit",
        action="store_true",
        help="Do not set S_ISGID for folders; S_ISGID ensures that new files inherit "
        "the folder group. The S_ISGID bit is not cleared from folders where it is "
        "already set",
    )
    parser.add_argument(
        "--group-writable",
        action="store_true",
        help="Files/folders should be group writable if the file/folder is writable",
    )
    parser.add_argument(
        "--other",
        action="store_true",
        help="Allow read access for 'other' users",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Apply planned changes",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print changes",
    )

    return parser.parse_args(argv)


def walk(
    *roots: os.DirEntry[str] | Path,
) -> Iterable[tuple[Path, Path | os.DirEntry[str], os.stat_result]]:
    queue = list(roots)
    while queue:
        root = queue.pop()
        if isinstance(root, os.DirEntry):
            yield Path(root), root, root.stat(follow_symlinks=False)
        else:
            yield root, root, os.lstat(root)

        for it in os.scandir(root):
            if it.is_dir(follow_symlinks=False):
                yield from walk(it)
            else:
                yield Path(it), it, it.stat(follow_symlinks=False)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    gid = get_group_id(args.group)
    uid = os.getuid()

    min_file_premissions = 0 if args.ignore_missing_permissions else 0o440
    min_folder_premissions = 0 if args.ignore_missing_permissions else 0o550

    group_mask = 0o070 if args.group_writable else 0o050
    other_mask = 0o005 if args.other else 0o000

    for filepath, direntry, stats in tqdm(walk(*args.root)):
        if stats.st_uid != uid:
            warning("Path is owned by different user:", quote(filepath))
            continue
        elif stats.st_gid != gid:
            if not args.quiet:
                print(
                    "lchown {} since ownership is {}/{}".format(
                        quote(filepath),
                        get_user_name(stats.st_uid),
                        get_group_name(stats.st_gid),
                    )
                )

            if args.commit:
                os.lchown(filepath, uid, gid)

        if not direntry.is_symlink():
            # Group perms same as owner perms, but read-only (by default)
            owner_mode = stats.st_mode & 0o700
            group_mode = (owner_mode >> 3) & group_mask
            other_mode = (owner_mode >> 6) & other_mask
            misc = stats.st_mode & 0o7000
            mode = owner_mode | group_mode | other_mode | misc

            if direntry.is_dir():
                mode |= min_folder_premissions

                if args.no_group_bit:
                    # Inherit S_ISGID
                    mode = mode | (stats.st_mode & stat.S_ISGID)
                else:
                    # Ensure that group IDs of files are inheritied from dirs
                    mode = mode | stat.S_ISGID
            else:
                mode |= min_file_premissions

            if stats.st_mode & 0o7777 != mode:
                if not args.quiet:
                    print(
                        f"chmod {quote(filepath)} to {mode:03o} since mode is "
                        f"{stats.st_mode & 0o7777:03o}"
                    )

                if args.commit:
                    filepath.chmod(mode)

    if not args.commit:
        print("Run with --commit to apply changes")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
