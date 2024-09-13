#!/usr/bin/env python3
import argparse
import grp
import os
import pwd
import shlex
import stat
import sys
from pathlib import Path


def tqdm(seq):
    if sys.stderr.isatty():
        try:
            import tqdm

            return tqdm.tqdm(seq, unit=" entry", unit_scale=True)
        except ImportError:
            pass

    return seq


def warning(*args, file=sys.stderr, **kwargs):
    print("WARNING:", *args, file=file, **kwargs)


def abort(*args, file=sys.stderr, **kwargs):
    print("ERROR:", *args, file=file, **kwargs)
    sys.exit(1)


def quote(value):
    return shlex.quote(str(value))


def get_group_id(name: str) -> int:
    try:
        groupinfo = grp.getgrnam(name)
    except KeyError:
        abort(f"Group with name {name} not found!")

    return groupinfo.gr_gid


def get_group_name(id: int) -> str:
    try:
        return grp.getgrgid(id).gr_name
    except KeyError:
        return str(id)


def get_user_name(id: int) -> str:
    try:
        return pwd.getpwuid(id).pw_name
    except KeyError:
        return str(id)


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("width", 79)

        super().__init__(*args, **kwargs)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        formatter_class=HelpFormatter,
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


def walk(*roots):
    queue = list(roots)
    while queue:
        root = queue.pop()
        if isinstance(root, os.DirEntry):
            yield root.path, root, root.stat(follow_symlinks=False)
        else:
            yield root, Path(root), os.lstat(root)

        for it in os.scandir(root):
            if it.is_dir(follow_symlinks=False):
                yield from walk(it)
            else:
                yield it.path, it, it.stat(follow_symlinks=False)


def main(argv):
    args = parse_args(argv)

    gid = get_group_id(args.group)
    uid = os.getuid()

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
                if args.no_group_bit:
                    # Inherit S_ISGID
                    mode = mode | (stats.st_mode & stat.S_ISGID)
                else:
                    # Ensure that group IDs of files are inheritied from dirs
                    mode = mode | stat.S_ISGID

            if stats.st_mode & 0o7777 != mode:
                if not args.quiet:
                    print(
                        "chmod {} to {} since mode is {}".format(
                            quote(filepath),
                            oct(mode),
                            oct(stats.st_mode & 0o7777),
                        )
                    )

                if args.commit:
                    os.chmod(filepath, mode)

    if not args.commit:
        print("Run with --commit to apply changes")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
