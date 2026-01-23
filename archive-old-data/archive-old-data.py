#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime
import functools
import grp
import os
import pwd
import shlex
import shutil
import stat
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, NamedTuple, NoReturn, TypeAlias

FileStates: TypeAlias = Literal[
    "compressed",
    "filetype",
    "hardlinks",
    "not_found",
    "target_exists",
    "uncompressible",
]


@dataclass
class FileStats:
    n: int = 0
    size_before: int = 0
    size_after: int = 0


def eprint(*values: object) -> None:
    print(*values, file=sys.stderr)


def error(*values: object) -> None:
    eprint("ERROR:", *values)


def warning(*values: object) -> None:
    eprint("WARNING:", *values)


def abort(*values: object) -> NoReturn:
    error(*values)
    sys.exit(1)


def quote_path(value: Path) -> str:
    return shlex.quote(str(value))


def cpu_count() -> int:
    if hasattr(os, "sched_getaffinity"):
        cpus = len(os.sched_getaffinity(0))
    else:
        cpus = os.cpu_count()

    return max(1, cpus or 1)


def read_file_lists(filepaths: list[Path]) -> Iterable[Path]:
    for filepath in filepaths:
        with filepath.open(encoding="utf-8", newline="\n") as handle:
            for line in handle:
                if (line := line.strip()) and not line.startswith("#"):
                    *_, filename = line.rsplit("\t", 1)

                    yield Path(filename)


def read_file_states(filepath: Path) -> set[Path]:
    paths: set[Path] = set()
    try:
        with filepath.open(encoding="utf-8", newline="\n") as handle:
            for linenum, line in enumerate(handle, start=1):
                row = line.rstrip("\n").split("\t", 3)
                if len(row) != 4:
                    abort(f"Wrong column number on line {linenum}: {line!r}")

                state, _, _, filename = row
                if state not in (
                    "compressed",
                    "filetype",
                    "hardlinks",
                    "not_found",
                    "target_exists",
                    "uncompressible",
                ):
                    abort(f"Invalid state {state!r} on line {linenum}: {line!r}")

                paths.add(Path(filename))
    except FileNotFoundError:
        pass

    return paths


def format_name(func: Callable[[int], str], key: int) -> str:
    try:
        name = func(key)
    except KeyError:
        name = str(key)

    return f"{key}/{name}"


@functools.cache
def user_name(uid: int) -> str:
    return format_name(lambda key: pwd.getpwuid(key).pw_name, uid)


@functools.cache
def group_name(gid: int) -> str:
    return format_name(lambda key: grp.getgrgid(key).gr_name, gid)


def timestamp(value: float) -> str:
    # Get local timezone
    tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    ts = datetime.datetime.fromtimestamp(value, tz=tz)

    return ts.isoformat(timespec="microseconds")


def stats_to_text(filepath: Path, stats: os.stat_result) -> str:
    lines: list[str] = [
        f"filename={filepath}",
        f"uid={user_name(stats.st_uid)}",
        f"gid={group_name(stats.st_gid)}",
        f"mode={stats.st_mode:o}",
        f"size={stats.st_size}",
        f"atime={timestamp(stats.st_atime)}",
        f"mtime={timestamp(stats.st_mtime)}",
        f"ctime={timestamp(stats.st_ctime)}",
        "",  # trailing newline
    ]

    return "\n".join(lines)


def process_file(
    source: Path,
    pigz: str,
    threads: int,
    compression_ratio: float,
    nth: int,
    of_n: int,
) -> tuple[FileStates, int, int]:
    # 1. Verify that the file is a candidate for compression
    try:
        stats = source.lstat()
    except FileNotFoundError:
        warning("file not found:", quote_path(source))
        return ("not_found", 0, 0)

    if not stat.S_ISREG(stats.st_mode):
        warning("skipping non-file path:", quote_path(source))
        return ("filetype", stats.st_size, stats.st_size)
    # It's hard to predict if compressing hardlinked files will be beneficial
    elif stats.st_nlink > 1:
        warning("skipping file with > 1 hardlinks:", quote_path(source))
        return ("hardlinks", stats.st_size, stats.st_size)

    # 2. Verify that the file has not been (partially) processed before
    target = source.parent / f"{source.name}.gz"
    target_txt = source.parent / f"{source.name}.archived_by_dap.txt"
    if os.path.lexists(target) or os.path.lexists(target_txt):
        warning("skipping; target already exists:", quote_path(target))
        return ("target_exists", stats.st_size, stats.st_size)

    eprint(f"[{nth:,}/{of_n:,}] Compressing", quote_path(source))
    # 3. Create stats file; should not fail, but do it early in case it does; this will
    #    also prevent new attempts on this data, until the stats file has been removed
    stats_txt = stats_to_text(source, stats)
    with target_txt.open("xt", encoding="utf-8") as handle_out:
        handle_out.write(stats_txt)

    # 4. Compress to temporary file
    temp_gz = source.parent / f"{source.name}.archived_by_dap.tmp"
    with temp_gz.open("xb") as handle_out:
        cmd = subprocess.run(
            [pigz, "--processes", str(threads), "--to-stdout", source],
            stdout=handle_out,
            cwd=source.parent,
            check=False,
        )

    if cmd.returncode:
        abort(f"pigz failed with return-code {cmd.returncode}")

    # 5. Check if the compression gains were worthwhile
    stats_gz = temp_gz.stat()
    ratio = max(stats_gz.st_size, 1) / max(stats.st_size, 1)
    if ratio > compression_ratio:
        temp_gz.unlink()
        target_txt.unlink()
        eprint(f"    -> skipped; only compressed to {ratio * 100:.1f}%")
        return ("uncompressible", stats.st_size, stats_gz.st_size)

    # 6. Ensure that processed files are in place before unlinking source
    if os.path.lexists(target):
        error("target race condition; skipping: ", quote_path(target))
        return ("target_exists", stats.st_size, stats.st_size)

    eprint(f"    -> compressed to {ratio * 100:.1f}%")
    temp_gz.rename(target)
    source.unlink()

    return ("compressed", stats.st_size, stats_gz.st_size)


class Args(NamedTuple):
    filelist: list[Path]
    state: Path
    compression_ratio: float
    threads: int


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument("filelist", nargs="+", type=Path)
    parser.add_argument(
        "--state",
        required=True,
        type=Path,
        help="Location of log file listing files already processed or skipped",
    )
    parser.add_argument(
        "--compression-ratio",
        type=float,
        default=0.9,
        help="Compression ratio must be no more than this value, calculated as "
        "compressed_size / original_size. If the size is larger, the file is skipped",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=min(32, cpu_count()),
        help="Number of threads used for gzip compression",
    )

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    pigz_exec = shutil.which("pigz")
    if pigz_exec is None:
        sys.exit("ERROR: `pigz` not found on PATH")

    paths = read_file_states(args.state)

    processed: dict[FileStates, FileStats] = defaultdict(FileStats)

    with args.state.open("at", encoding="utf-8", newline="\n") as handle:
        files = sorted(read_file_lists(args.filelist))
        for nth, filepath in enumerate(files, start=1):
            if filepath in paths:
                continue

            if results := process_file(
                filepath,
                pigz=pigz_exec,
                threads=args.threads,
                compression_ratio=args.compression_ratio,
                nth=nth,
                of_n=len(files),
            ):
                state, old_size, new_size = results

                proc = processed[state]
                proc.n += 1
                proc.size_before += old_size
                proc.size_after += new_size

                print(state, old_size, new_size, filepath, sep="\t", file=handle)
                handle.flush()

    if sys.stdout.isatty():
        tmpl = "{:<14}  {:>10}  {:>16}  {:>16}  {}".format

        print(tmpl("State", "Files", "SizeBefore", "SizeAfter", "Ratio"))
        for state, stats in processed.items():
            ratio = stats.size_after / stats.size_before if stats.size_before else 1.0
            ratio_s = f"{ratio:.2f}"
            print(tmpl(state, stats.n, stats.size_before, stats.size_after, ratio_s))
    else:
        print("State\tFiles\tSizeBefore\tSizeAfter\tRatio")
        for state, stats in processed.items():
            ratio = stats.size_after / stats.size_before if stats.size_before else 1.0
            print(
                state,
                stats.n,
                stats.size_before,
                stats.size_after,
                f"{ratio:.2f}",
                sep="\t",
            )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
