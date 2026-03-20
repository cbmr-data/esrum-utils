#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "isal",
#     "tqdm",
# ]
# [tool.uv]
# exclude-newer = "2025-11-18T00:00:00Z"
# ///
from __future__ import annotations

import argparse
import functools
import hashlib
import sys
import zlib
from pathlib import Path
from typing import NamedTuple, NoReturn

import isal.igzip
import tqdm


def eprint(*values: object) -> None:
    print(*values, file=sys.stderr)


def error(*values: object) -> None:
    eprint("ERROR:", *values)


def warning(*values: object) -> None:
    eprint("WARNING:", *values)


def abort(*values: object) -> NoReturn:
    error(*values)
    sys.exit(1)


def collect_candidate_files(filepath: Path) -> list[Path]:
    paths: set[Path] = set()
    try:
        with filepath.open(encoding="utf-8", newline="\n") as handle:
            for linenum, line in enumerate(handle, start=1):
                row = line.rstrip("\n").split("\t", 3)
                if len(row) != 4:
                    abort(f"Wrong column number on line {linenum}: {line!r}")

                state, _, _, filename = row
                if state == "target_exists":
                    paths.add(Path(filename))
    except FileNotFoundError:
        pass

    return sorted(paths)


def checksum_file(source: Path, *, decompress: bool, blocksize: int = 4 * 1024) -> str:
    checksum = hashlib.sha384()
    fopen = isal.igzip.open if decompress else open
    with (
        tqdm.tqdm(desc=str(source), unit_scale=True) as progress,
        fopen(source, "rb") as handle,
    ):
        while block := handle.read(blocksize):
            progress.update(len(block))
            checksum.update(block)

    return checksum.hexdigest()


class Args(NamedTuple):
    state: Path


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "state",
        type=Path,
        help="Location of log file listing files already processed or skipped",
    )

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    for src in collect_candidate_files(args.state):
        dst = src.parent / f"{src.name}.gz"

        if not (src.exists() and dst.exists()):
            eprint("SKIP FILE-NOT-FOUND", src)
            continue

        try:
            src_hash = checksum_file(src, decompress=False)
            dst_hash = checksum_file(dst, decompress=True)
        except (OSError, EOFError, zlib.error) as err:
            error("Checksumming failed due to", err)
            continue

        print("MATCH" if (src_hash == dst_hash) else "MISMATCH", src)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
