#!/usr/bin/env python3
from __future__ import annotations

import argparse
import functools
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, NoReturn


def humanize(n: float) -> str:
    if n < 1024:
        return str(n)
    elif n < 1024**2:
        n, ext = n / 1024, "K"
    elif n < 1024**3:
        n, ext = n / 1024**2, "M"
    elif n < 1024**4:
        n, ext = n / 1024**3, "G"
    else:
        n, ext = n / 1024**4, "T"

    return f"{n:.1f}{ext}"


@dataclass
class FileStats:
    n: int = 0
    size_before: int = 0
    size_after: int = 0


def eprint(*values: object) -> None:
    print(*values, file=sys.stderr)


def error(*values: object) -> None:
    eprint("ERROR:", *values)


def abort(*values: object) -> NoReturn:
    error(*values)
    sys.exit(1)


class Args(NamedTuple):
    states: list[Path]
    human: bool


def parse_args(argv: list[str]) -> Args:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument("states", nargs="+", type=Path)
    parser.add_argument("--human", default="False", action="store_true")

    return Args(**vars(parser.parse_args(argv)))


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    processed: dict[str, FileStats] = defaultdict(FileStats)
    for filepath in args.states:
        with filepath.open(encoding="utf-8") as handle:
            for linenum, line in enumerate(handle, start=1):
                row = line.rstrip("\n").split("\t", 3)
                if len(row) != 4:
                    abort(f"Wrong column number on line {linenum}: {line!r}")

                state, size_before, size_after, _ = row

                stats = processed[state]
                stats.n += 1
                stats.size_before += int(size_before)
                stats.size_after += int(size_after)

    to_size = humanize if args.human else str
    if sys.stdout.isatty():
        tmpl = "{:<14}  {:>10}  {:>16}  {:>16}  {}".format

        print(tmpl("State", "Files", "SizeBefore", "SizeAfter", "Ratio"))
        for state, stats in processed.items():
            ratio = stats.size_after / stats.size_before if stats.size_before else 1.0
            ratio_s = f"{ratio:.2f}"
            print(
                tmpl(
                    state,
                    stats.n,
                    to_size(stats.size_before),
                    to_size(stats.size_after),
                    ratio_s,
                )
            )
    else:
        print("State\tFiles\tSizeBefore\tSizeAfter\tRatio")
        for state, stats in processed.items():
            ratio = stats.size_after / stats.size_before if stats.size_before else 1.0
            print(
                state,
                stats.n,
                to_size(stats.size_before),
                to_size(stats.size_after),
                f"{ratio:.2f}",
                sep="\t",
            )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
