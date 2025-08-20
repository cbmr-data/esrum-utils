#!/usr/bin/env python3.9
# pyright: strict
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Callable, NoReturn, TypeVar

__VERSION__ = 2025_01_21_1


RE_SPLIT: re.Pattern[str] = re.compile(r"[_:]")


def split_key(value: str, *, maxsplit: int = 0) -> list[str]:
    """
    Split a chr:pos or chr:pos:ref:alt key. Reverse split is used to avoid splitting
    contig names that contain underscores, such as alt or random sequences.
    """
    return [it[::-1] for it in reversed(RE_SPLIT.split(value[::-1], maxsplit=maxsplit))]


T = TypeVar("T")


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def abort(*args: object) -> NoReturn:
    eprint(*args)
    sys.exit(1)


def progress(values: Iterable[T], *, desc: str | None = None) -> Iterable[T]:
    try:
        import tqdm  # pyright: ignore[reportMissingModuleSource]  # noqa: PLC0415
    except ImportError:
        return values
    else:
        return tqdm.tqdm(values, unit_scale=True, desc=desc)


def read_identifiers(filepath: Path) -> Iterator[tuple[str, int, str, str, str]]:
    with filepath.open() as handle:
        current_chr: str = ""
        current_pos: int = -1
        current_alleles: list[tuple[str, str, str]] = []

        def _yield_alleles() -> Iterator[tuple[str, int, str, str, str]]:
            if current_alleles:
                current_alleles.sort()
                iterator = iter(current_alleles)

                last_ref, last_alt, last_value = next(iterator)
                for ref, alt, value in iterator:
                    if ref != last_ref or alt != last_alt:
                        yield current_chr, current_pos, last_ref, last_alt, last_value
                        last_ref, last_alt, last_value = ref, alt, value
                    else:
                        last_value = f"{last_value},{value}"

                yield current_chr, current_pos, last_ref, last_alt, last_value

        for line in progress(handle, desc="load "):
            position, value = line.split()
            chrom, position, refs, alts = split_key(position, maxsplit=3)
            position = int(position)

            # Multiple references per line are not expected, but handled just in case
            for ref in refs.split(","):
                if position != current_pos or chrom != current_chr:
                    yield from _yield_alleles()

                    current_alleles.clear()
                    current_chr = chrom
                    current_pos = position

                for alt in alts.split(","):
                    current_alleles.append((ref, alt, value))

        yield from _yield_alleles()


def main_index(database: Path, source: Path) -> int:
    con = sqlite3.connect(os.fspath(database))
    cur = con.cursor()

    cur.execute("PRAGMA TEMP_STORE=MEMORY;")
    cur.execute("PRAGMA JOURNAL_MODE=OFF;")
    cur.execute("PRAGMA SYNCHRONOUS=OFF;")
    cur.execute("PRAGMA LOCKING_MODE=EXCLUSIVE;")

    cur.execute("DROP TABLE IF EXISTS data;")
    cur.execute(
        "CREATE TABLE data("
        "  chr"
        ", pos INTEGER"
        ", ref"
        ", alt"
        ", identifiers"
        ", PRIMARY KEY (chr, pos, ref, alt)"
        ") WITHOUT ROWID;"
    )

    cur.executemany("INSERT INTO data VALUES(?, ?, ?, ?, ?);", read_identifiers(source))

    cur.execute("COMMIT;")
    cur.execute("ANALYZE;")

    return 0


def get_column_indices(keys: dict[str, int], names: list[str]) -> list[int] | None:
    column_indices: list[int] = []
    for name in names:
        indice = keys.get(name.upper())
        if indice is None:
            return None

        column_indices.append(indice)

    return column_indices


def get_combined_key_function(
    keys: dict[str, int],
    key_column: str,
) -> Callable[[list[str]], tuple[str, int, str, str]]:
    column = keys.get(key_column.upper())
    if column is None:
        if key_column.isdigit():
            column = int(key_column) - 1
        else:
            abort(f"Unknown key column {key_column!r}")

    def _get_primary_key(row: list[str]) -> tuple[str, int, str, str]:
        values = split_key(row[column], maxsplit=3)
        if len(values) != 4:
            abort("Malformed key; expected 4 values, but found", repr(row[column]))

        chrom, pos, ref, alt = values
        return (chrom, int(pos), ref, alt)

    return _get_primary_key


def get_primary_key_lookup(
    header: list[str] | None,
    key_column: str | None,
) -> Callable[[list[str]], tuple[str, int, str, str]]:
    keys = {key.upper(): idx for idx, key in enumerate(header or ())}

    if key_column is not None:
        return get_combined_key_function(keys=keys, key_column=key_column)
    elif header is None:
        raise RuntimeError("Header is required if key-column is unspecified")

    ####################################################################################
    # Style 1: Individual columns for each value
    indices = get_column_indices(keys=keys, names=["CHROM", "POS", "REF", "ALT"])
    if indices is not None:
        chrom, pos, ref, alt = indices

        def _get_primary_key(row: list[str]) -> tuple[str, int, str, str]:
            return (row[chrom], int(row[pos]), row[ref], row[alt])

        return _get_primary_key

    ####################################################################################
    # Style 2: Combined chr/pos column and individual alt/ref columns
    indices = get_column_indices(keys=keys, names=["MarkerName", "Allele1", "Allele2"])
    if indices is not None:
        chr_pos, ref, alt = indices

        def _get_primary_key(row: list[str]) -> tuple[str, int, str, str]:
            values = split_key(row[chr_pos], maxsplit=3)
            if len(values) != 2:
                abort("Malformed key; expected 4 values, but found", repr(row[chr_pos]))

            chrom, pos = values
            return (chrom, int(pos), row[ref], row[alt])

        return _get_primary_key

    abort(
        "Table schema could not be determined. See --help text for --key-column for "
        "more information about how this script looks up information"
    )


def main_lookup(
    *,
    database: Path,
    source: Path,
    key_column: str | None,
    missing_value: str,
    no_header: bool,
    unordered_alleles: bool,
) -> int:
    con = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    cur = con.cursor()
    # Avoid locking/unlocking between queries (2x speedup)
    cur.execute("PRAGMA LOCKING_MODE=EXCLUSIVE;")

    with source.open() as handle:
        header: list[str] | None = None
        if not no_header:
            header = handle.readline().rstrip("\r\n").split()
            print(*header, "rsID")

        get_primary_key = get_primary_key_lookup(header, key_column=key_column)

        records_found = 0
        records_missing = 0
        for line in progress(handle, desc="query "):
            try:
                chrom, position, ref, alt = get_primary_key(line.split())
            except IndexError:
                abort("Malformed key; insufficient number of columns:\n", line)
            except ValueError:
                abort("Malformed key; position is not a number:\n", line)

            ref, alt = ref.upper(), alt.upper()

            if unordered_alleles:
                query = cur.execute(
                    "SELECT identifiers "
                    "FROM data "
                    "WHERE chr = ? AND pos = ? "
                    "  AND ((ref = ? AND alt = ?) OR (alt = ? AND ref = ?));",
                    (chrom, int(position), ref, alt, ref, alt),
                )
            else:
                query = cur.execute(
                    "SELECT identifiers "
                    "FROM data "
                    "WHERE chr = ? AND pos = ? AND ref = ? AND alt = ?;",
                    (chrom, int(position), ref, alt),
                )

            identifiers = ",".join(sorted({key for (key,) in query}))
            if identifiers:
                records_found += 1
            else:
                identifiers = missing_value
                records_missing += 1

            print(line.rstrip("\r\n"), identifiers)

    records_total = records_found + records_missing
    records_found_pct = int(1000 * records_found / records_total) / 10  # rounded down
    eprint(
        f"Found IDs for {records_found} of {records_total} records "
        f"({records_found_pct:.1f}%), {records_missing} not found"
    )

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
    )
    parser.add_argument(
        "database",
        type=Path,
        help="Path to SQLite3 database",
    )
    parser.add_argument(
        "source",
        type=Path,
        help="Input file",
    )
    parser.add_argument(
        "--action",
        metavar="X",
        choices=("index", "lookup"),
        default="lookup",
        help="Either create a new index file or look up positions and add IDs to a "
        "whitespace separated table",
    )
    parser.add_argument(
        "--key-column",
        help="Column number (1-based) or name containing allele keys in the form "
        "chr:pos:alt:ref (may use '_' as the separator). If not set, the script will "
        "look for columns 'CHROM', 'POS', 'REF', and 'ALT', or columns 'MarkerName' "
        "(containing chr:pos), 'Allele1', 'Allele2'. Column names are "
        "case-insensitive.  For lookup only",
    )
    parser.add_argument(
        "--missing-value",
        metavar="X",
        default="NA",
        help="Value used when no IDs were found. For lookup only",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="If set, the columns are assumed to not have names. --key-column must be "
        "set to a number. For lookup only",
    )
    parser.add_argument(
        "--unordered-alleles",
        action="store_true",
        help="When enabled, this script will look up IDs for alleles chrom:pos:A:B and "
        " chrom:pos:B:A, i.e. making no assumption about which allele is the reference "
        "allele and which is the alternative allele.",
    )

    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    action: str = args.action
    database: Path = args.database
    source: Path = args.source
    key_column: str | None = args.key_column
    missing_value: str = args.missing_value
    no_header: bool = args.no_header
    unordered_alleles: bool = args.unordered_alleles

    if action == "index":
        return main_index(database=database, source=source)

    if no_header and (key_column is None or not key_column.isdigit()):
        abort("ERROR: --no-header requires --key-column with a column number")

    try:
        return main_lookup(
            database=database,
            source=source,
            key_column=key_column,
            missing_value=missing_value,
            no_header=no_header,
            unordered_alleles=unordered_alleles,
        )
    except BrokenPipeError as error:
        abort("ERROR:", error)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
