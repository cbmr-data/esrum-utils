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

__VERSION__ = 2023_12_06_1

KEY_SPLIT = re.compile(r"[_:]")


T = TypeVar("T")


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def abort(*args: object) -> NoReturn:
    eprint(*args)
    sys.exit(1)


def progress(values: Iterable[T], *, desc: str | None = None) -> Iterable[T]:
    try:
        import tqdm
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
            chrom, position, ref, alts = KEY_SPLIT.split(position)
            position = int(position)

            assert "," not in ref, repr(line)

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


def get_primary_key_lookup(
    header: list[str] | None,
    key_column: str | None,
) -> Callable[[list[str]], tuple[str, int, str, str]]:
    keys = {key.upper(): idx for idx, key in enumerate(header or ())}

    if key_column is None:
        assert header is not None

        columns: list[int] = []
        for key in ["CHROM", "POS", "REF", "ALT"]:
            column = keys.get(key)
            if column is None:
                abort(f"Column {key} not found in table")
            columns.append(column)

        chrom, pos, ref, alt = columns

        def _get_primary_key(row: list[str]) -> tuple[str, int, str, str]:
            return (row[chrom], int(row[pos]), row[ref], row[alt])

        return _get_primary_key
    else:
        column = keys.get(key_column.upper())
        if column is None:
            if key_column.isdigit():
                column = int(key_column) - 1
            else:
                abort(f"Unknown key column {key_column!r}")

        splitter = KEY_SPLIT

        def _get_primary_key(row: list[str]) -> tuple[str, int, str, str]:
            values = splitter.split(row[column])
            if len(values) != 4:
                abort("Malformed key; expected 4 values, but found", repr(row[column]))

            chrom, pos, ref, alt = splitter.split(row[column])
            return (chrom, int(pos), ref, alt)

        return _get_primary_key


def main_lookup(
    *,
    database: Path,
    source: Path,
    key_column: str | None,
    missing_value: str,
    no_header: bool,
) -> int:
    con = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    cur = con.cursor()
    # Avoid locking/unlocking between queries (2x speedup)
    cur.execute("PRAGMA LOCKING_MODE=EXCLUSIVE;")

    with source.open() as handle:
        if no_header:
            get_primary_key = get_primary_key_lookup(None, key_column=key_column)
        else:
            header = handle.readline()
            print(header.rstrip("\r\n"), "rsID")
            header = header.split()
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

            query = cur.execute(
                "SELECT"
                " identifiers "
                "FROM"
                " data "
                "WHERE"
                " chr = ? AND pos = ? AND ref = ? AND alt = ?;",
                (chrom, int(position), ref, alt),
            )

            identifers = ",".join(key for (key,) in query)
            if identifers:
                records_found += 1
            else:
                identifers = missing_value
                records_missing += 1

            print(line.rstrip("\r\n"), identifers)

    records_total = records_found + records_missing
    eprint(
        "Found IDs for {} of {} records ({:.1f}%), {} not found".format(
            records_found,
            records_total,
            int(1000 * records_found / records_total) / 10,  # rounded down
            records_missing,
        )
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
        choices=("index", "lookup"),
        default="lookup",
        help="Either create a new index file or look up positions and add IDs to a "
        "whitespace separaed table.",
    )
    parser.add_argument(
        "--key-column",
        help="Column number (1-based) or name containing allele keys in the form "
        "chr:pos:alt:ref. If not set, the script will look for columns 'CHROM', 'POS', "
        "'REF', and 'ALT' (case-insensitive). For lookup only",
    )
    parser.add_argument(
        "--missing-value",
        default="NA",
        help="Value used when no IDs were found",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="If set, the columns are assumed to not have names. "
        "--key-column must be set to a number",
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
        )
    except BrokenPipeError as error:
        abort("ERROR:", error)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
