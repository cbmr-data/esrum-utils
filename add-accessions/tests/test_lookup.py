from __future__ import annotations

import subprocess
import sys
from pathlib import Path

TEST_ROOT = Path(__file__).parent
DATABASE = TEST_ROOT / "unique.keys.sqlite3"


def execute(*, args: list[str], stdin: str | None) -> str:
    command = [
        sys.executable,
        "-W",
        "error",
        "-X",
        "dev",
        "add_dbsnp_ids.py",
        DATABASE,
        "/dev/stdin",
        *args,
    ]
    print("Running command %s", command)
    proc = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL if stdin is None else subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    stdout, stderr = proc.communicate(stdin)
    print(stderr)
    assert proc.returncode == 0
    return stdout


def lookup(*, args: list[str], stdin: str) -> str:
    return execute(args=["--action", "lookup", *args], stdin=stdin)


########################################################################################


def test_defaults() -> None:
    data_in = """CHROM POS NA REF ALT
1 10043 OK T A
1 10045 Missing C T
1 10051 OK A G
1 10063 OK A C
1 10800 BAD T A
1 10109 OK A T
2 10800 Unknown A C
"""
    data_out = """CHROM POS NA REF ALT rsID
1 10043 OK T A rs1008829651
1 10045 Missing C T NA
1 10051 OK A G rs1052373574
1 10063 OK A C rs1010989343
1 10800 BAD T A NA
1 10109 OK A T rs376007522
2 10800 Unknown A C NA
"""

    assert lookup(args=[], stdin=data_in) == data_out


def test_no_header_key_column_1() -> None:
    data_in = """1:10043:T:A OK bar
1:10045:C:T Missing bar
1:10051:A:G OK bar
1:10063:A:C OK bar
1:10800:T:A BAD bar
1:10109:A:T OK bar
2:10800:A:C Unknown Chr
"""
    data_out = """1:10043:T:A OK bar rs1008829651
1:10045:C:T Missing bar NA
1:10051:A:G OK bar rs1052373574
1:10063:A:C OK bar rs1010989343
1:10800:T:A BAD bar NA
1:10109:A:T OK bar rs376007522
2:10800:A:C Unknown Chr NA
"""

    assert lookup(args=["--no-header", "--key-column", "1"], stdin=data_in) == data_out


def test_no_header_key_column_named() -> None:
    data_in = """name key status other
blah 1:10043:T:A OK bar
blah 1:10045:C:T Missing bar
blah 1:10051:A:G OK bar
blah 1:10063:A:C OK bar
blah 1:10800:T:A BAD bar
blah 1:10109:A:T OK bar
blah 2:10800:A:C Unknown Chr
"""
    data_out = """name key status other rsID
blah 1:10043:T:A OK bar rs1008829651
blah 1:10045:C:T Missing bar NA
blah 1:10051:A:G OK bar rs1052373574
blah 1:10063:A:C OK bar rs1010989343
blah 1:10800:T:A BAD bar NA
blah 1:10109:A:T OK bar rs376007522
blah 2:10800:A:C Unknown Chr NA
"""

    assert lookup(args=["--key-column", "key"], stdin=data_in) == data_out


def test_no_header_key_column_2() -> None:
    data_in = """OK 1:10043:T:A bar
Missing 1:10045:C:T bar
OK 1:10051:A:G bar
OK 1:10063:A:C bar
BAD 1:10800:T:A bar
OK 1:10109:A:T bar
Unknown 2:10800:A:C Chr
"""
    data_out = """OK 1:10043:T:A bar rs1008829651
Missing 1:10045:C:T bar NA
OK 1:10051:A:G bar rs1052373574
OK 1:10063:A:C bar rs1010989343
BAD 1:10800:T:A bar NA
OK 1:10109:A:T bar rs376007522
Unknown 2:10800:A:C Chr NA
"""

    assert lookup(args=["--no-header", "--key-column", "2"], stdin=data_in) == data_out
