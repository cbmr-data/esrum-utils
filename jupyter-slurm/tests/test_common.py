import sys


def test_dev_mode() -> None:
    assert sys.flags.dev_mode
