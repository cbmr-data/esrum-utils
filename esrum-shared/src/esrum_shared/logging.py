from logging import Logger
from typing import Literal, TypeAlias

import colorlog

LogLevels: TypeAlias = Literal[
    "NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
]

DEFAULT_FMT = (
    "%(cyan)s%(asctime)s %(log_color)s%(levelname)-8s%(reset)s "
    "%(cyan)s%(name)s%(reset)s  %(message)s"
)


def install(
    log_level: LogLevels,
    *,
    fmt: str = DEFAULT_FMT,
    millisecond: bool = False,
) -> Logger:
    datefmt = None if millisecond else "%Y-%m-%d %H:%M:%S"
    formatter = colorlog.ColoredFormatter(
        datefmt=datefmt,
        fmt=fmt,
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(formatter)

    logger = colorlog.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(handler)

    return logger
