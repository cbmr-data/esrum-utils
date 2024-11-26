import logging

import coloredlogs


def setup_logging(*, log_level: str, log_sql: bool = False) -> None:
    coloredlogs.install(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        level=log_level,
        milliseconds=True,
    )

    if log_sql:
        # Echo SQL alchemy commands to log
        logging.getLogger("sqlalchemy.engine").setLevel(log_level)
