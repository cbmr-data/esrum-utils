# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2026 Mikkel Schubert
from __future__ import annotations

import enum
import json
import logging
import subprocess
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import override

from koda_validate import DataclassValidator, Valid

from monitor_sinfo._utilities import abort

_BAD_STATES = {"down", "drain", "drng", "fail", "failg", "pow_dn", "unk"}


class ChangeType(enum.Enum):
    Added = "added"
    Removed = "removed"
    Trivial = "trivial"
    Available = "available"
    Unavailable = "unavailable"

    def __str__(self) -> str:
        return self.value


@dataclass
class Status:
    state: str
    reason: str | None
    is_responding: bool

    @classmethod
    def parse(cls, *, state: str, reason: str | None) -> Status:
        # Responding/not responding is not interesting for nodes in a bad state
        if state in _BAD_STATES:
            state = state.rstrip("*")

        return Status(
            state=state.removesuffix("*"),
            reason=(reason.strip() if reason else None) or None,
            is_responding=not state.endswith("*"),
        )

    @property
    def is_bad_state(self) -> bool:
        return self.state in _BAD_STATES

    @property
    def is_available(self) -> bool:
        return self.is_bad_state or not self.is_responding

    def __str__(self) -> str:
        if not self.is_responding:
            return f"{self.state} (not responding)"

        return self.state


@dataclass
class StatusChange:
    change: ChangeType
    new: Status
    old: Status | None


@dataclass
class StatusDB:
    timestamp: datetime
    nodes: dict[str, Status]


StatusDBValidator = DataclassValidator(StatusDB)


class KodaJSONEncoder(json.JSONEncoder):
    """Json encoder that generates Koda compatible JSON"""

    @override
    def default(self, o: object) -> object:
        if isinstance(o, datetime):
            return o.isoformat()
        elif is_dataclass(o) and not isinstance(o, type):
            return asdict(o)

        return super().default(o)


def collect_node_status(sinfo: str) -> dict[str, Status] | None:
    logger = logging.getLogger(__name__)
    logger.debug("Running sinfo")
    try:
        proc = subprocess.run(
            [sinfo, "--Node", "--format=%N|%t|%E"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            text=True,
        )
    except OSError as error:
        logger.error("Error collecting node states: %s", error)
        return None

    if proc.returncode:
        logger.error("sinfo exited with code %i: %s", proc.returncode, proc.stderr)
        return None

    lines = proc.stdout.splitlines()
    header = lines[0].split("|")

    result: dict[str, Status] = {}
    for line in lines[1:]:
        row = dict(zip(header, line.split("|")))
        reason: str | None = row["REASON"]
        if reason == "none":
            reason = None

        name = row["NODELIST"]
        state = row["STATE"]

        result[name] = Status.parse(state=state, reason=reason)

    return result


def diff_node_states(
    prev_states: dict[str, Status],
    curr_states: dict[str, Status],
) -> dict[str, StatusChange]:
    updates: dict[str, StatusChange] = {}
    for key, node in sorted(curr_states.items()):
        prev = prev_states.get(key)
        if prev is None:
            updates[key] = StatusChange(
                change=ChangeType.Added,
                new=node,
                old=None,
            )
            continue
        elif prev != node:
            if node.is_bad_state:
                change = ChangeType.Unavailable
            elif prev_states[key].is_bad_state:
                change = ChangeType.Available
            else:
                change = ChangeType.Trivial

            updates[key] = StatusChange(
                change=change,
                new=node,
                old=prev_states[key],
            )

    for key in set(prev_states) - set(curr_states):
        updates[key] = StatusChange(
            change=ChangeType.Removed,
            new=Status(state="unk", reason=None, is_responding=True),
            old=None,
        )

    return updates


def save_node_status(filepath: Path, nodes: dict[str, Status]) -> None:
    obj = StatusDB(timestamp=datetime.now(tz=timezone.utc), nodes=nodes)
    encoder = KodaJSONEncoder(indent=2)
    text = encoder.encode(obj)
    filepath.write_text(text)


def load_node_status(filepath: Path) -> dict[str, Status] | None:
    if not filepath.exists():
        logger = logging.getLogger(__name__)
        logger.warning("State file %s does not exist; assuming first run", filepath)
        return None

    text = filepath.read_text()
    db = StatusDBValidator(json.loads(text))
    if not isinstance(db, Valid):
        abort("Node state DB is not valid: %s", db.err_type)

    return db.val.nodes
