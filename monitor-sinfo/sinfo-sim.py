#!/usr/bin/env python3
# ignore use of insecure RNG; only used for simulating sinfo states
# ruff: noqa: S311
from __future__ import annotations

import argparse
import functools
import json
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, cast

_GOOD_STATES = (
    "alloc",
    "comp",
    "futr",
    "idle",
    "maint",
    "mix",
    "npc",
    "pow_up",
    "resv",
)
_BAD_STATES = ("down", "drain", "drng", "fail", "failg", "pow_dn", "unk")


@dataclass
class Node:
    name: str
    state: str
    reason: str

    def to_json(self) -> dict[str, str]:
        return {"name": self.name, "state": self.state, "reason": self.reason}

    @classmethod
    def from_json(cls, data: object) -> Node:
        if not isinstance(data, dict):
            abort(f"invalid node object: {data!r}")

        data = cast("dict[object, object]", data)
        for key, value in data.items():
            if not (key and value and isinstance(key, str) and isinstance(value, str)):
                abort(f"invalid node object: {data!r}")

        data = cast("dict[str, str]", data)
        try:
            return Node(name=data["name"], state=data["state"], reason=data["reason"])
        except KeyError:
            abort(f"invalid node object: {data!r}")


def eprint(msg: str, *args: object) -> None:
    print(msg, *args, file=sys.stderr)


def abort(msg: str, *args: object) -> NoReturn:
    eprint(msg, *args)
    sys.exit(1)


def random_state(state: str) -> str:
    value = random.random()

    if state.endswith("*"):
        if random.random() < 0.25:
            state = state.removesuffix("*")
        return state
    elif value < 0.8:
        if random.random() < 0.1:
            state = f"{state}*"

        return state
    elif random.random() < 0.96:
        return random.choice(_GOOD_STATES)
    else:
        return random.choice(_BAD_STATES)


def random_reason() -> str:
    if random.random() < 0.5:
        return str(random.random())

    return "none"


def initialize(filepath: Path, nnodes: int) -> int:
    samples: list[Node] = []
    for nth in range(1, nnodes + 1):
        state = random_state("idle")
        reason = "none" if state.startswith("idle") else random_reason()

        samples.append(Node(name=f"esrumcmpn{nth}fl", state=state, reason=reason))

    write_sim(filepath, samples)

    return 0


def write_sim(filepath: Path, data: list[Node]) -> None:
    with filepath.open("w") as handle:
        json.dump([it.to_json() for it in data], handle, indent=2)


def read_sim(filepath: Path) -> list[Node]:
    with filepath.open("r") as handle:
        data = json.load(handle)

    if not isinstance(data, list):
        abort(f"invalid sim data {data!r}")

    return [Node.from_json(it) for it in cast("list[object]", data)]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        )
    )

    parser.add_argument(
        "--init",
        metavar="N",
        type=int,
        help="Initialize --sim-file with N nodes with randomly selected states",
    )
    parser.add_argument(
        "--Node",
        action="store_true",
        help="Option used by monitor-sinfo.py; is expected when not initializing.",
    )
    parser.add_argument(
        "--format",
        help="Option used by monitor-sinfo.py; is expected when not initializing.",
    )
    parser.add_argument(
        "--sim-file",
        type=Path,
        default=Path("sim.json"),
        help="Location of JSON file containing node states between invocations",
    )

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.init is not None:
        return initialize(filepath=args.sim_file, nnodes=args.init)

    if not args.Node:
        abort("--Node not specified on command-line")
    elif args.format != "%N|%t|%E":
        abort("--format does not match expectations")
    elif not args.sim_file.is_file():
        abort("--sim-file not initialized")

    nodes = read_sim(args.sim_file)

    print("NODELIST|STATE|REASON")
    for node in nodes:
        new_state = random_state(node.state)
        if new_state != node.state:
            node.state = new_state
            node.reason = "none" if node.state.startswith("idle") else random_reason()

        print(node.name, node.state, node.reason, sep="|")

    write_sim(filepath=args.sim_file, data=nodes)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
