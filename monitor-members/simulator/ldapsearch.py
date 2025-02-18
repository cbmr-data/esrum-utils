#!/usr/bin/env python3
# pyright: strict
# ruff: noqa: S311
from __future__ import annotations

import json
import random
import re
import string
import sys
from pathlib import Path
from typing import NoReturn, TypedDict

_RE_CN = re.compile(r"^\(cn=(.*)\)", re.IGNORECASE)

_CACHE = Path(__file__).resolve().parent / "ldapcache.json"
_N_USERS = 200
_N_MEMBERS = 30
_CHANCE_REMOVE = 0.01
_CHANCE_ADD = 0.10

FIRST_NAMES = (
    "Amanda",
    "Amy",
    "Andrew",
    "Anthony",
    "Ashley",
    "Barbara",
    "Betty",
    "Brian",
    "Carol",
    "Charles",
    "Christopher",
    "Cynthia",
    "Daniel",
    "David",
    "Deborah",
    "Donald",
    "Donna",
    "Dorothy",
    "Edward",
    "Elizabeth",
    "Emily",
    "George",
    "Jacob",
    "James",
    "Jason",
    "Jeffrey",
    "Jennifer",
    "Jessica",
    "John",
    "Joseph",
    "Joshua",
    "Karen",
    "Kathleen",
    "Kenneth",
    "Kevin",
    "Kimberly",
    "Laura",
    "Linda",
    "Lisa",
    "Margaret",
    "Mark",
    "Mary",
    "Matthew",
    "Melissa",
    "Michael",
    "Michelle",
    "Nancy",
    "Nicholas",
    "Patricia",
    "Paul",
    "Rebecca",
    "Richard",
    "Robert",
    "Ronald",
    "Ryan",
    "Sandra",
    "Sarah",
    "Sharon",
    "Stephanie",
    "Steven",
    "Susan",
    "Thomas",
    "Timothy",
    "William",
)

LAST_NAMES = (
    "Adams",
    "Allen",
    "Anderson",
    "Baker",
    "Brown",
    "Campbell",
    "Carter",
    "Clark",
    "Collins",
    "Cruz",
    "Davis",
    "Diaz",
    "Edwards",
    "Evans",
    "Flores",
    "Garcia",
    "Gomez",
    "Gonzalez",
    "Green",
    "Hall",
    "Harris",
    "Hernandez",
    "Hill",
    "Jackson",
    "Johnson",
    "Jones",
    "King",
    "Lee",
    "Lewis",
    "Lopez",
    "Martin",
    "Martinez",
    "Miller",
    "Mitchell",
    "Moore",
    "Morales",
    "Morris",
    "Murphy",
    "Nelson",
    "Nguyen",
    "Parker",
    "Perez",
    "Phillips",
    "Ramirez",
    "Reyes",
    "Rivera",
    "Roberts",
    "Robinson",
    "Rodriguez",
    "Sanchez",
    "Scott",
    "Smith",
    "Stewart",
    "Taylor",
    "Thomas",
    "Thompson",
    "Torres",
    "Turner",
    "Walker",
    "White",
    "Williams",
    "Wilson",
    "Wright",
    "Young",
)


class Cache(TypedDict):
    # username to displayName
    users: dict[str, str]
    # group name to group members
    groups: dict[str, list[str]]


def abort(*args: object) -> NoReturn:
    print("ERROR:", *args, file=sys.stderr)
    sys.exit(1)


def new_username(rng: random.Random) -> str:
    username: list[str] = []
    username.extend(rng.choices(string.ascii_lowercase, k=3))
    username.extend(rng.choices(string.digits, k=3))
    return "".join(username)


def new_display_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def read_cache(path: Path, rng: random.Random) -> Cache:
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        cache: Cache = {"users": {}, "groups": {}}
        display_names: set[str] = set()

        for _ in range(_N_USERS):
            username = new_username(rng)
            while username in cache["users"]:
                username = new_username(rng)

            display_name = new_display_name(rng)
            while display_name in display_names:
                display_name = new_display_name(rng)

            display_names.add(display_name)
            cache["users"][username] = display_name

        return cache


def write_cache(path: Path, cache: Cache) -> None:
    path.write_text(json.dumps(cache), encoding="utf-8")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        abort(f"Usage: {__file__} [...] (cn=name) <member/displayName>")
    elif argv[-1] not in ("member", "displayName"):
        abort(f"Invalid argument {argv[-1]!r}; must be 'member' or 'displayname'")

    match = _RE_CN.match(argv[-2])
    if match is None:
        abort(f"Invalid argument {argv[-2]!r}; must match '(CN=key)'")

    rng = random.Random()
    cache = read_cache(_CACHE, rng=rng)

    (key,) = match.groups()

    if argv[-1] == "displayName":
        print("displayName:", cache["users"][key])
        return 0

    users = tuple(cache["users"])

    try:
        group = cache["groups"][key]
    except KeyError:
        cache["groups"][key] = []
        group = cache["groups"][key]

        group.extend(rng.sample(users, k=random.randint(0, _N_MEMBERS)))

    new_group = [user for user in group if rng.random() > _CHANCE_REMOVE]

    while rng.random() <= _CHANCE_ADD:
        choices = tuple(set(users) - set(group))
        if choices:
            new_group.append(rng.choice(choices))

    for member in sorted(new_group):
        print(f"member: CN={member},OU=Active,OU=KU Users,DC=unicph,DC=domain")

    cache["groups"][key] = new_group

    write_cache(_CACHE, cache)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
