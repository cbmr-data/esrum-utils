#!/usr/bin/env python3
# pyright: strict
from __future__ import annotations

import argparse
import base64
import functools
import json
import random
import re
import string
import sys
from pathlib import Path
from typing import NoReturn, TypedDict, cast

_RE_CN = re.compile(r"\(cn=([^)]*)\)", re.IGNORECASE)
_RE_KUID = re.compile(r"[a-z]{3}\d{3}(-ucph)?", re.IGNORECASE)

_CACHE = Path(__file__).resolve().parent / "ldapcache.json"
_N_USERS = 200
_N_MEMBERS = 30
_CHANCE_USER_REMOVE = 0.01
_CHANCE_USER_ADD = 0.10
_CHANCE_USER_MODIFY = 0.001

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

DEPARTMENTS = (
    "Indefinite Studies",
    "Recent Runes",
    "Inadvisably Applied Magic",
    "Cruel and Unusual Geography",
    "Post Morthem Communications",
    "Applied Astrology",
    "Applied Anthropics",
    "Approximate Accuracy",
    "Extreme Horticulture",
    "Liberal Studies",
    "Illiberal Studies",
    "Morbid Bibliomancy",
    "Recondite Architecture and Origami Map Folding",
    "Recondite Phenomena",
    "Slood Dynamics",
    "Esoteric Studies",
    "Woolly Thinking",
)

TITLES = (
    "Auxiliary academician",
    "Barber surgeon",
    "Bear-leader",
    "Biritch",
    "Chancellor",
    "Chandler",
    "City physician",
    "Clockkeeper",
    "Coarb",
    "Edict attendant",
    "Erenagh",
    "Gaberlunzie",
    "Hayward",
    "Hersir",
    "High-reeve",
    "Hofmeister",
    "Jitō",
    "Man-at-arms",
    "Minstrel",
    "Naperer",
    "Road master",
    "Scop",
    "Skomorokh",
    "Troubadour",
    "Villein",
)


class User(TypedDict):
    display_name: str
    department: str
    division: str
    title: str


class Cache(TypedDict):
    # username to displayName
    users: dict[str, User]
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


def new_department(rng: random.Random) -> str:
    return f"Department of {rng.choice(DEPARTMENTS)}"


def new_division(rng: random.Random) -> str:
    return f"{rng.choice(LAST_NAMES)} Group"


def new_title(rng: random.Random) -> str:
    return rng.choice(TITLES)


def new_display_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def read_cache(path: Path, rng: random.Random) -> Cache:
    try:
        with path.open(encoding="utf-8") as handle:
            # FIXME: Validate input
            return cast("Cache", json.load(handle))
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
            cache["users"][username] = User(
                display_name=display_name,
                department=new_department(rng),
                division=new_division(rng),
                title=new_title(rng),
            )

        return cache


def write_cache(path: Path, cache: Cache) -> None:
    path.write_text(json.dumps(cache), encoding="utf-8")


def update_user(user: User, *, rng: random.Random) -> None:
    if rng.random() <= _CHANCE_USER_MODIFY:
        user["display_name"] = new_display_name(rng)

    if rng.random() <= _CHANCE_USER_MODIFY:
        user["department"] = new_department(rng)

    if rng.random() <= _CHANCE_USER_MODIFY:
        user["division"] = new_division(rng)

    if rng.random() <= _CHANCE_USER_MODIFY:
        user["title"] = new_title(rng)


def update_group(
    key: str,
    *,
    cache: Cache,
    rng: random.Random,
) -> list[str]:
    try:
        group = cache["groups"][key]
    except KeyError:
        cache["groups"][key] = []
        group = cache["groups"][key]

        users = tuple(cache["users"])
        group.extend(rng.sample(users, k=random.randint(0, _N_MEMBERS)))

        return group
    else:
        new_group = [user for user in group if rng.random() > _CHANCE_USER_REMOVE]
        while rng.random() <= _CHANCE_USER_ADD:
            choices = tuple(set(cache["users"]) - set(group))
            if choices:
                new_group.append(rng.choice(choices))

        cache["groups"][key] = new_group
        return new_group


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=functools.partial(
            argparse.ArgumentDefaultsHelpFormatter,
            width=79,
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "query",
        help="LDAP query; expected to be in the form '(cn=abc)' or '(|(cn=abc)...)",
    )
    parser.add_argument(
        "fields",
        nargs="+",
        choices=(
            "cn",
            "department",
            "displayName",
            "division",
            "member",
            "title",
        ),
        help="LDAP field to simulate",
    )

    return parser.parse_known_args(argv)[0]


def main(argv: list[str]) -> int:
    if argv and "--" not in argv:
        abort(f"CLI arguments must contain '--' before query and fields: {argv!r}")

    argv = argv[argv.index("--") + 1 :]
    args = parse_args(argv)
    matches = _RE_CN.findall(args.query)
    if matches is None:
        abort(f"Invalid argument {args.query}; must contain '(CN=key)'")

    rng = random.Random()
    cache = read_cache(_CACHE, rng=rng)

    def _print_field(key: str, value: str) -> None:
        if value.isascii():
            print(f"{key}: {value}")
        else:
            value = base64.b64encode(value.encode()).decode()
            print(f"{key}:: {value}")

    def _print_optional_field(key: str, value: str) -> None:
        if key in args.fields:
            _print_field(key, value)

    for key in matches:
        if _RE_KUID.match(key):
            if user := cache["users"].get(key):
                update_user(user, rng=rng)
                _print_field("dn", f"CN={key},FOO=bar")
                _print_optional_field("cn", key)
                _print_optional_field("displayName", user["display_name"])
                _print_optional_field("department", user["department"])
                _print_optional_field("division", user["division"])
                _print_optional_field("title", user["title"])
            else:
                continue
        else:
            _print_field("dn", f"CN={key},FOO=bar")
            _print_optional_field("cn", key)
            _print_optional_field("displayName", key)

            if "member" in args.fields:  # don't change member lists if not queried
                for member in update_group(key, cache=cache, rng=rng):
                    _print_field("member", f"CN={member},FOO=BAR")

        print()

    write_cache(_CACHE, cache)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
