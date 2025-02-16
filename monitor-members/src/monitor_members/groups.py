from __future__ import annotations

import enum
import fnmatch
from functools import total_ordering

from monitor_members.common import abort


@total_ordering
class GroupType(enum.Enum):
    SENSITIVE = 0
    MANDATORY = 1
    REGULAR = 2

    def __lt__(self, other: GroupType) -> int:
        return self.value < other.value


def collect_groups(
    *,
    regular_groups: list[str],
    mandatory_groups: list[str],
    sensitive_groups: list[str],
) -> dict[str, GroupType]:
    groups = {name.lower(): GroupType.REGULAR for name in regular_groups}
    if invalid_groups := [name for name in groups if _is_glob(name)]:
        abort("invalid group names in `monitor` list: ", invalid_groups)

    sensitive = _collect_groups(sensitive_groups, groups)
    mandatory = _collect_groups(mandatory_groups, groups)

    groups.update((name, GroupType.MANDATORY) for name in mandatory)
    groups.update((name, GroupType.SENSITIVE) for name in sensitive)

    return groups


def _collect_groups(current: list[str], regular: dict[str, GroupType]) -> set[str]:
    groups: set[str] = set()
    for name in current:
        name = name.lower()
        if _is_glob(name):
            if matches := list(fnmatch.filter(regular, name)):
                groups.update(matches)
            else:
                abort(f"No matching groups found for glob {name!r}")
        else:
            groups.add(name)

    return groups


def _is_glob(value: str) -> bool:
    return bool({"*", "?", "[", "]"}.intersection(value))
