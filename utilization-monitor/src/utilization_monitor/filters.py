import fnmatch
from collections.abc import Iterable
from pathlib import Path


class CommandFilters:
    __slots__ = [
        "_executables",
        "_patterns",
    ]

    _executables: set[str]
    _patterns: list[str]

    def __init__(self, patterns: Iterable[str]) -> None:
        self._executables = set()
        self._patterns = []

        for pat in patterns:
            if "*" in pat or "[" in pat or "]" in pat:
                self._patterns.append(pat)
            else:
                self._executables.add(pat)

    def __call__(self, command: list[str] | None) -> bool:
        if not command:
            return "*" in self._patterns
        elif Path(command[0]).name in self._executables:
            return True

        return any(fnmatch.fnmatch(" ".join(command), pat) for pat in self._patterns)
