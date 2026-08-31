import nox

nox.options.sessions = [
    "style",
    "lints",
    "typing",
]


SOURCES = (
    "noxfile.py",
    "simulator",
    "src",
)


@nox.session
def style(session: nox.Session) -> None:
    session.install("--group", "dev", ".")
    session.run("ruff", "format", "--check", *SOURCES)


@nox.session
def lints(session: nox.Session) -> None:
    session.install("--group", "dev", ".")
    session.run("ruff", "check", *SOURCES)


@nox.session()
def typing(session: nox.Session) -> None:
    session.install("--group", "dev", ".")
    session.run("zuban", "check", "--strict", *SOURCES)
