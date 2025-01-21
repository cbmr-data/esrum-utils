import nox

nox.options.sessions = [
    "style",
    "lints",
    "typing",
    "tests",
]


SOURCES = ("add_dbsnp_ids.py",)


RUFF_REQUIREMENT = "ruff==0.9.0"


@nox.session
def style(session: nox.Session) -> None:
    session.install(RUFF_REQUIREMENT)
    # Replaces `black --check`
    session.run("ruff", "format", "--check", *SOURCES)
    # Replaces `isort --check-only`
    session.run("ruff", "check", "--select", "I", *SOURCES)


@nox.session
def lints(session: nox.Session) -> None:
    session.install(RUFF_REQUIREMENT)
    session.run("ruff", "check", *SOURCES)


@nox.session()
def typing(session: nox.Session) -> None:
    session.install("nox~=2023.4.22")
    session.install("basedpyright==1.23.2")
    session.run("basedpyright", *SOURCES)


@nox.session()
def tests(session: nox.Session) -> None:
    session.install("pytest")
    session.run("pytest", "./tests/")
