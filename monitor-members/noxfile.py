import nox

nox.options.sessions = [
    "style",
    "lints",
    "typing",
]


SOURCES = (
    "noxfile.py",
    "src",
)


class Requirements:
    NOX = "nox~=2024.10.9"
    PYRIGHT = "basedpyright==1.26.0"
    RUFF = "ruff==0.9.3"


@nox.session
def style(session: nox.Session) -> None:
    session.install(Requirements.RUFF)
    # Replaces `black --check`
    session.run("ruff", "format", "--check", *SOURCES)
    # Replaces `isort --check-only`
    session.run("ruff", "check", "--select", "I", *SOURCES)


@nox.session
def lints(session: nox.Session) -> None:
    session.install(Requirements.RUFF)
    session.run("ruff", "check", *SOURCES)


@nox.session()
def typing(session: nox.Session) -> None:
    session.install(
        ".",
        Requirements.NOX,
        Requirements.PYRIGHT,
    )

    session.run("basedpyright", *SOURCES)
