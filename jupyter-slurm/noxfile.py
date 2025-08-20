import nox

nox.options.sessions = [
    "style",
    "lints",
    "typing",
    "tests",
]


SOURCES = (
    "noxfile.py",
    "src",
    "tests",
)


@nox.session
def style(session: nox.Session) -> None:
    session.install("ruff==0.12.2")
    # Replaces `black --check`
    session.run("ruff", "format", "--check", *SOURCES)
    # Replaces `isort --check-only`
    session.run("ruff", "check", "--select", "I", *SOURCES)


@nox.session
def lints(session: nox.Session) -> None:
    session.install("ruff==0.12.2")
    session.run("ruff", "check", *SOURCES)


@nox.session()
def typing(session: nox.Session) -> None:
    session.install(
        "basedpyright==1.23.2",
        "nox==2024.4.15",
        "pytest==7.4.4",
    )

    session.run("basedpyright", *SOURCES)


@nox.session()
def tests(session: nox.Session) -> None:
    session.install(
        "-e",
        ".",
        "pytest==7.4.4",
        "pytest-cov~=6.0",
        "coverage[toml]~=7.6",
    )

    session.run(
        "python3",
        # Run tests in development mode (enables extra checks)
        "-X",
        "dev",
        # Treat warnings (deprections, etc.) as errors
        "-Werror",
        "-m",
        "pytest",
        "tests",
        "-vv",
        "--cov-branch",
        "--cov",
        "tests",
        "--cov",
        "src",
        "--cov-report=term-missing",
        "--no-cov-on-fail",
    )
