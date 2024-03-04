"""Manage tasks for the stats_can library."""

import shutil
from pathlib import Path
from textwrap import dedent

import nox
from nox_poetry import Session, session

package = "stats_can"
nox.options.sessions = (
    "pre-commit",
    "safety",
    "tests",
    "coverage",
    "docs",
)

python_versions = [
    "3.11",
    "3.10",
]
python_version = python_versions[0]


def activate_virtualenv_in_precommit_hooks(session: Session) -> None:
    """Activate virtualenv in hooks installed by pre-commit.

    This function patches git hooks installed by pre-commit to activate the
    session's virtual environment. This allows pre-commit to locate hooks in
    that environment when invoked from git.

    Parameters
    ----------
    session: Session
        The Session object.
    """
    if session.bin is None:
        return

    virtualenv = session.env.get("VIRTUAL_ENV")
    if virtualenv is None:
        return

    hookdir = Path(".git") / "hooks"
    if not hookdir.is_dir():
        return

    for hook in hookdir.iterdir():
        if hook.name.endswith(".sample") or not hook.is_file():
            continue

        text = hook.read_text()
        bindir = repr(session.bin)[1:-1]  # strip quotes
        if not (
            Path("A") == Path("a") and bindir.lower() in text.lower() or bindir in text
        ):
            continue

        lines = text.splitlines()
        if not (lines[0].startswith("#!") and "python" in lines[0].lower()):
            continue

        header = dedent(
            f"""\
            import os
            os.environ["VIRTUAL_ENV"] = {virtualenv!r}
            os.environ["PATH"] = os.pathsep.join((
                {session.bin!r},
                os.environ.get("PATH", ""),
            ))
            """
        )

        lines.insert(1, header)
        hook.write_text("\n".join(lines))


@session(name="pre-commit", python=python_version)
def precommit(session: Session) -> None:
    """Lint using pre-commit.

    Parameters
    ----------
    session: Session
        The Session object.
    """
    args = session.posargs or ["run", "--all-files", "--show-diff-on-failure"]
    session.install(
        "black",
        "darglint",
        "flake8",
        "flake8-bandit",
        "flake8-bugbear",
        "flake8-docstrings",
        "pep8-naming",
        "pre-commit",
        "pre-commit-hooks",
        "reorder-python-imports",
    )
    session.run("pre-commit", *args)
    if args and args[0] == "install":
        activate_virtualenv_in_precommit_hooks(session)


@session(python=python_version)
def safety(session):
    """Scan dependencies for insecure packages.

    Parameters
    ----------
    session
        The Session object.
    """
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", f"--file={requirements}", "--full-report")


@session(python=python_versions)
def tests(session):
    """Run the test suite.

    Parameters
    ----------
    session
        The Session object.
    """
    session.install(".")
    session.install("coverage[toml]", "pytest", "pytest-cov", "pytest-vcr", "freezegun")
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage")


@session
def coverage(session):
    """Upload coverage data.

    Parameters
    ----------
    session
        The Session object.
    """
    # Do not use session.posargs unless this is the only session.
    has_args = session.posargs and len(session._runner.manifest) == 1
    args = session.posargs if has_args else ["report"]

    session.install("coverage[toml]")

    if not has_args and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(python=python_version)
def docs(session):
    """Build the documentation.

    Parameters
    ----------
    session
        The Session object.
    """
    args = session.posargs or ["docs/source", "docs/_build"]
    session.install(".")
    session.install("sphinx", "sphinx-autodoc-typehints", "sphinx-rtd-theme")

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)
