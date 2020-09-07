"""Manage tasks for the stats_can library."""
import tempfile

import nox

package = "stats_can"
nox.options.sessions = "tests", "safety", "black", "lint"
locations = "src", "tests", "noxfile.py", "docs/source/conf.py"


def install_with_constraints(session, *args, **kwargs):
    """Install packages constrained by Poetry's lock file.

    This function is a wrapper for nox.sessions.Session.install. It
    invokes pip to install packages inside of the session's virtualenv.
    Additionally, pip is passed a constraints file generated from
    Poetry's lock file, to ensure that the packages are pinned to the
    versions specified in poetry.lock. This allows you to manage the
    packages as Poetry development dependencies.

    Parameters
    ----------
    session
        The Session object.
    args
        Command-line arguments for pip.
    kwargs
        Additional keyword arguments for Session.install.
    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python="3.8")
def black(session):
    """Run black code formatter.

    Parameters
    ----------
    session
        The Session object.
    """
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python=["3.8", "3.7"])
def lint(session):
    """Lint using flake8.

    Parameters
    ----------
    session
        The Session object.
    """
    args = session.posargs or locations
    install_with_constraints(
        session,
        "flake8",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-import-order",
        "darglint",
        "pandas-vet",
    )
    session.run("flake8", *args)


@nox.session(python="3.8")
def safety(session):
    """Scan dependencies for insecure packages.

    Parameters
    ----------
    session
        The Session object.
    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")


@nox.session(python=["3.8", "3.7"])
def tests(session):
    """Run the test suite.

    Parameters
    ----------
    session
        The Session object.
    """
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(
        session, "coverage[toml]", "pytest", "pytest-cov", "pytest-vcr", "freezegun"
    )
    session.run("pytest", *args)


@nox.session(python="3.8")
def coverage(session):
    """Upload coverage data.

    Parameters
    ----------
    session
        The Session object.
    """
    install_with_constraints(session, "coverage[toml]", "codecov")
    session.run("coverage", "xml", "--fail-under=0")
    session.run("codecov", *session.posargs)


@nox.session(python="3.8")
def docs(session):
    """Build the documentation.

    Parameters
    ----------
    session
        The Session object.
    """
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(
        session, "sphinx", "sphinx-autodoc-typehints", "sphinx_rtd_theme"
    )
    session.run("sphinx-build", "docs/source", "docs/_build")
