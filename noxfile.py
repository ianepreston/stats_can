import nox
import tempfile


nox.options.sessions = "tests"  # , "lint", "safety"


def install_with_constraints(session, *args, **kwargs):
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
def tests(session):
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", external=True)
    session.run("pytest", *args)


locations = "src", "tests", "noxfile.py"


# @nox.session(python=["3.8", "3.7"])
# def lint(session):
#     args = session.posargs or locations
#     install_with_constraints(
#         session,
#         "flake8",
#         "flake8-black",
#         "flake8-import-order",
#         "pandas-vet",
#         "flake8-bugbear",
#         "flake8-bandit",
#     )
#     session.run("flake8", *args)


@nox.session(python="3.8")
def black(session):
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python="3.8")
def safety(session):
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


@nox.session(python="3.8")
def coverage(session):
    """Upload coverage data."""
    install_with_constraints(session, "coverage[toml]", "codecov")
    session.run("coverage", "xml", "--fail-under=0")
    session.run("codecov", *session.posargs)
