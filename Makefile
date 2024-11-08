.PHONY: docs
test:
	nix develop --impure . -c ./.venv/bin/pre-commit run --all-files --show-diff-on-failure
	nix develop --impure .#python39 -c ./.venv/bin/coverage run -m pytest
	nix develop --impure .#python310 -c ./.venv/bin/coverage run -m pytest
	nix develop --impure .#python311 -c ./.venv/bin/coverage run -m pytest
	nix develop --impure .#python312 -c ./.venv/bin/coverage run -m pytest
	nix develop --impure . -c ./.venv/bin/coverage report
safety:
	nix develop --impure . -c ./.venv/bin/safety check --file=poetry.lock --full-report
docs:
	nix develop --impure . -c ./.venv/bin/sphinx-build docs/source docs/_build
