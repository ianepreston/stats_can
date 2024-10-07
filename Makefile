.PHONY: docs
test:
	nix develop --impure . -c poetry install && pre-commit run --all-files --show-diff-on-failure
	nix develop --impure . -c poetry install && ruff check .
	nix develop --impure .#python39 -c poetry install && coverage run -m pytest
	nix develop --impure .#python310 -c poetry install && coverage run -m pytest
	nix develop --impure .#python311 -c poetry install && coverage run -m pytest
	nix develop --impure .#python312 -c poetry install && coverage run -m pytest
	nix develop --impure . -c poetry install && coverage report
safety:
	nix develop --impure . -c poetry install && safety check --file=poetry.lock --full-report
docs:
	nix develop --impure . -c poetry install && sphinx-build docs/source docs/_build
