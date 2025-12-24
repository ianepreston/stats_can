.PHONY: docs
test:
	nix develop --impure . -c pre-commit run --all-files --show-diff-on-failure
	nix develop --impure . -c ruff check .
	nix develop --impure .#python310 -c coverage run -m pytest
	nix develop --impure .#python311 -c coverage run -m pytest
	nix develop --impure .#python312 -c coverage run -m pytest
	nix develop --impure .#python313 -c coverage run -m pytest
	nix develop --impure . -c coverage report
safety:
	nix develop --impure . -c safety check --file=uv.lock --full-report
