test:
	nix develop . -c pre-commit run --all-files --show-diff-on-failure
	nix develop .#python310 -c coverage run -m pytest
	nix develop .#python311 -c coverage run -m pytest
	nix develop . -c coverage report
safety:
	nix develop . -c safety check --file=poetry.lock --full-report
docs:
	nix develop . -c sphinx-build docs/source docs/_build
