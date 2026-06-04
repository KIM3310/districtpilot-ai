.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3

.PHONY: verify

verify:
	$(PYTHON) scripts/validate_repository_surface.py
	$(PYTHON) scripts/validate_architecture_blueprint.py
