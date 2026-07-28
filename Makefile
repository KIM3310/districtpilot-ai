.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3

.PHONY: test verify

test:
	$(PYTHON) -m unittest discover -s tests -v

verify: test
	$(PYTHON) scripts/validate_submission_surface.py
	$(PYTHON) scripts/validate_repository_surface.py
	$(PYTHON) scripts/validate_architecture_blueprint.py
