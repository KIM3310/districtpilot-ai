.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3

.PHONY: test verify pages-deploy

test:
	$(PYTHON) -m unittest discover -s tests -v

verify: test
	$(PYTHON) scripts/validate_submission_surface.py
	$(PYTHON) scripts/validate_repository_surface.py
	$(PYTHON) scripts/validate_architecture_blueprint.py

pages-deploy:
	npx --yes wrangler@4 pages deploy site --project-name districtpilot-ai
