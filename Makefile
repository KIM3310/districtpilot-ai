SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
VENV_STAMP := $(VENV)/.installed-dev

.PHONY: install test repository-verify verify pages-deploy

$(VENV_PYTHON):
	$(PYTHON) -m venv $(VENV)

$(VENV_STAMP): requirements-dev.txt $(VENV_PYTHON)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt
	touch $(VENV_STAMP)

install: $(VENV_STAMP)

test: install
	$(VENV_PYTHON) -m pytest -q

repository-verify: install
	$(VENV_PYTHON) scripts/validate_submission_surface.py
	$(VENV_PYTHON) scripts/validate_repository_surface.py
	$(VENV_PYTHON) scripts/validate_architecture_blueprint.py

verify: test repository-verify

pages-deploy:
	npx --yes wrangler@4 pages deploy site --project-name districtpilot-ai
