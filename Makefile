.DEFAULT_GOAL := help
VENV_NAME:=.venv
VENV_TEST:=.venv-test
sources = src

PYTHON:=python
PTYHON_RUN:=uv run
PIP:=uv pip


.PHONY: venv  ## Creates virtual environment.
venv:
	${PYTHON} -m venv ${VENV_NAME}

.PHONY: upgrade-pip  ## Upgrades pip.
upgrade-pip:
	${PIP} install -U pip

.PHONY: install-dev  ## Install the package and only dev dependencies.
install: upgrade-pip
	${PIP} install .[bq,lint,dev,build,docs]

.PHONY: install-test  ## Install the package and only test dependencies.
install-test: upgrade-pip
	${PIP} install .[bq,beam,test]

.PHONY: install-pre-commit  ## Install pre-commit.
install-pre-commit:
	${PYTHON} -m pre_commit install --install-hooks
	${PYTHON} -m pre_commit install --hook-type commit-msg

.PHONY: install  ## Install the package in editable mode for local development.
install: upgrade-pip
	${PIP} install -e .[bq,beam,dev,test,lint,build,docs]

.PHONY: format  ## Auto-format python source files according with PEP8.
format:
	${PYTHON} -m ruff check --fix
	${PYTHON} -m ruff format
	${PYTHON} -m black $(sources) tests

.PHONY: lint  ## Lint python source files.
lint:
	${PYTHON} -m ruff check --no-fix
	${PYTHON} -m ruff format --check
	${PYTHON} -m black --check --diff $(sources) tests

.PHONY: codespell  ## Use Codespell to do spell checking.
codespell:
	${PYTHON} -m codespell_lib

.PHONY: typecheck  ## Perform type-checking.
typecheck:
	${PYTHON} -m mypy

.PHONY: audit  ## Use pip-audit to scan for known vulnerabilities.
audit:
	${PYTHON} -m pip_audit .

.PHONY: test  ## Run all unit tests and generate a coverage report.
test:
	${PYTHON} -m pytest -m "not integration" --cov-report term --cov-report=xml --cov=$(sources)

.PHONY: test-integration  ## Run only integration tests (if configured) without generate a coverage report.
test-integration:
	${PYTHON} -m pytest -m "integration" -rs -n auto --dist=loadscope --maxfail=5 --durations=10 --tb=short

.PHONY: pre-commit  ## Run all pre-commit hooks.
pre-commit:
	${PTYHON_RUN} pre-commit run --all-files

.PHONY: all  ## Run the standard set of checks performed in CI.
all: lint codespell typecheck audit test

.PHONY: clean  ## Clear local caches and build artifacts.
clean:
	# remove Python file artifacts
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]'`
	rm -f `find . -type f -name '*~'`
	rm -f `find . -type f -name '.*~'`
	rm -rf .cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	# remove build artifacts
	rm -rf build
	rm -rf dist
	rm -rf `find . -name '*.egg-info'`
	rm -rf `find . -name '*.egg'`
	# remove test and coverage artifacts
	rm -rf .tox/
	rm -f .coverage
	rm -f .coverage.*
	rm -rf coverage.*
	rm -rf htmlcov/
	rm -rf .pytest_cache
	rm -rf htmlcov

.PHONY: docs  ## Generate HTML documentation.
docs:
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

.PHONY: servedocs  ## Build, watch and serve documentation with live reload in the browser.
servedocs:
	$(MAKE) -C docs livehtml

.PHONY: build  ## Build a source distribution and a wheel distribution.
build: clean
	${PYTHON} -m build

.PHONY: publish  ## Publish the distribution to PyPI.
publish: build
	${PYTHON} -m twine upload dist/* --verbose

.PHONY: publish-test  ## Publish the distribution to TestPyPI.
publish-test: build
	${PYTHON} -m twine upload --repository-url https://test.pypi.org/legacy/ dist/* --verbose

.PHONY: test-installed  ## Run tests against installed package in a fresh venv with coverage.
test-installed:
	python3.12 -m venv $(VENV_TEST)
	$(VENV_TEST)/bin/pip install --upgrade pip setuptools wheel
	$(VENV_TEST)/bin/python -m pip install .[bq,beam,test]
	$(VENV_TEST)/bin/python -m pytest --cov=gfw --cov-report=term --cov-report=xml

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
