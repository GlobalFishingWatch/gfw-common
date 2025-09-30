<h1 align="center" style="border-bottom: none;"> gfw-common </h1>

<p align="center">
  <a href="https://github.com/GlobalFishingWatch/gfw-common-client/actions/workflows/ci.yaml" >
    <img src="https://github.com/GlobalFishingWatch/gfw-common/actions/workflows/ci.yaml/badge.svg"/>
  </a>
  <a href="https://codecov.io/gh/GlobalFishingWatch/gfw-common" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/gfw-common/graph/badge.svg?token=bpFiU6qtrd"/>
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/gfw-common">
  </a>
</p>

Common place for GFW reusable Python components.

[commitizen]: https://github.com/commitizen-tools/commitizen
[Conventional Commits]: https://www.conventionalcommits.org/en/v1.0.0/
[GitHub Flow]: https://githubflow.github.io
[How to Write a Git Commit Message]: https://cbea.ms/git-commit/
[PEP8]: https://peps.python.org/pep-0008/
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[Semantic Versioning]: https://semver.org

[examples]: examples/
[GITHUB-FLOW.md]: GITHUB-FLOW.md
[Makefile]: Makefile
[pre-commit hooks]: .pre-commit-config.yaml
[pyproject.toml]: pyproject.toml
[release.yaml]: .github/workflows/release.yaml

[gfw.common.beam]: src/gfw/common/beam/
[gfw.common.cli]: src/gfw/common/cli/
[gfw.common.bigquery_helper.py]: src/gfw/common/bigquery_helper.py
[gfw.common.datetime]: src/gfw/common/datetime.py
[gfw.common.decorators.py]: src/gfw/common/decorators.py
[gfw.common.dictionaries.py]: src/gfw/common/dictionaries.py
[gfw.common.io.py]: src/gfw/common/io.py
[gfw.common.iterables.py]: src/gfw/common/iterables.py
[gfw.common.logging.py]: src/gfw/common/logging.py
[gfw.common.serialization.py]: src/gfw/common/serialization.py


## Introduction

<div align="justify">

The following table shows a summary of the current supported features:

<div align="center">

| Module/Package                 | Description                                                         |
| ------------------------------ | ------------------------------------------------------------------- |
|[gfw.common.beam]               | Common utilities and wrappers for Apache Beam pipelines.            |
|[gfw.common.cli]                | Lightweight framework around argparse for building CLIs more easily.|
|[gfw.common.bigquery_helper.py] | Wrapper with extended functionality for bigquery.Client.            |
|[gfw.common.datetime]           | Simple helper functions around stdlib datetime module.              |
|[gfw.common.decorators.py]      | Basic function decorators.                                          |
|[gfw.common.dictionaries.py]    | Simple helper functions for dictionary manipulation.                |
|[gfw.common.io.py]              | Basic IO functions.                                                 |
|[gfw.common.iterables.py]       | Iterables utilities.                                                |
|[gfw.common.logging.py]         | Basic logging configuration.                                        |
|[gfw.common.serialization.py]   | Basic serialization utilities.                                      |

</div>

## Installation

The package offers extras to avoid installing unnecessary dependencies:

- `beam`: includes dependencies for the `gfw.common.beam` package.
- `bq`: includes dependencies for BigQuery utilities.

For a default installation without extras, run:
```shell
pip install gfw-common
```
To install all extras, run:
```shell
pip install gfw-common[bq,beam]
```

## Usage

You can see examples in the [examples] folder.

## How to Contribute

### Preparing the environment

First, clone the repository.
```shell
git clone https://github.com/GlobalFishingWatch/gfw-common.git
```

Create virtual environment and activate it:
```shell
make venv
./.venv/bin/activate
```

Install pre-commit hooks for local development:
```shell
make install-pre-commit
```

Install dependencies and package in editable mode for local development:
```shell
make install
```

Make sure you can run unit tests:
```shell
make test
```

### Development Workflow

Regarding the git workflow, we follow [GitHub Flow].
See [GITHUB-FLOW.md] for a quick summary.

Try to write good commit messages.
See [How to Write a Git Commit Message] guide for details.

The [pre-commit hooks] will take care of validating your code before a commit
in terms of [PEP8] standards, type-checking, miss-pellings, missing documentation, etc.
If you want/need to do it manually, you have commands in the [Makefile].
To see options, type `make`.

### How to Release

Creating a tag will automatically trigger a GitHub Action ([release.yaml]) to publish the package to PyPI.
The tag must match the version declared in [pyproject.toml]; this will be validated by the action.
The tag name must follow the format `vX.Y.Z`.

</div>

> [!NOTE]
In this context, **X**, **Y** and **Z** refer to **MAJOR**, **MINOR** and **PATCH** of [Semantic Versioning].
