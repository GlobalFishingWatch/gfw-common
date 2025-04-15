<h1 align="center" style="border-bottom: none;"> gfw-common </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/gfw-common" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/gfw-common/graph/badge.svg?token=bpFiU6qtrd"/>
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/gfw-common">
  </a>
</p>

Common place for GFW reusable components.

[commitizen]: https://github.com/commitizen-tools/commitizen
[Conventional Commits]: https://www.conventionalcommits.org/en/v1.0.0/
[git-flow]: https://nvie.com/posts/a-successful-git-branching-model/
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/


[GIT-WORKFLOW.md]: GIT-WORKFLOW.md
[Makefile]: Makefile
[pre-commit hooks]: .pre-commit-config.yaml
[pyproject.toml]: pyproject.toml

## Introduction

<div align="justify">

TDB.

</div>

| Module | Description |
| --- | --- |
| [-] | -.  |

## Installation

TBD.

## Usage

TBD.

## Development

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

Install the package, dependencies, and pre-commit hooks for local development:
```shell
make install
```

Make sure you can run unit tests:
```shell
make test
```

> [!NOTE]
> Alternatively,
  you can perform all the development inside a docker container
  without the need of installing dependencies in a virtual environment.
  See other options in the [Makefile].

### Workflow

Regarding the git workflow, we just use [git-flow].
See [GIT-WORKFLOW.md] for details.

We use [Conventional Commits] as a standard for commit messages and we enforce it with [commitizen].

The [pre-commit hooks] will take care of validating your code before a commit
in terms of PEP8 standards, type-checking, miss-pellings, missing documentation, etc.
If you want/need to do it manually, you have commands in the [Makefile].
To see options, type:
```shell
make
```

### How to release

TDB.
