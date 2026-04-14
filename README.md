<p align='left'>
  <a href='https://carbonplan.org/#gh-light-mode-only'>
    <img
      src='https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png'
      height='48px'
    />
  </a>
  <a href='https://carbonplan.org/#gh-dark-mode-only'>
    <img
      src='https://carbonplan-assets.s3.amazonaws.com/monogram/light-small.png'
      height='48px'
    />
  </a>
</p>

[![CI](https://github.com/carbonplan/offsets-db-data/actions/workflows/CI.yaml/badge.svg)](https://github.com/carbonplan/offsets-db-data/actions/workflows/CI.yaml)
[![PyPI](https://github.com/carbonplan/offsets-db-data/actions/workflows/pypi.yaml/badge.svg)](https://github.com/carbonplan/offsets-db-data/actions/workflows/pypi.yaml)
[![PyPI][pypi-badge]][pypi-link]
[![Documentation Status][rtd-badge]][rtd-link]

# carbonplan / offsets-db-data

Utilities for cleaning, and processing data for the [OffsetsDB web tool](https://carbonplan.org/research/offsets-db/)

## installation

This project uses [Pixi](https://pixi.sh) for environment and dependency management.

1. Install Pixi: follow the [installation instructions](https://pixi.sh/latest/#installation)

2. Clone and set up:

```bash
git clone https://github.com/carbonplan/offsets-db-data.git
cd offsets-db-data
pixi install
```

Common tasks:

```bash
pixi run test              # Run unit tests
pixi run test-cov          # Run unit tests with coverage
pixi run test-integration  # Run integration tests (requires S3 + OpenRefine)
pixi run test-all          # Run unit + integration tests
pixi run lint              # Run linting
pixi run format            # Format code
pixi run format-check      # Check formatting without modifying files
pixi run docs-build        # Build documentation
```

Activate an interactive shell with all dependencies:

```bash
pixi shell
```

### pip (alternative)

```bash
python -m pip install offsets-db-data
# or editable install with dev deps:
python -m pip install -e ".[dev]"
```

## building the documentation

```bash
pixi run docs-build
```

Open `docs/_build/index.html` in your browser to view the result.

## license

All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/) licensed.

> [!IMPORTANT]
> Data associated with this repository are subject to additional [terms of data access](https://github.com/carbonplan/offsets-db-data/blob/main/TERMS_OF_DATA_ACCESS).

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/offsets-db/issues/new) or [sending us an email](mailto:hello@carbonplan.org).

[pypi-badge]: https://img.shields.io/pypi/v/offsets-db-data?logo=pypi
[pypi-link]: https://pypi.org/project/offsets-db-data
[rtd-badge]: https://readthedocs.org/projects/offsets-db-data/badge/?version=latest
[rtd-link]: https://offsets-db-data.readthedocs.io/en/latest/?badge=latest
