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

### Using Pixi (Recommended)

This project uses [Pixi](https://pixi.sh) for environment and dependency management. To get started:

1. Install Pixi by following the [installation instructions](https://pixi.sh/latest/#installation)

2. Clone the repository and set up the environment:

```bash
git clone https://github.com/carbonplan/offsets-db-data.git
cd offsets-db-data
pixi install
```

The default environment includes all development dependencies. You can run commands using:

```bash
pixi run test          # Run tests
pixi run test-cov      # Run tests with coverage
pixi run lint          # Run linting
pixi run format        # Format code
pixi run docs-build    # Build documentation
```

To activate the environment in your shell:

```bash
pixi shell
```

### Using pip

You can also install the package using pip:

```bash
python -m pip install git+https://github.com/carbonplan/offsets-db-data.git
```

Or install locally with development dependencies:

```bash
git clone https://github.com/carbonplan/offsets-db-data.git
cd offsets-db-data
python -m pip install -e ".[dev]"
```

## building the documentation

With Pixi:

```bash
pixi run docs-build
```

Or with pip after installing documentation dependencies:

```bash
python -m pip install -e ".[docs]"
sphinx-build docs docs/_build
```

You can view the documentation by opening `docs/_build/index.html` in your browser.

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
