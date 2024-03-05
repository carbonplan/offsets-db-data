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

# carbonplan / offsets-db-data

Utilities for cleaning, and processing data for the [carbonplan/offsets-db-web](https://github.com/carbonplan/offsets-db-web)

## Installation

To install the package, you can use pip:

```bash
python -m pip install git+https://github.com/carbonplan/offsets-db-data.git
```

You can also install the package locally by cloning the repository and running:

```bash
git clone https://github.com/carbonplan/offsets-db-data.git
cd offsets-db-data
python -m pip install -e .
```

To install the dependencies for development, you can use pip:

```bash
python -m pip install -e ".[all]"

# or

python -m pip install -e ".[dev]"

```

## Building the documentation

To build the documentation locally, you can use [sphinx](https://www.sphinx-doc.org/en/master/). You can install the documentation dependencies by running:

```bash
python -m pip install -r requirements-docs.txt
python -m pip install .
```

Then, you can build the documentation by running:

```bash
sphinx-build docs docs/_build
```

You can view the documentation by opening `docs/_build/index.html` in your browser.

## license

> [!IMPORTANT]
> All the code in this repository is [MIT](https://choosealicense.com/licenses/mit/) licensed.
Data associated with this repository are subject to additional [terms of data access](./docs/TERMS-OF-DATA-ACCESS.md).

## about us

CarbonPlan is a non-profit organization that uses data and science for climate action. We aim to improve the transparency and scientific integrity of carbon removal and climate solutions through open data and tools. Find out more at [carbonplan.org](https://carbonplan.org/) or get in touch by [opening an issue](https://github.com/carbonplan/offsets-db/issues/new) or [sending us an email](mailto:hello@carbonplan.org).

[pypi-badge]: https://img.shields.io/pypi/v/offsets-db-data?logo=pypi
[pypi-link]: https://pypi.org/project/offsets-db-data
