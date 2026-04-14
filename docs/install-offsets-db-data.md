# Install offsets-db-data

## Installing the package

```{eval-rst}

.. tab-set::

    .. tab-item:: pip

        .. code:: bash

            $ python -m pip install offsets-db-data

    .. tab-item:: pixi

        Add to an existing Pixi project:

        .. code:: bash

            $ pixi add --pypi offsets-db-data

    .. tab-item:: uv

        Add to an existing uv project:

        .. code:: bash

            $ uv add offsets-db-data

        Or install directly into a virtual environment:

        .. code:: bash

            $ uv pip install offsets-db-data
```

## Development setup

To contribute to offsets-db-data, clone the repository and set up the full development environment using [Pixi](https://pixi.sh). Install Pixi first by following the [installation instructions](https://pixi.sh/latest/#installation).

```{eval-rst}

.. tab-set::

    .. tab-item:: Pixi (recommended)

        .. code:: bash

            $ git clone https://github.com/carbonplan/offsets-db-data
            $ cd offsets-db-data
            $ pixi install

        Run common tasks with ``pixi run <task>``:

        .. code:: bash

            $ pixi run test              # run unit tests
            $ pixi run test-cov          # run unit tests with coverage
            $ pixi run test-integration  # run integration tests (requires S3 + OpenRefine)
            $ pixi run test-all          # run unit + integration tests
            $ pixi run lint              # lint
            $ pixi run format            # format code
            $ pixi run format-check      # check formatting
            $ pixi run docs-build        # build documentation

        Activate an interactive shell with all dependencies:

        .. code:: bash

            $ pixi shell

    .. tab-item:: pip

        .. code:: bash

            $ git clone https://github.com/carbonplan/offsets-db-data
            $ cd offsets-db-data
            $ python -m pip install -e ".[dev]"
```
