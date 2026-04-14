# Releasing a New Version of Offsets-DB-Data

This document outlines the steps to release a new version of the offsets-db-data package to PyPI.

## 1. Create a GitHub Release

Go to the GitHub repository and create a new release. Navigate to the ["Releases"](https://github.com/carbonplan/offsets-db-data/releases) section and click "Draft a new release." Tag the release using [CalVer](https://calver.org/) format: `vYYYY.MM.DD` (e.g., `v2023.10.01`). Click "Generate release notes" to auto-populate notes from commits since the last release, review and edit as needed, then publish.

## 2. Automated Deployment to PyPI

Publishing the release triggers the GitHub Actions workflow defined in [`.github/workflows/pypi.yaml`](https://github.com/carbonplan/offsets-db-data/blob/main/.github/workflows/pypi.yaml). The workflow:

1. **Builds artifacts** using Pixi's `publish` environment:
   ```bash
   pixi run -e publish python -m build --sdist --wheel .
   pixi run -e publish python -m twine check dist/*
   ```
2. **Publishes to PyPI** using the [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish) action via OIDC (no API token required).

The `publish` Pixi environment is defined in `pyproject.toml` and includes `python-build`, `twine`, and `check-manifest`.

## 3. Verify the Release on PyPI

After the workflow completes, verify the package is available at [pypi.org/project/offsets-db-data](https://pypi.org/project/offsets-db-data/).
