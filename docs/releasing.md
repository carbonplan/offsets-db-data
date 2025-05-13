# Releasing a New Version of Offsets-DB-Data

This document outlines the steps to release a new version of the offsets-db-data package to PyPI. It includes updating the version number, building the package, and deploying it using GitHub Actions.

## 1. Create a GitHub Release

Go to the GitHub repository and create a new release. This can be done by navigating to the ["Releases"](https://github.com/carbonplan/offsets-db-data/releases) section and clicking on "Draft a new release." Tag the release with the version number you just updated. We've been using [CalVer](https://calver.org/) for versioning, so the tag should look like `vYYYY.MM.DD` (e.g., `v2023.10.01`). Once you have filled in the tag details, click on "Generate release notes" to automatically generate the release notes based on the commits since the last release. Review and edit the notes as necessary, then publish the release.

## 2. Automated Deployment to PyPI

Once the release is published, the GitHub Actions workflow will automatically trigger the deployment to PyPI. The workflow defined in `.github/workflows/pypi.yaml` will handle the following:

- Downloading the built artifacts from the previous steps.
- Publishing the package to PyPI using the `pypa/gh-action-pypi-publish` action.

## 3. Verify the Release on PyPI

After the GitHub Actions workflow completes, verify that your package is available on PyPI by visiting [PyPI](https://pypi.org/project/offsets-db-data/).
