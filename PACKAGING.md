# Packaging for Distribution

This file contains instructions for building and distributing the metrics-pipeline package.

## Building the Package

To build the package, run:

```bash
# Install build dependencies
pip install build twine

# Build the package
python -m build
```

This will create both source distribution (.tar.gz) and wheel (.whl) files in the `dist/` directory.

## Checking the Package

Before uploading to PyPI, it's a good practice to check the package:

```bash
# Check the package
twine check dist/*
```

## Uploading to PyPI

To upload the package to PyPI:

```bash
# Upload to PyPI
twine upload dist/*
```

You'll need to have a PyPI account and be registered as a maintainer of the package.

## Uploading to TestPyPI

For testing purposes, you can upload to TestPyPI first:

```bash
# Upload to TestPyPI
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

## Installing from TestPyPI

To install the package from TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ metrics-pipeline
```

## Version Management

When releasing a new version:

1. Update the version number in `src/metrics_pipeline/__init__.py`
2. Update the CHANGELOG.md file
3. Commit the changes
4. Tag the commit with the version number
5. Push the changes and tags
6. Build and upload the package

## Automated Releases

The CI/CD pipeline is configured to automatically build and publish the package to PyPI when a new tag is pushed to the repository. To create a new release:

1. Update the version number in `src/metrics_pipeline/__init__.py`
2. Update the CHANGELOG.md file
3. Commit the changes
4. Tag the commit with the version number:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   ```
5. Push the changes and tags:
   ```bash
   git push origin main --tags
   ```

The CI/CD pipeline will automatically build and publish the package to PyPI.
