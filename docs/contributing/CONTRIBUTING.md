# Contributing to Metrics Pipeline

Thank you for your interest in contributing to the Metrics Pipeline project! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. Please read it before contributing.

## How to Contribute

### Reporting Bugs

If you find a bug, please report it by creating an issue on GitHub. When filing an issue, make sure to answer these questions:

1. What version of the Metrics Pipeline are you using?
2. What operating system and processor architecture are you using?
3. What did you do?
4. What did you expect to see?
5. What did you see instead?

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

1. A clear and descriptive title
2. A detailed description of the proposed functionality
3. Any potential implementation approaches you've considered
4. Why this enhancement would be useful to most users

### Pull Requests

We actively welcome your pull requests!

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code lints
6. Submit the pull request!

## Development Process

### Setting Up Development Environment

1. Clone the repository:
   ```bash
   git clone https://github.com/zaingz/metrics-pipeline.git
   cd metrics-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

4. Start the local development environment:
   ```bash
   make dev
   ```

### Testing

Run the tests:
```bash
make test
```

Run linting:
```bash
make lint
```

Format code:
```bash
make format
```

### Coding Style

We use the following tools to enforce coding style:

- Black for code formatting
- isort for import sorting
- flake8 for linting
- mypy for type checking

These are all configured in the project's `pyproject.toml` and will be run automatically by pre-commit hooks.

## Extending the Metrics Pipeline

### Adding a New Ingestion Adapter

1. Create a new file in `src/metrics_pipeline/adapters/ingestion/` (e.g., `custom.py`)
2. Implement the `IngestionAdapter` interface
3. Add your adapter to `__init__.py`
4. Add tests in `tests/unit/adapters/ingestion/`
5. Update documentation

### Adding a New Storage Adapter

1. Create a new file in `src/metrics_pipeline/adapters/storage/` (e.g., `custom.py`)
2. Implement the `StorageAdapter` interface
3. Add your adapter to `__init__.py`
4. Add tests in `tests/unit/adapters/storage/`
5. Update documentation

### Adding a New Visualization Adapter

1. Create a new file in `src/metrics_pipeline/adapters/visualization/` (e.g., `custom.py`)
2. Implement the `VisualizationAdapter` interface
3. Add your adapter to `__init__.py`
4. Add tests in `tests/unit/adapters/visualization/`
5. Update documentation

## Release Process

1. Update version in `src/metrics_pipeline/__init__.py`
2. Update CHANGELOG.md
3. Create a new GitHub release
4. CI/CD will automatically publish to PyPI

## Project Structure

```
metrics-pipeline/
├── src/
│   └── metrics_pipeline/
│       ├── adapters/
│       │   ├── ingestion/
│       │   ├── storage/
│       │   └── visualization/
│       ├── core/
│       │   ├── models/
│       │   └── pipeline/
│       ├── utils/
│       └── cli/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── docs/
│   ├── architecture/
│   ├── api/
│   ├── deployment/
│   └── contributing/
├── examples/
│   ├── aws/
│   └── local/
└── deployment/
    ├── aws/
    └── local/
```

## License

By contributing, you agree that your contributions will be licensed under the project's MIT License.
