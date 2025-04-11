.PHONY: help setup dev test lint format clean build deploy-local deploy-aws docs

# Default target
help:
	@echo "Metrics Pipeline Makefile"
	@echo "-------------------------"
	@echo "setup         - Install dependencies"
	@echo "dev           - Start local development environment"
	@echo "test          - Run tests"
	@echo "lint          - Run linting checks"
	@echo "format        - Format code"
	@echo "clean         - Clean build artifacts"
	@echo "build         - Build package"
	@echo "deploy-local  - Deploy to local environment"
	@echo "deploy-aws    - Deploy to AWS"
	@echo "docs          - Generate documentation"

# Setup development environment
setup:
	pip install -e ".[dev]"
	pre-commit install

# Start local development environment
dev:
	docker-compose up -d

# Run tests
test:
	pytest

# Run linting
lint:
	flake8 src tests
	mypy src tests
	black --check src tests
	isort --check src tests

# Format code
format:
	black src tests
	isort src tests

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete

# Build package
build: clean
	python -m build

# Deploy to local environment
deploy-local:
	docker-compose down -v
	docker-compose up -d

# Deploy to AWS
deploy-aws:
	cd deployment/aws && pulumi up

# Generate documentation
docs:
	cd docs && mkdocs build

# Initialize local environment
init-local:
	docker-compose up -d localstack
	sleep 5
	python deployment/local/init_localstack.py

# Run integration tests
integration-test:
	pytest tests/integration

# Run end-to-end tests
e2e-test:
	pytest tests/e2e
