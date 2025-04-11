# Metrics Pipeline

A scalable, extensible metrics ingestion and visualization pipeline for collecting, processing, and visualizing user interaction metrics.

[![CI/CD](https://github.com/zaingz/metrics-pipeline/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/zaingz/metrics-pipeline/actions/workflows/ci-cd.yml)
[![PyPI version](https://badge.fury.io/py/metrics-pipeline.svg)](https://badge.fury.io/py/metrics-pipeline)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

Metrics Pipeline is an open-source framework designed to help developers collect, process, and visualize user interaction metrics such as page views, clicks, and other events. The system is built with extensibility in mind, allowing you to adapt it to your specific needs through a flexible adapter pattern.

### Key Features

- **Extensible Architecture**: Adapter pattern allows easy integration with different ingestion sources, storage backends, and visualization tools
- **AWS Integration**: Out-of-the-box support for AWS services (SQS, etc.)
- **Local Development**: LocalStack support for local testing without cloud resources
- **Performance Optimized**: Batch processing, caching, and retry mechanisms for reliability and scalability
- **Comprehensive Documentation**: Detailed guides for setup, deployment, and extending the system
- **Well-Tested**: Extensive unit and integration tests

## Quick Start

### Installation

```bash
pip install metrics-pipeline
```

### Basic Usage

```python
import asyncio
from metrics_pipeline.adapters.ingestion import HTTPIngestionAdapter
from metrics_pipeline.adapters.storage import InMemoryStorageAdapter
from metrics_pipeline.core.pipeline import MetricsPipeline

async def main():
    # Initialize adapters
    ingestion_adapter = HTTPIngestionAdapter(
        api_url="https://api.example.com/metrics"
    )
    
    storage_adapter = InMemoryStorageAdapter()
    
    # Create pipeline
    pipeline = MetricsPipeline(
        ingestion_adapter=ingestion_adapter,
        storage_adapter=storage_adapter
    )
    
    # Start processing
    await pipeline.start_processing()
    
    # Keep the pipeline running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        # Stop processing on keyboard interrupt
        await pipeline.stop_processing()

if __name__ == "__main__":
    asyncio.run(main())
```

## Documentation

For detailed documentation, please see the [docs](./docs) directory:

- [Architecture Overview](./docs/architecture/overview.md)
- [API Reference](./docs/api/reference.md)
- [Usage Guide](./docs/api/usage.md)
- [Deployment Guide](./docs/deployment/guide.md)
- [Contributing Guidelines](./docs/contributing/CONTRIBUTING.md)

## Examples

Check out the [examples](./examples) directory for complete working examples:

- [Local Development](./examples/local/local_pipeline_example.py)
- [AWS Integration](./examples/aws/aws_pipeline_example.py)

## Extending the Pipeline

The Metrics Pipeline is designed to be extended through the adapter pattern. You can create custom adapters for:

### Ingestion Sources

```python
from metrics_pipeline.adapters.ingestion import IngestionAdapter

class CustomIngestionAdapter(IngestionAdapter):
    # Implement your custom ingestion logic
    ...
```

### Storage Backends

```python
from metrics_pipeline.adapters.storage import StorageAdapter

class CustomStorageAdapter(StorageAdapter):
    # Implement your custom storage logic
    ...
```

### Visualization Tools

```python
from metrics_pipeline.adapters.visualization import VisualizationAdapter

class CustomVisualizationAdapter(VisualizationAdapter):
    # Implement your custom visualization logic
    ...
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/zaingz/metrics-pipeline.git
cd metrics-pipeline

# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run all tests with coverage
pytest --cov=metrics_pipeline
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please see our [Contributing Guidelines](./docs/contributing/CONTRIBUTING.md) for more details.
