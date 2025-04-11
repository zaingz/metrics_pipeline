from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="metrics_pipeline",
    version="0.1.0",
    author="Metrics Pipeline Contributors",
    author_email="your.email@example.com",
    description="A scalable, extensible metrics ingestion and visualization pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zaingz/metrics_pipeline",
    project_urls={
        "Bug Tracker": "https://github.com/zaingz/metrics_pipeline/issues",
        "Documentation": "https://github.com/zaingz/metrics_pipeline/tree/main/docs",
        "Source Code": "https://github.com/zaingz/metrics_pipeline",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.26.0",
        "pulumi>=3.0.0",
        "pulumi-aws>=5.0.0",
        "clickhouse-driver>=0.2.0",
        "pydantic>=2.0.0",
        "fastapi>=0.95.0",
        "uvicorn>=0.22.0",
        "python-dotenv>=1.0.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.3.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.3.0",
            "pre-commit>=3.3.2",
            "localstack>=2.0.0",
            "moto>=4.1.0",
        ],
        "aws": [
            "boto3>=1.26.0",
            "pulumi>=3.0.0",
            "pulumi-aws>=5.0.0",
        ],
        "local": [
            "localstack>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "metrics_pipeline=metrics_pipeline.cli.main:main",
        ],
    },
)
