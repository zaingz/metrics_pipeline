"""Initialization file for the ingestion adapters."""
from metrics_pipeline.adapters.ingestion.base import IngestionAdapter
from metrics_pipeline.adapters.ingestion.sqs import SQSIngestionAdapter
from metrics_pipeline.adapters.ingestion.http import HTTPIngestionAdapter

__all__ = ["IngestionAdapter", "SQSIngestionAdapter", "HTTPIngestionAdapter"]
