"""Initialization file for the storage adapters."""
from metrics_pipeline.adapters.storage.base import StorageAdapter
from metrics_pipeline.adapters.storage.clickhouse import ClickHouseStorageAdapter
from metrics_pipeline.adapters.storage.memory import InMemoryStorageAdapter

__all__ = ["StorageAdapter", "ClickHouseStorageAdapter", "InMemoryStorageAdapter"]
