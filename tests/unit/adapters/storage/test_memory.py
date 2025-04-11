"""Unit tests for InMemory storage adapter."""
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

from metrics_pipeline.adapters.storage.memory import InMemoryStorageAdapter
from metrics_pipeline.core.models.metrics import Metric, MetricsData


@pytest.fixture
def memory_adapter():
    """Create an in-memory storage adapter for testing."""
    return InMemoryStorageAdapter()


@pytest.fixture
def sample_metrics_data():
    """Create sample metrics data for testing."""
    return MetricsData(
        timestamp=datetime(2025, 4, 10, 10, 0, 0),
        metric_type="page_view",
        metrics=[
            Metric(
                name="load_time",
                value=1.23,
                unit="seconds",
                tags={"page": "home", "device": "mobile"}
            ),
            Metric(
                name="user_count",
                value=1,
                tags={"user_type": "new"}
            )
        ],
        source="web",
        context={"user_agent": "Mozilla/5.0"}
    )


@pytest.mark.asyncio
async def test_store_metrics(memory_adapter, sample_metrics_data):
    """Test storing metrics data in memory."""
    # Act
    result = await memory_adapter.store(sample_metrics_data)
    
    # Assert
    assert result is True
    assert len(memory_adapter.storage) == 1
    assert memory_adapter.storage[0] == sample_metrics_data


@pytest.mark.asyncio
async def test_batch_store_metrics(memory_adapter, sample_metrics_data):
    """Test batch storing metrics data in memory."""
    # Arrange
    metrics_batch = [
        sample_metrics_data,
        MetricsData(
            timestamp=datetime(2025, 4, 10, 10, 1, 0),
            metric_type="click",
            metrics=[
                Metric(
                    name="button_click",
                    value=1,
                    tags={"button": "submit", "page": "checkout"}
                )
            ]
        )
    ]
    
    # Act
    result = await memory_adapter.batch_store(metrics_batch)
    
    # Assert
    assert result["success_count"] == 2
    assert result["failure_count"] == 0
    assert len(result["failures"]) == 0
    assert len(memory_adapter.storage) == 2


@pytest.mark.asyncio
async def test_query_metrics(memory_adapter, sample_metrics_data):
    """Test querying metrics from memory."""
    # Arrange
    await memory_adapter.store(sample_metrics_data)
    
    # Add another metrics data with different type
    click_metrics = MetricsData(
        timestamp=datetime(2025, 4, 10, 10, 1, 0),
        metric_type="click",
        metrics=[
            Metric(
                name="button_click",
                value=1,
                tags={"button": "submit", "page": "checkout"}
            )
        ]
    )
    await memory_adapter.store(click_metrics)
    
    # Act
    result = await memory_adapter.query(
        metric_type="page_view",
        metric_names=["load_time"],
        start_time=datetime(2025, 4, 10, 0, 0, 0),
        end_time=datetime(2025, 4, 10, 23, 59, 59),
        tags={"device": "mobile"}
    )
    
    # Assert
    assert len(result) == 1
    assert result[0].metric_type == "page_view"
    assert any(metric.name == "load_time" for metric in result[0].metrics)


@pytest.mark.asyncio
async def test_query_metrics_with_time_filter(memory_adapter, sample_metrics_data):
    """Test querying metrics with time filter."""
    # Arrange
    await memory_adapter.store(sample_metrics_data)
    
    # Add another metrics data with different timestamp
    future_metrics = MetricsData(
        timestamp=datetime(2025, 4, 11, 10, 0, 0),  # Next day
        metric_type="page_view",
        metrics=[
            Metric(
                name="load_time",
                value=0.95,
                unit="seconds",
                tags={"page": "home", "device": "mobile"}
            )
        ]
    )
    await memory_adapter.store(future_metrics)
    
    # Act - Query only today's metrics
    result = await memory_adapter.query(
        start_time=datetime(2025, 4, 10, 0, 0, 0),
        end_time=datetime(2025, 4, 10, 23, 59, 59)
    )
    
    # Assert
    assert len(result) == 1
    assert result[0].timestamp == datetime(2025, 4, 10, 10, 0, 0)


@pytest.mark.asyncio
async def test_query_metrics_with_pagination(memory_adapter):
    """Test querying metrics with pagination."""
    # Arrange
    # Add 5 metrics data entries
    for i in range(5):
        metrics_data = MetricsData(
            timestamp=datetime(2025, 4, 10, 10, i, 0),
            metric_type="page_view",
            metrics=[
                Metric(
                    name="load_time",
                    value=1.0 + i * 0.1,
                    unit="seconds",
                    tags={"page": f"page_{i}"}
                )
            ]
        )
        await memory_adapter.store(metrics_data)
    
    # Act - Query with limit and offset
    result1 = await memory_adapter.query(limit=2, offset=0)
    result2 = await memory_adapter.query(limit=2, offset=2)
    
    # Assert
    assert len(result1) == 2
    assert len(result2) == 2
    # Results should be sorted by timestamp descending
    assert result1[0].timestamp > result1[1].timestamp
    assert result2[0].timestamp > result2[1].timestamp
    # First result of second page should be different from first page
    assert result1[0].timestamp != result2[0].timestamp


@pytest.mark.asyncio
async def test_aggregate_metrics_sum(memory_adapter):
    """Test aggregating metrics with sum."""
    # Arrange
    # Add metrics with the same name but different values
    for i in range(3):
        metrics_data = MetricsData(
            timestamp=datetime(2025, 4, 10, 10, i, 0),
            metric_type="page_view",
            metrics=[
                Metric(
                    name="load_time",
                    value=1.0 + i,
                    unit="seconds",
                    tags={"page": "home"}
                )
            ]
        )
        await memory_adapter.store(metrics_data)
    
    # Act
    result = await memory_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="sum"
    )
    
    # Assert
    assert result["aggregation"] == "sum"
    assert result["metric_type"] == "page_view"
    assert result["metric_name"] == "load_time"
    assert result["value"] == 6.0  # 1.0 + 2.0 + 3.0


@pytest.mark.asyncio
async def test_aggregate_metrics_avg(memory_adapter):
    """Test aggregating metrics with average."""
    # Arrange
    # Add metrics with the same name but different values
    for i in range(3):
        metrics_data = MetricsData(
            timestamp=datetime(2025, 4, 10, 10, i, 0),
            metric_type="page_view",
            metrics=[
                Metric(
                    name="load_time",
                    value=1.0 + i,
                    unit="seconds",
                    tags={"page": "home"}
                )
            ]
        )
        await memory_adapter.store(metrics_data)
    
    # Act
    result = await memory_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="avg"
    )
    
    # Assert
    assert result["aggregation"] == "avg"
    assert result["metric_type"] == "page_view"
    assert result["metric_name"] == "load_time"
    assert result["value"] == 2.0  # (1.0 + 2.0 + 3.0) / 3


@pytest.mark.asyncio
async def test_aggregate_metrics_with_group_by(memory_adapter):
    """Test aggregating metrics with group by."""
    # Arrange
    # Add metrics with different devices
    devices = ["mobile", "desktop", "tablet"]
    for i, device in enumerate(devices):
        metrics_data = MetricsData(
            timestamp=datetime(2025, 4, 10, 10, i, 0),
            metric_type="page_view",
            metrics=[
                Metric(
                    name="load_time",
                    value=1.0 + i,
                    unit="seconds",
                    tags={"device": device}
                )
            ],
            source=device
        )
        await memory_adapter.store(metrics_data)
    
    # Act
    result = await memory_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="sum",
        group_by=["source"]
    )
    
    # Assert
    assert result["aggregation"] == "sum"
    assert "results" in result
    assert len(result["results"]) == 3
    
    # Check each device has its own result
    device_values = {r["source"]: r["value"] for r in result["results"]}
    assert "mobile" in device_values
    assert "desktop" in device_values
    assert "tablet" in device_values
    assert device_values["mobile"] == 1.0
    assert device_values["desktop"] == 2.0
    assert device_values["tablet"] == 3.0


@pytest.mark.asyncio
async def test_health_check(memory_adapter, sample_metrics_data):
    """Test health check of in-memory adapter."""
    # Arrange
    await memory_adapter.store(sample_metrics_data)
    
    # Act
    result = await memory_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "storage_type" in result["details"]
    assert result["details"]["storage_type"] == "in-memory"
    assert result["details"]["metrics_count"] == 1
