"""Integration tests for the metrics pipeline."""
import pytest
import asyncio
from datetime import datetime, timedelta
import os
import json

from metrics_pipeline.adapters.ingestion.http import HTTPIngestionAdapter
from metrics_pipeline.adapters.storage.memory import InMemoryStorageAdapter
from metrics_pipeline.adapters.visualization.mock import MockVisualizationAdapter
from metrics_pipeline.core.pipeline.processor import MetricsPipeline
from metrics_pipeline.core.models.metrics import Metric, MetricsData


@pytest.fixture
def http_adapter():
    """Create an HTTP adapter for testing."""
    # Use a mock HTTP endpoint for testing
    return HTTPIngestionAdapter(
        api_url="http://localhost:8000/metrics",
        headers={"Content-Type": "application/json"},
        timeout=5
    )


@pytest.fixture
def memory_adapter():
    """Create an in-memory storage adapter for testing."""
    return InMemoryStorageAdapter()


@pytest.fixture
def mock_viz_adapter():
    """Create a mock visualization adapter for testing."""
    return MockVisualizationAdapter()


@pytest.fixture
def pipeline(http_adapter, memory_adapter):
    """Create a metrics pipeline with real adapters."""
    return MetricsPipeline(
        ingestion_adapter=http_adapter,
        storage_adapter=memory_adapter,
        batch_size=10,
        processing_interval=0.1
    )


@pytest.fixture
def sample_metrics_data():
    """Create sample metrics data for testing."""
    return {
        "timestamp": datetime.now().isoformat(),
        "metric_type": "page_view",
        "metrics": [
            {
                "name": "load_time",
                "value": 1.23,
                "unit": "seconds",
                "tags": {"page": "home", "device": "mobile"}
            },
            {
                "name": "user_count",
                "value": 1,
                "tags": {"user_type": "new"}
            }
        ],
        "source": "web",
        "context": {"user_agent": "Mozilla/5.0"}
    }


@pytest.mark.asyncio
async def test_end_to_end_flow(memory_adapter, mock_viz_adapter):
    """Test the end-to-end flow from ingestion to visualization."""
    # 1. Store metrics directly in the storage adapter
    metrics_data = MetricsData(
        timestamp=datetime.now(),
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
    
    await memory_adapter.store(metrics_data)
    
    # 2. Query the stored metrics
    query_result = await memory_adapter.query(
        metric_type="page_view",
        start_time=datetime.now() - timedelta(hours=1),
        end_time=datetime.now() + timedelta(hours=1)
    )
    
    assert len(query_result) == 1
    assert query_result[0].metric_type == "page_view"
    assert len(query_result[0].metrics) == 2
    
    # 3. Create a dashboard and visualization
    await mock_viz_adapter.connect()
    dashboard = await mock_viz_adapter.create_dashboard(
        name="Test Dashboard",
        description="Integration Test Dashboard"
    )
    
    visualization = await mock_viz_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Page Load Time",
        visualization_type="line",
        query={
            "sql": "SELECT timestamp, metric_value FROM metrics WHERE metric_name = 'load_time'",
            "dimensions": ["timestamp"],
            "metrics": ["metric_value"]
        }
    )
    
    # 4. Verify the dashboard and visualization were created
    dashboards = await mock_viz_adapter.get_dashboards()
    assert len(dashboards) == 1
    assert dashboards[0]["id"] == dashboard["id"]
    
    visualizations = await mock_viz_adapter.get_visualizations(dashboard["id"])
    assert len(visualizations) == 1
    assert visualizations[0]["id"] == visualization["id"]
    
    # 5. Export the dashboard
    export_data = await mock_viz_adapter.export_dashboard(dashboard["id"], "json")
    export_json = json.loads(export_data.decode("utf-8"))
    
    assert "dashboard" in export_json
    assert export_json["dashboard"]["id"] == dashboard["id"]
    assert "visualizations" in export_json
    assert len(export_json["visualizations"]) == 1


@pytest.mark.asyncio
async def test_pipeline_batch_processing(memory_adapter):
    """Test batch processing of metrics through the pipeline."""
    # Create a pipeline with memory adapter
    pipeline = MetricsPipeline(
        ingestion_adapter=HTTPIngestionAdapter(
            api_url="http://localhost:8000/metrics",
            headers={"Content-Type": "application/json"}
        ),
        storage_adapter=memory_adapter,
        batch_size=10,
        processing_interval=0.1
    )
    
    # Create a batch of metrics
    metrics_batch = []
    for i in range(5):
        metrics_data = {
            "timestamp": (datetime.now() + timedelta(minutes=i)).isoformat(),
            "metric_type": "page_view",
            "metrics": [
                {
                    "name": "load_time",
                    "value": 1.0 + i * 0.1,
                    "unit": "seconds",
                    "tags": {"page": f"page_{i}"}
                }
            ],
            "source": "web"
        }
        metrics_batch.append(metrics_data)
    
    # Mock the ingestion adapter's validate method to always return valid
    pipeline.ingestion_adapter.validate = lambda data: {"valid": True, "errors": None}
    
    # Process the batch
    result = await pipeline.process_batch(metrics_batch)
    
    # Verify all metrics were processed successfully
    assert result["success_count"] == 5
    assert result["failure_count"] == 0
    
    # Query the storage to verify metrics were stored
    query_result = await memory_adapter.query(
        metric_type="page_view",
        start_time=datetime.now() - timedelta(hours=1),
        end_time=datetime.now() + timedelta(hours=1)
    )
    
    assert len(query_result) == 5


@pytest.mark.asyncio
async def test_metrics_aggregation(memory_adapter):
    """Test metrics aggregation functionality."""
    # Store metrics with different devices
    devices = ["mobile", "desktop", "tablet", "mobile"]
    for i, device in enumerate(devices):
        metrics_data = MetricsData(
            timestamp=datetime.now() - timedelta(minutes=i),
            metric_type="page_view",
            metrics=[
                Metric(
                    name="load_time",
                    value=1.0 + i * 0.5,
                    unit="seconds",
                    tags={"device": device}
                )
            ],
            source="web"
        )
        await memory_adapter.store(metrics_data)
    
    # Aggregate by device
    result = await memory_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="avg",
        group_by=["tags.device"]
    )
    
    # Verify aggregation results
    assert result["aggregation"] == "avg"
    assert "results" in result
    
    # Convert results to a dict for easier verification
    device_values = {r["tags.device"]: r["value"] for r in result["results"]}
    
    # Check each device has the correct average
    assert "mobile" in device_values
    assert "desktop" in device_values
    assert "tablet" in device_values
    
    # Mobile should have 2 entries: (1.0 + 2.5) / 2 = 1.75
    assert abs(device_values["mobile"] - 1.75) < 0.01
    
    # Desktop should have 1 entry: 1.5
    assert abs(device_values["desktop"] - 1.5) < 0.01
    
    # Tablet should have 1 entry: 2.0
    assert abs(device_values["tablet"] - 2.0) < 0.01


@pytest.mark.asyncio
async def test_health_check_integration(pipeline, memory_adapter, mock_viz_adapter):
    """Test health check integration across components."""
    # Connect visualization adapter
    await mock_viz_adapter.connect()
    
    # Check health of all components
    pipeline_health = await pipeline.health_check()
    storage_health = await memory_adapter.health_check()
    viz_health = await mock_viz_adapter.health_check()
    
    # Verify health status
    assert pipeline_health["status"] in ["healthy", "degraded", "unhealthy"]
    assert storage_health["status"] == "healthy"
    assert viz_health["status"] == "healthy"
    
    # Verify pipeline health includes component health
    assert "ingestion" in pipeline_health["details"]
    assert "storage" in pipeline_health["details"]
