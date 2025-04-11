"""Unit tests for metrics pipeline processor."""
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from metrics_pipeline.core.pipeline.processor import MetricsPipeline
from metrics_pipeline.core.models.metrics import Metric, MetricsData


@pytest.fixture
def mock_ingestion_adapter():
    """Create a mock ingestion adapter."""
    adapter = AsyncMock()
    adapter.ingest = AsyncMock(return_value=True)
    adapter.batch_ingest = AsyncMock(return_value={"success_count": 2, "failure_count": 0, "failures": []})
    adapter.validate = AsyncMock(return_value={"valid": True, "errors": None})
    adapter.health_check = AsyncMock(return_value={"status": "healthy", "details": {}})
    return adapter


@pytest.fixture
def mock_storage_adapter():
    """Create a mock storage adapter."""
    adapter = AsyncMock()
    adapter.store = AsyncMock(return_value=True)
    adapter.batch_store = AsyncMock(return_value={"success_count": 2, "failure_count": 0, "failures": []})
    adapter.query = AsyncMock(return_value=[])
    adapter.aggregate = AsyncMock(return_value={"value": 0})
    adapter.health_check = AsyncMock(return_value={"status": "healthy", "details": {}})
    return adapter


@pytest.fixture
def pipeline(mock_ingestion_adapter, mock_storage_adapter):
    """Create a metrics pipeline with mock adapters."""
    return MetricsPipeline(
        ingestion_adapter=mock_ingestion_adapter,
        storage_adapter=mock_storage_adapter,
        batch_size=10,
        processing_interval=0.1
    )


@pytest.fixture
def sample_metrics_data():
    """Create sample metrics data for testing."""
    return {
        "timestamp": "2025-04-10T10:00:00Z",
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
async def test_process_metrics(pipeline, mock_ingestion_adapter, mock_storage_adapter, sample_metrics_data):
    """Test processing a single metrics data item."""
    # Act
    result = await pipeline.process_metrics(sample_metrics_data)
    
    # Assert
    assert result is True
    mock_ingestion_adapter.validate.assert_called_once()
    mock_storage_adapter.store.assert_called_once()


@pytest.mark.asyncio
async def test_process_metrics_invalid_data(pipeline, mock_ingestion_adapter, mock_storage_adapter, sample_metrics_data):
    """Test processing invalid metrics data."""
    # Arrange
    mock_ingestion_adapter.validate.return_value = {"valid": False, "errors": ["Missing required field"]}
    
    # Act
    result = await pipeline.process_metrics(sample_metrics_data)
    
    # Assert
    assert result is False
    mock_ingestion_adapter.validate.assert_called_once()
    mock_storage_adapter.store.assert_not_called()


@pytest.mark.asyncio
async def test_process_metrics_storage_failure(pipeline, mock_ingestion_adapter, mock_storage_adapter, sample_metrics_data):
    """Test processing metrics with storage failure."""
    # Arrange
    mock_storage_adapter.store.return_value = False
    
    # Act
    result = await pipeline.process_metrics(sample_metrics_data)
    
    # Assert
    assert result is False
    mock_ingestion_adapter.validate.assert_called_once()
    mock_storage_adapter.store.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch(pipeline, mock_ingestion_adapter, mock_storage_adapter, sample_metrics_data):
    """Test processing a batch of metrics data."""
    # Arrange
    metrics_batch = [sample_metrics_data, sample_metrics_data]
    
    # Act
    result = await pipeline.process_batch(metrics_batch)
    
    # Assert
    assert result["success_count"] == 2
    assert result["failure_count"] == 0
    assert len(result["failures"]) == 0
    assert mock_ingestion_adapter.validate.call_count == 2
    mock_storage_adapter.batch_store.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch_with_invalid_data(pipeline, mock_ingestion_adapter, mock_storage_adapter, sample_metrics_data):
    """Test processing a batch with some invalid data."""
    # Arrange
    metrics_batch = [sample_metrics_data, sample_metrics_data]
    
    # Make the second validation fail
    validation_results = [
        {"valid": True, "errors": None},
        {"valid": False, "errors": ["Missing required field"]}
    ]
    mock_ingestion_adapter.validate.side_effect = validation_results
    
    # Act
    result = await pipeline.process_batch(metrics_batch)
    
    # Assert
    assert result["success_count"] == 1
    assert result["failure_count"] == 1
    assert len(result["failures"]) == 1
    assert "Missing required field" in result["failures"][0]["errors"][0]
    assert mock_ingestion_adapter.validate.call_count == 2
    
    # Only valid metrics should be stored
    valid_metrics = [MetricsData.parse_obj(sample_metrics_data)]
    mock_storage_adapter.batch_store.assert_called_once()


@pytest.mark.asyncio
async def test_start_stop_processing(pipeline, mock_ingestion_adapter, mock_storage_adapter):
    """Test starting and stopping the continuous processing."""
    # Patch the _process_queue method to avoid actual processing
    with patch.object(pipeline, '_process_queue', AsyncMock()) as mock_process:
        # Start processing
        await pipeline.start_processing()
        
        # Let it run for a short time
        await asyncio.sleep(0.2)
        
        # Stop processing
        await pipeline.stop_processing()
        
        # Assert
        assert mock_process.called
        assert pipeline._processing_task is None


@pytest.mark.asyncio
async def test_health_check(pipeline, mock_ingestion_adapter, mock_storage_adapter):
    """Test pipeline health check."""
    # Act
    result = await pipeline.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "ingestion" in result["details"]
    assert "storage" in result["details"]
    assert result["details"]["ingestion"]["status"] == "healthy"
    assert result["details"]["storage"]["status"] == "healthy"
    mock_ingestion_adapter.health_check.assert_called_once()
    mock_storage_adapter.health_check.assert_called_once()


@pytest.mark.asyncio
async def test_health_check_degraded(pipeline, mock_ingestion_adapter, mock_storage_adapter):
    """Test pipeline health check with degraded components."""
    # Arrange
    mock_ingestion_adapter.health_check.return_value = {"status": "degraded", "details": {"reason": "High latency"}}
    
    # Act
    result = await pipeline.health_check()
    
    # Assert
    assert result["status"] == "degraded"
    assert "ingestion" in result["details"]
    assert "storage" in result["details"]
    assert result["details"]["ingestion"]["status"] == "degraded"
    assert result["details"]["storage"]["status"] == "healthy"


@pytest.mark.asyncio
async def test_health_check_unhealthy(pipeline, mock_ingestion_adapter, mock_storage_adapter):
    """Test pipeline health check with unhealthy components."""
    # Arrange
    mock_storage_adapter.health_check.return_value = {"status": "unhealthy", "details": {"error": "Connection error"}}
    
    # Act
    result = await pipeline.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "ingestion" in result["details"]
    assert "storage" in result["details"]
    assert result["details"]["ingestion"]["status"] == "healthy"
    assert result["details"]["storage"]["status"] == "unhealthy"
