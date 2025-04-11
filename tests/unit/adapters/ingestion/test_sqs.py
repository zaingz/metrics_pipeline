"""Unit tests for SQS ingestion adapter."""
import json
import pytest
from unittest.mock import MagicMock, patch

from metrics_pipeline.adapters.ingestion.sqs import SQSIngestionAdapter


@pytest.fixture
def mock_sqs_client():
    """Create a mock SQS client."""
    with patch("boto3.client") as mock_client:
        mock_sqs = MagicMock()
        mock_client.return_value = mock_sqs
        mock_sqs.get_queue_url.return_value = {"QueueUrl": "https://sqs.example.com/queue"}
        yield mock_sqs


@pytest.fixture
def sqs_adapter(mock_sqs_client):
    """Create an SQS adapter with a mock client."""
    return SQSIngestionAdapter(
        queue_name="test-queue",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )


@pytest.mark.asyncio
async def test_ingest_valid_data(sqs_adapter, mock_sqs_client):
    """Test ingesting valid metrics data."""
    # Arrange
    metrics_data = {
        "timestamp": "2025-04-10T10:00:00Z",
        "metric_type": "page_view",
        "metrics": [
            {
                "name": "load_time",
                "value": 1.23,
                "unit": "seconds",
                "tags": {"page": "home"}
            }
        ]
    }
    
    # Act
    result = await sqs_adapter.ingest(metrics_data)
    
    # Assert
    assert result is True
    mock_sqs_client.send_message.assert_called_once()
    args, kwargs = mock_sqs_client.send_message.call_args
    assert kwargs["QueueUrl"] == "https://sqs.example.com/queue"
    assert json.loads(kwargs["MessageBody"]) == metrics_data


@pytest.mark.asyncio
async def test_ingest_invalid_data(sqs_adapter, mock_sqs_client):
    """Test ingesting invalid metrics data."""
    # Arrange
    invalid_metrics_data = {
        "timestamp": "2025-04-10T10:00:00Z",
        # Missing required field: metric_type
        "metrics": [
            {
                "name": "load_time",
                "value": 1.23
            }
        ]
    }
    
    # Act
    result = await sqs_adapter.ingest(invalid_metrics_data)
    
    # Assert
    assert result is False
    mock_sqs_client.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_batch_ingest(sqs_adapter, mock_sqs_client):
    """Test batch ingestion of metrics data."""
    # Arrange
    mock_sqs_client.send_message_batch.return_value = {
        "Successful": [{"Id": "0"}, {"Id": "1"}],
        "Failed": []
    }
    
    metrics_batch = [
        {
            "timestamp": "2025-04-10T10:00:00Z",
            "metric_type": "page_view",
            "metrics": [{"name": "load_time", "value": 1.23}]
        },
        {
            "timestamp": "2025-04-10T10:01:00Z",
            "metric_type": "click",
            "metrics": [{"name": "button_click", "value": 1}]
        }
    ]
    
    # Act
    result = await sqs_adapter.batch_ingest(metrics_batch)
    
    # Assert
    assert result["success_count"] == 2
    assert result["failure_count"] == 0
    assert len(result["failures"]) == 0
    mock_sqs_client.send_message_batch.assert_called_once()


@pytest.mark.asyncio
async def test_batch_ingest_with_failures(sqs_adapter, mock_sqs_client):
    """Test batch ingestion with some failures."""
    # Arrange
    mock_sqs_client.send_message_batch.return_value = {
        "Successful": [{"Id": "0"}],
        "Failed": [{"Id": "1", "Code": "InternalError", "Message": "Error"}]
    }
    
    metrics_batch = [
        {
            "timestamp": "2025-04-10T10:00:00Z",
            "metric_type": "page_view",
            "metrics": [{"name": "load_time", "value": 1.23}]
        },
        {
            "timestamp": "2025-04-10T10:01:00Z",
            "metric_type": "click",
            "metrics": [{"name": "button_click", "value": 1}]
        }
    ]
    
    # Act
    result = await sqs_adapter.batch_ingest(metrics_batch)
    
    # Assert
    assert result["success_count"] == 1
    assert result["failure_count"] == 1
    assert len(result["failures"]) == 1
    mock_sqs_client.send_message_batch.assert_called_once()


@pytest.mark.asyncio
async def test_validate_valid_data(sqs_adapter):
    """Test validation of valid metrics data."""
    # Arrange
    valid_metrics_data = {
        "timestamp": "2025-04-10T10:00:00Z",
        "metric_type": "page_view",
        "metrics": [
            {
                "name": "load_time",
                "value": 1.23,
                "unit": "seconds",
                "tags": {"page": "home"}
            }
        ]
    }
    
    # Act
    result = await sqs_adapter.validate(valid_metrics_data)
    
    # Assert
    assert result["valid"] is True
    assert result["errors"] is None


@pytest.mark.asyncio
async def test_validate_invalid_data(sqs_adapter):
    """Test validation of invalid metrics data."""
    # Arrange
    invalid_metrics_data = {
        # Missing timestamp
        "metric_type": "page_view",
        "metrics": [
            {
                # Missing value
                "name": "load_time"
            }
        ]
    }
    
    # Act
    result = await sqs_adapter.validate(invalid_metrics_data)
    
    # Assert
    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert "Missing required field: timestamp" in result["errors"]


@pytest.mark.asyncio
async def test_health_check_healthy(sqs_adapter, mock_sqs_client):
    """Test health check when SQS is healthy."""
    # Arrange
    mock_sqs_client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "0"}
    }
    
    # Act
    result = await sqs_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "queue_url" in result["details"]
    assert result["details"]["queue_url"] == "https://sqs.example.com/queue"


@pytest.mark.asyncio
async def test_health_check_unhealthy(sqs_adapter, mock_sqs_client):
    """Test health check when SQS is unhealthy."""
    # Arrange
    mock_sqs_client.get_queue_attributes.side_effect = Exception("Connection error")
    
    # Act
    result = await sqs_adapter.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "error" in result["details"]
    assert "Connection error" in result["details"]["error"]
