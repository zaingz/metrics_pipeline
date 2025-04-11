"""Unit tests for HTTP ingestion adapter."""
import json
import pytest
from unittest.mock import MagicMock, patch

from metrics_pipeline.adapters.ingestion.http import HTTPIngestionAdapter


@pytest.fixture
def mock_requests():
    """Create a mock for the requests library."""
    with patch("requests.post") as mock_post, patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        
        mock_post.return_value = mock_response
        mock_get.return_value = mock_response
        
        yield {
            "post": mock_post,
            "get": mock_get,
            "response": mock_response
        }


@pytest.fixture
def http_adapter():
    """Create an HTTP adapter for testing."""
    return HTTPIngestionAdapter(
        api_url="https://api.example.com/metrics",
        headers={"Content-Type": "application/json", "X-API-Key": "test-key"},
        timeout=5,
        verify_ssl=False
    )


@pytest.mark.asyncio
async def test_ingest_valid_data(http_adapter, mock_requests):
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
    result = await http_adapter.ingest(metrics_data)
    
    # Assert
    assert result is True
    mock_requests["post"].assert_called_once_with(
        "https://api.example.com/metrics",
        headers={"Content-Type": "application/json", "X-API-Key": "test-key"},
        json=metrics_data,
        timeout=5,
        verify=False
    )


@pytest.mark.asyncio
async def test_ingest_invalid_data(http_adapter, mock_requests):
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
    result = await http_adapter.ingest(invalid_metrics_data)
    
    # Assert
    assert result is False
    mock_requests["post"].assert_not_called()


@pytest.mark.asyncio
async def test_ingest_request_exception(http_adapter, mock_requests):
    """Test ingestion when request raises an exception."""
    # Arrange
    from requests.exceptions import RequestException
    mock_requests["post"].side_effect = RequestException("Connection error")
    
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
    result = await http_adapter.ingest(metrics_data)
    
    # Assert
    assert result is False


@pytest.mark.asyncio
async def test_batch_ingest(http_adapter, mock_requests):
    """Test batch ingestion of metrics data."""
    # Arrange
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
    result = await http_adapter.batch_ingest(metrics_batch)
    
    # Assert
    assert result["success_count"] == 2
    assert result["failure_count"] == 0
    assert len(result["failures"]) == 0
    assert mock_requests["post"].call_count == 2


@pytest.mark.asyncio
async def test_batch_ingest_with_failures(http_adapter, mock_requests):
    """Test batch ingestion with some failures."""
    # Arrange
    from requests.exceptions import RequestException
    
    # Make the second call fail
    def side_effect(*args, **kwargs):
        if mock_requests["post"].call_count == 1:
            return mock_requests["response"]
        else:
            raise RequestException("Connection error")
    
    mock_requests["post"].side_effect = side_effect
    
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
    result = await http_adapter.batch_ingest(metrics_batch)
    
    # Assert
    assert result["success_count"] == 1
    assert result["failure_count"] == 1
    assert len(result["failures"]) == 1
    assert mock_requests["post"].call_count == 2


@pytest.mark.asyncio
async def test_validate_valid_data(http_adapter):
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
    result = await http_adapter.validate(valid_metrics_data)
    
    # Assert
    assert result["valid"] is True
    assert result["errors"] is None


@pytest.mark.asyncio
async def test_validate_invalid_data(http_adapter):
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
    result = await http_adapter.validate(invalid_metrics_data)
    
    # Assert
    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert "Missing required field: timestamp" in result["errors"]


@pytest.mark.asyncio
async def test_health_check_healthy(http_adapter, mock_requests):
    """Test health check when API is healthy."""
    # Arrange
    mock_requests["response"].status_code = 200
    
    # Act
    result = await http_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "api_url" in result["details"]
    assert result["details"]["api_url"] == "https://api.example.com/metrics"


@pytest.mark.asyncio
async def test_health_check_degraded(http_adapter, mock_requests):
    """Test health check when API is degraded."""
    # Arrange
    mock_requests["response"].status_code = 429  # Too Many Requests
    
    # Act
    result = await http_adapter.health_check()
    
    # Assert
    assert result["status"] == "degraded"
    assert "api_url" in result["details"]
    assert "status_code" in result["details"]
    assert result["details"]["status_code"] == 429


@pytest.mark.asyncio
async def test_health_check_unhealthy(http_adapter, mock_requests):
    """Test health check when API is unhealthy."""
    # Arrange
    from requests.exceptions import RequestException
    mock_requests["get"].side_effect = RequestException("Connection error")
    
    # Act
    result = await http_adapter.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "api_url" in result["details"]
    assert "error" in result["details"]
    assert "Connection error" in result["details"]["error"]
