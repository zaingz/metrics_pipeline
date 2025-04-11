"""Unit tests for Metabase visualization adapter."""
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from metrics_pipeline.adapters.visualization.metabase import MetabaseVisualizationAdapter


@pytest.fixture
def mock_requests():
    """Create a mock for the requests library."""
    with patch("requests.post") as mock_post, patch("requests.get") as mock_get, patch("requests.put") as mock_put:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        
        # Set up session response
        session_response = MagicMock()
        session_response.status_code = 200
        session_response.raise_for_status = MagicMock()
        session_response.json.return_value = {"id": "test-session-token"}
        
        mock_post.return_value = session_response
        mock_get.return_value = mock_response
        mock_put.return_value = mock_response
        
        yield {
            "post": mock_post,
            "get": mock_get,
            "put": mock_put,
            "response": mock_response
        }


@pytest.fixture
def metabase_adapter():
    """Create a Metabase adapter for testing."""
    return MetabaseVisualizationAdapter(
        url="http://localhost:3000",
        username="admin@example.com",
        password="password",
        database_id=1
    )


@pytest.mark.asyncio
async def test_connect(metabase_adapter, mock_requests):
    """Test connecting to Metabase."""
    # Arrange
    mock_requests["get"].return_value.json.return_value = [{"id": 1, "name": "ClickHouse"}]
    
    # Act
    result = await metabase_adapter.connect()
    
    # Assert
    assert result is True
    assert metabase_adapter.session_token == "test-session-token"
    mock_requests["post"].assert_called_once_with(
        "http://localhost:3000/api/session",
        json={
            "username": "admin@example.com",
            "password": "password"
        }
    )


@pytest.mark.asyncio
async def test_connect_failure(metabase_adapter, mock_requests):
    """Test connection failure to Metabase."""
    # Arrange
    from requests.exceptions import RequestException
    mock_requests["post"].side_effect = RequestException("Connection error")
    
    # Act
    result = await metabase_adapter.connect()
    
    # Assert
    assert result is False
    assert metabase_adapter.session_token is None


@pytest.mark.asyncio
async def test_create_dashboard(metabase_adapter, mock_requests):
    """Test creating a dashboard in Metabase."""
    # Arrange
    mock_requests["post"].return_value.json.return_value = {
        "id": 123,
        "name": "Test Dashboard",
        "description": "Test Description"
    }
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.create_dashboard(
        name="Test Dashboard",
        description="Test Description",
        metadata={"owner": "test-user"}
    )
    
    # Assert
    assert result["id"] == 123
    assert result["name"] == "Test Dashboard"
    assert "url" in result
    assert result["url"] == "http://localhost:3000/dashboard/123"
    
    # Check that the dashboard creation API was called
    mock_requests["post"].assert_called_with(
        "http://localhost:3000/api/dashboard",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"},
        json={
            "name": "Test Dashboard",
            "description": "Test Description",
            "parameters": [],
            "collection_id": None
        }
    )


@pytest.mark.asyncio
async def test_create_visualization(metabase_adapter, mock_requests):
    """Test creating a visualization in Metabase."""
    # Arrange
    # First response for card creation
    card_response = MagicMock()
    card_response.status_code = 200
    card_response.json.return_value = {
        "id": 456,
        "name": "Test Visualization",
        "description": "Test Description"
    }
    
    # Second response for adding card to dashboard
    dashboard_card_response = MagicMock()
    dashboard_card_response.status_code = 200
    dashboard_card_response.json.return_value = {
        "id": 789,
        "dashboard_id": 123,
        "card_id": 456
    }
    
    mock_requests["post"].side_effect = [card_response, dashboard_card_response]
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.create_visualization(
        dashboard_id="123",
        name="Test Visualization",
        visualization_type="line",
        query={
            "sql": "SELECT * FROM metrics",
            "dimensions": ["timestamp"],
            "metrics": ["value"]
        },
        description="Test Description"
    )
    
    # Assert
    assert result["id"] == 456
    assert result["dashboard_id"] == "123"
    assert result["dashboard_card_id"] == 789
    assert result["name"] == "Test Visualization"
    assert "url" in result
    assert result["url"] == "http://localhost:3000/question/456"
    
    # Check that the card creation API was called
    assert mock_requests["post"].call_count == 2
    card_call_args = mock_requests["post"].call_args_list[0]
    assert "http://localhost:3000/api/card" in card_call_args[0]
    
    # Check that the dashboard card API was called
    dashboard_call_args = mock_requests["post"].call_args_list[1]
    assert "http://localhost:3000/api/dashboard/123/cards" in dashboard_call_args[0]


@pytest.mark.asyncio
async def test_get_dashboards(metabase_adapter, mock_requests):
    """Test getting dashboards from Metabase."""
    # Arrange
    mock_requests["get"].return_value.json.return_value = [
        {"id": 123, "name": "Dashboard 1"},
        {"id": 456, "name": "Dashboard 2"}
    ]
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.get_dashboards()
    
    # Assert
    assert len(result) == 2
    assert result[0]["id"] == 123
    assert result[0]["name"] == "Dashboard 1"
    assert result[0]["url"] == "http://localhost:3000/dashboard/123"
    assert result[1]["id"] == 456
    assert result[1]["name"] == "Dashboard 2"
    assert result[1]["url"] == "http://localhost:3000/dashboard/456"
    
    # Check that the dashboards API was called
    mock_requests["get"].assert_called_with(
        "http://localhost:3000/api/dashboard",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"}
    )


@pytest.mark.asyncio
async def test_get_visualizations(metabase_adapter, mock_requests):
    """Test getting visualizations from a dashboard."""
    # Arrange
    mock_requests["get"].return_value.json.return_value = {
        "id": 123,
        "name": "Test Dashboard",
        "ordered_cards": [
            {
                "id": 789,
                "card": {
                    "id": 456,
                    "name": "Test Visualization",
                    "description": "Test Description"
                }
            },
            {
                "id": 790,
                "card": {
                    "id": 457,
                    "name": "Another Visualization",
                    "description": "Another Description"
                }
            }
        ]
    }
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.get_visualizations("123")
    
    # Assert
    assert len(result) == 2
    assert result[0]["id"] == 456
    assert result[0]["name"] == "Test Visualization"
    assert result[0]["dashboard_id"] == "123"
    assert result[0]["dashboard_card_id"] == 789
    assert result[0]["url"] == "http://localhost:3000/question/456"
    
    assert result[1]["id"] == 457
    assert result[1]["name"] == "Another Visualization"
    assert result[1]["dashboard_id"] == "123"
    assert result[1]["dashboard_card_id"] == 790
    assert result[1]["url"] == "http://localhost:3000/question/457"
    
    # Check that the dashboard API was called
    mock_requests["get"].assert_called_with(
        "http://localhost:3000/api/dashboard/123",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"}
    )


@pytest.mark.asyncio
async def test_export_dashboard_json(metabase_adapter, mock_requests):
    """Test exporting a dashboard as JSON."""
    # Arrange
    mock_requests["get"].return_value.json.return_value = {
        "id": 123,
        "name": "Test Dashboard",
        "ordered_cards": []
    }
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.export_dashboard("123", "json")
    
    # Assert
    assert result is not None
    assert len(result) > 0
    dashboard_data = json.loads(result.decode("utf-8"))
    assert dashboard_data["id"] == 123
    assert dashboard_data["name"] == "Test Dashboard"
    
    # Check that the dashboard API was called
    mock_requests["get"].assert_called_with(
        "http://localhost:3000/api/dashboard/123",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"}
    )


@pytest.mark.asyncio
async def test_export_dashboard_pdf(metabase_adapter, mock_requests):
    """Test exporting a dashboard as PDF."""
    # Arrange
    mock_requests["post"].return_value.content = b"PDF content"
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.export_dashboard("123", "pdf")
    
    # Assert
    assert result == b"PDF content"
    
    # Check that the export API was called
    mock_requests["post"].assert_called_with(
        "http://localhost:3000/api/dashboard/123/export",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"},
        json={"format": "pdf"}
    )


@pytest.mark.asyncio
async def test_health_check_healthy(metabase_adapter, mock_requests):
    """Test health check when Metabase is healthy."""
    # Arrange
    mock_requests["get"].return_value.status_code = 200
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "url" in result["details"]
    assert result["details"]["url"] == "http://localhost:3000"
    
    # Check that the dashboard API was called for health check
    mock_requests["get"].assert_called_with(
        "http://localhost:3000/api/dashboard",
        headers={"Content-Type": "application/json", "X-Metabase-Session": "test-session-token"}
    )


@pytest.mark.asyncio
async def test_health_check_degraded(metabase_adapter, mock_requests):
    """Test health check when Metabase is degraded."""
    # Arrange
    mock_requests["get"].return_value.status_code = 429  # Too Many Requests
    
    # Ensure connect returns True
    metabase_adapter.session_token = "test-session-token"
    metabase_adapter.session_expiry = datetime.now().timestamp() + 3600
    
    # Act
    result = await metabase_adapter.health_check()
    
    # Assert
    assert result["status"] == "degraded"
    assert "url" in result["details"]
    assert "status_code" in result["details"]
    assert result["details"]["status_code"] == 429


@pytest.mark.asyncio
async def test_health_check_unhealthy(metabase_adapter, mock_requests):
    """Test health check when Metabase is unhealthy."""
    # Arrange
    from requests.exceptions import RequestException
    mock_requests["post"].side_effect = RequestException("Connection error")
    
    # Act
    result = await metabase_adapter.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "url" in result["details"]
    assert "error" in result["details"]
    assert "Connection error" in result["details"]["error"]
