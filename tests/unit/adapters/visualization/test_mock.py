"""Unit tests for Mock visualization adapter."""
import json
import pytest
from datetime import datetime

from metrics_pipeline.adapters.visualization.mock import MockVisualizationAdapter


@pytest.fixture
def mock_adapter():
    """Create a mock visualization adapter for testing."""
    return MockVisualizationAdapter()


@pytest.mark.asyncio
async def test_connect(mock_adapter):
    """Test connecting to mock visualization backend."""
    # Act
    result = await mock_adapter.connect()
    
    # Assert
    assert result is True
    assert mock_adapter.connected is True


@pytest.mark.asyncio
async def test_create_dashboard(mock_adapter):
    """Test creating a dashboard in mock backend."""
    # Arrange
    await mock_adapter.connect()
    
    # Act
    result = await mock_adapter.create_dashboard(
        name="Test Dashboard",
        description="Test Description",
        metadata={"owner": "test-user"}
    )
    
    # Assert
    assert result["id"] == "dashboard_1"
    assert result["name"] == "Test Dashboard"
    assert result["description"] == "Test Description"
    assert result["metadata"] == {"owner": "test-user"}
    assert "url" in result
    assert result["url"] == "mock://dashboard/dashboard_1"
    
    # Check that the dashboard was stored
    assert "dashboard_1" in mock_adapter.dashboards
    assert mock_adapter.dashboards["dashboard_1"] == result


@pytest.mark.asyncio
async def test_create_visualization(mock_adapter):
    """Test creating a visualization in mock backend."""
    # Arrange
    await mock_adapter.connect()
    dashboard = await mock_adapter.create_dashboard(name="Test Dashboard")
    
    # Act
    result = await mock_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Test Visualization",
        visualization_type="line",
        query={"sql": "SELECT * FROM metrics"},
        description="Test Description",
        metadata={"creator": "test-user"}
    )
    
    # Assert
    assert result["id"] == "viz_1"
    assert result["dashboard_id"] == dashboard["id"]
    assert result["name"] == "Test Visualization"
    assert result["visualization_type"] == "line"
    assert result["query"] == {"sql": "SELECT * FROM metrics"}
    assert result["description"] == "Test Description"
    assert result["metadata"] == {"creator": "test-user"}
    assert "url" in result
    assert result["url"] == "mock://visualization/viz_1"
    
    # Check that the visualization was stored
    assert dashboard["id"] in mock_adapter.visualizations
    assert len(mock_adapter.visualizations[dashboard["id"]]) == 1
    assert mock_adapter.visualizations[dashboard["id"]][0] == result


@pytest.mark.asyncio
async def test_get_dashboards(mock_adapter):
    """Test getting dashboards from mock backend."""
    # Arrange
    await mock_adapter.connect()
    dashboard1 = await mock_adapter.create_dashboard(name="Dashboard 1")
    dashboard2 = await mock_adapter.create_dashboard(name="Dashboard 2")
    
    # Act
    result = await mock_adapter.get_dashboards()
    
    # Assert
    assert len(result) == 2
    assert result[0]["id"] == dashboard1["id"]
    assert result[0]["name"] == "Dashboard 1"
    assert result[1]["id"] == dashboard2["id"]
    assert result[1]["name"] == "Dashboard 2"


@pytest.mark.asyncio
async def test_get_visualizations(mock_adapter):
    """Test getting visualizations from a dashboard."""
    # Arrange
    await mock_adapter.connect()
    dashboard = await mock_adapter.create_dashboard(name="Test Dashboard")
    viz1 = await mock_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Visualization 1",
        visualization_type="line",
        query={"sql": "SELECT * FROM metrics"}
    )
    viz2 = await mock_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Visualization 2",
        visualization_type="bar",
        query={"sql": "SELECT * FROM metrics"}
    )
    
    # Act
    result = await mock_adapter.get_visualizations(dashboard["id"])
    
    # Assert
    assert len(result) == 2
    assert result[0]["id"] == viz1["id"]
    assert result[0]["name"] == "Visualization 1"
    assert result[1]["id"] == viz2["id"]
    assert result[1]["name"] == "Visualization 2"


@pytest.mark.asyncio
async def test_get_visualizations_nonexistent_dashboard(mock_adapter):
    """Test getting visualizations from a nonexistent dashboard."""
    # Arrange
    await mock_adapter.connect()
    
    # Act
    result = await mock_adapter.get_visualizations("nonexistent")
    
    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_export_dashboard_json(mock_adapter):
    """Test exporting a dashboard as JSON."""
    # Arrange
    await mock_adapter.connect()
    dashboard = await mock_adapter.create_dashboard(name="Test Dashboard")
    viz = await mock_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Test Visualization",
        visualization_type="line",
        query={"sql": "SELECT * FROM metrics"}
    )
    
    # Act
    result = await mock_adapter.export_dashboard(dashboard["id"], "json")
    
    # Assert
    assert result is not None
    assert len(result) > 0
    dashboard_data = json.loads(result.decode("utf-8"))
    assert "dashboard" in dashboard_data
    assert "visualizations" in dashboard_data
    assert dashboard_data["dashboard"]["id"] == dashboard["id"]
    assert dashboard_data["dashboard"]["name"] == "Test Dashboard"
    assert len(dashboard_data["visualizations"]) == 1
    assert dashboard_data["visualizations"][0]["id"] == viz["id"]


@pytest.mark.asyncio
async def test_export_dashboard_pdf(mock_adapter):
    """Test exporting a dashboard as PDF."""
    # Arrange
    await mock_adapter.connect()
    dashboard = await mock_adapter.create_dashboard(name="Test Dashboard")
    
    # Act
    result = await mock_adapter.export_dashboard(dashboard["id"], "pdf")
    
    # Assert
    assert result is not None
    assert len(result) > 0
    assert b"Mock PDF export" in result


@pytest.mark.asyncio
async def test_export_nonexistent_dashboard(mock_adapter):
    """Test exporting a nonexistent dashboard."""
    # Arrange
    await mock_adapter.connect()
    
    # Act
    result = await mock_adapter.export_dashboard("nonexistent", "json")
    
    # Assert
    assert result == b""


@pytest.mark.asyncio
async def test_health_check_connected(mock_adapter):
    """Test health check when connected."""
    # Arrange
    await mock_adapter.connect()
    dashboard = await mock_adapter.create_dashboard(name="Test Dashboard")
    
    # Act
    result = await mock_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "type" in result["details"]
    assert result["details"]["type"] == "mock"
    assert result["details"]["dashboards_count"] == 1
    assert result["details"]["visualizations_count"] == 0


@pytest.mark.asyncio
async def test_health_check_not_connected(mock_adapter):
    """Test health check when not connected."""
    # Act
    result = await mock_adapter.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "type" in result["details"]
    assert result["details"]["type"] == "mock"
    assert result["details"]["dashboards_count"] == 0
    assert result["details"]["visualizations_count"] == 0
