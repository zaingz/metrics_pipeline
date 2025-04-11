"""Unit tests for ClickHouse storage adapter."""
import json
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from metrics_pipeline.adapters.storage.clickhouse import ClickHouseStorageAdapter
from metrics_pipeline.core.models.metrics import Metric, MetricsData


@pytest.fixture
def mock_clickhouse_client():
    """Create a mock ClickHouse client."""
    with patch("clickhouse_driver.Client") as mock_client:
        client_instance = MagicMock()
        mock_client.return_value = client_instance
        yield client_instance


@pytest.fixture
def clickhouse_adapter(mock_clickhouse_client):
    """Create a ClickHouse adapter with a mock client."""
    return ClickHouseStorageAdapter(
        host="localhost",
        port=9000,
        user="default",
        password="default",
        database="metrics",
        table="metrics",
        create_table_if_not_exists=True
    )


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
async def test_store_metrics(clickhouse_adapter, mock_clickhouse_client, sample_metrics_data):
    """Test storing metrics data in ClickHouse."""
    # Arrange
    mock_clickhouse_client.execute.return_value = []
    
    # Act
    result = await clickhouse_adapter.store(sample_metrics_data)
    
    # Assert
    assert result is True
    mock_clickhouse_client.execute.assert_called()
    # Check that the second call is the INSERT
    args, kwargs = mock_clickhouse_client.execute.call_args_list[1]
    assert "INSERT INTO metrics" in args[0]
    assert len(args[1]) == 2  # Two metrics in the sample data


@pytest.mark.asyncio
async def test_batch_store_metrics(clickhouse_adapter, mock_clickhouse_client, sample_metrics_data):
    """Test batch storing metrics data in ClickHouse."""
    # Arrange
    mock_clickhouse_client.execute.return_value = []
    metrics_batch = [sample_metrics_data, sample_metrics_data]
    
    # Act
    result = await clickhouse_adapter.batch_store(metrics_batch)
    
    # Assert
    assert result["success_count"] == 2
    assert result["failure_count"] == 0
    assert len(result["failures"]) == 0
    mock_clickhouse_client.execute.assert_called()
    # Check that the second call is the INSERT
    args, kwargs = mock_clickhouse_client.execute.call_args_list[1]
    assert "INSERT INTO metrics" in args[0]
    assert len(args[1]) == 4  # Four metrics total (2 per sample data)


@pytest.mark.asyncio
async def test_batch_store_with_error(clickhouse_adapter, mock_clickhouse_client, sample_metrics_data):
    """Test batch storing with an error."""
    # Arrange
    from clickhouse_driver.errors import Error as ClickHouseError
    mock_clickhouse_client.execute.side_effect = ClickHouseError("Database error")
    metrics_batch = [sample_metrics_data, sample_metrics_data]
    
    # Act
    result = await clickhouse_adapter.batch_store(metrics_batch)
    
    # Assert
    assert result["success_count"] == 0
    assert result["failure_count"] == 2
    assert len(result["failures"]) == 2


@pytest.mark.asyncio
async def test_query_metrics(clickhouse_adapter, mock_clickhouse_client):
    """Test querying metrics from ClickHouse."""
    # Arrange
    mock_rows = [
        (
            datetime(2025, 4, 10, 10, 0, 0),  # timestamp
            "page_view",                      # metric_type
            "load_time",                      # metric_name
            1.23,                             # metric_value
            "seconds",                        # metric_unit
            {"page": "home"},                 # tags
            "web",                            # source
            '{"user_agent": "Mozilla/5.0"}'   # context
        ),
        (
            datetime(2025, 4, 10, 10, 0, 0),  # timestamp
            "page_view",                      # metric_type
            "user_count",                     # metric_name
            1,                                # metric_value
            "",                               # metric_unit
            {"user_type": "new"},             # tags
            "web",                            # source
            '{"user_agent": "Mozilla/5.0"}'   # context
        )
    ]
    mock_clickhouse_client.execute.return_value = mock_rows
    
    # Act
    result = await clickhouse_adapter.query(
        metric_type="page_view",
        metric_names=["load_time", "user_count"],
        start_time=datetime(2025, 4, 10, 0, 0, 0),
        end_time=datetime(2025, 4, 10, 23, 59, 59),
        tags={"page": "home"},
        limit=10,
        offset=0
    )
    
    # Assert
    assert len(result) == 1  # One MetricsData object (grouped by timestamp and type)
    assert result[0].metric_type == "page_view"
    assert len(result[0].metrics) == 2
    assert result[0].metrics[0].name in ["load_time", "user_count"]
    assert result[0].metrics[1].name in ["load_time", "user_count"]
    mock_clickhouse_client.execute.assert_called_once()


@pytest.mark.asyncio
async def test_aggregate_metrics(clickhouse_adapter, mock_clickhouse_client):
    """Test aggregating metrics from ClickHouse."""
    # Arrange
    mock_clickhouse_client.execute.return_value = [(10.5,)]  # Sum of values
    
    # Act
    result = await clickhouse_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="sum",
        start_time=datetime(2025, 4, 10, 0, 0, 0),
        end_time=datetime(2025, 4, 10, 23, 59, 59)
    )
    
    # Assert
    assert result["aggregation"] == "sum"
    assert result["metric_type"] == "page_view"
    assert result["metric_name"] == "load_time"
    assert result["value"] == 10.5
    mock_clickhouse_client.execute.assert_called_once()


@pytest.mark.asyncio
async def test_aggregate_metrics_with_group_by(clickhouse_adapter, mock_clickhouse_client):
    """Test aggregating metrics with group by."""
    # Arrange
    mock_clickhouse_client.execute.return_value = [
        ("mobile", 5.5),  # (device, sum)
        ("desktop", 8.2)
    ]
    
    # Act
    result = await clickhouse_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="sum",
        start_time=datetime(2025, 4, 10, 0, 0, 0),
        end_time=datetime(2025, 4, 10, 23, 59, 59),
        group_by=["metric_unit"]
    )
    
    # Assert
    assert result["aggregation"] == "sum"
    assert result["metric_type"] == "page_view"
    assert result["metric_name"] == "load_time"
    assert "results" in result
    assert len(result["results"]) == 2
    assert result["results"][0]["metric_unit"] == "mobile"
    assert result["results"][0]["value"] == 5.5
    assert result["results"][1]["metric_unit"] == "desktop"
    assert result["results"][1]["value"] == 8.2
    mock_clickhouse_client.execute.assert_called_once()


@pytest.mark.asyncio
async def test_health_check_healthy(clickhouse_adapter, mock_clickhouse_client):
    """Test health check when ClickHouse is healthy."""
    # Arrange
    mock_clickhouse_client.execute.return_value = [(1,)]
    
    # Act
    result = await clickhouse_adapter.health_check()
    
    # Assert
    assert result["status"] == "healthy"
    assert "host" in result["details"]
    assert result["details"]["host"] == "localhost"
    assert "port" in result["details"]
    assert result["details"]["port"] == 9000


@pytest.mark.asyncio
async def test_health_check_unhealthy(clickhouse_adapter, mock_clickhouse_client):
    """Test health check when ClickHouse is unhealthy."""
    # Arrange
    from clickhouse_driver.errors import Error as ClickHouseError
    mock_clickhouse_client.execute.side_effect = ClickHouseError("Connection error")
    
    # Act
    result = await clickhouse_adapter.health_check()
    
    # Assert
    assert result["status"] == "unhealthy"
    assert "error" in result["details"]
    assert "Connection error" in result["details"]["error"]
