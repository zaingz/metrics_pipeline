"""Unit tests for metrics models."""
import pytest
from datetime import datetime
from typing import Dict, Any, List, Optional

from metrics_pipeline.core.models.metrics import Metric, MetricsData, MetricsValidationResult, MetricsBatchResult


def test_metric_creation():
    """Test creating a Metric object."""
    # Basic metric
    metric = Metric(name="load_time", value=1.23)
    assert metric.name == "load_time"
    assert metric.value == 1.23
    assert metric.unit is None
    assert metric.tags == {}
    
    # Metric with all fields
    metric = Metric(
        name="load_time",
        value=1.23,
        unit="seconds",
        tags={"page": "home", "device": "mobile"}
    )
    assert metric.name == "load_time"
    assert metric.value == 1.23
    assert metric.unit == "seconds"
    assert metric.tags == {"page": "home", "device": "mobile"}


def test_metrics_data_creation():
    """Test creating a MetricsData object."""
    # Basic metrics data
    timestamp = datetime(2025, 4, 10, 10, 0, 0)
    metrics = [
        Metric(name="load_time", value=1.23, unit="seconds"),
        Metric(name="user_count", value=1)
    ]
    
    metrics_data = MetricsData(
        timestamp=timestamp,
        metric_type="page_view",
        metrics=metrics
    )
    
    assert metrics_data.timestamp == timestamp
    assert metrics_data.metric_type == "page_view"
    assert len(metrics_data.metrics) == 2
    assert metrics_data.source is None
    assert metrics_data.context is None
    
    # Metrics data with all fields
    metrics_data = MetricsData(
        timestamp=timestamp,
        metric_type="page_view",
        metrics=metrics,
        source="web",
        context={"user_agent": "Mozilla/5.0"}
    )
    
    assert metrics_data.timestamp == timestamp
    assert metrics_data.metric_type == "page_view"
    assert len(metrics_data.metrics) == 2
    assert metrics_data.source == "web"
    assert metrics_data.context == {"user_agent": "Mozilla/5.0"}


def test_metrics_validation_result_creation():
    """Test creating a MetricsValidationResult object."""
    # Valid result
    result = MetricsValidationResult(valid=True)
    assert result.valid is True
    assert result.errors is None
    
    # Invalid result with errors
    errors = ["Missing required field", "Invalid value"]
    result = MetricsValidationResult(valid=False, errors=errors)
    assert result.valid is False
    assert result.errors == errors


def test_metrics_batch_result_creation():
    """Test creating a MetricsBatchResult object."""
    # Success only result
    result = MetricsBatchResult(success_count=10, failure_count=0)
    assert result.success_count == 10
    assert result.failure_count == 0
    assert result.failures == []
    
    # Result with failures
    failures = [
        {"data": {"metric_type": "page_view"}, "errors": ["Missing timestamp"]},
        {"data": {"timestamp": "2025-04-10"}, "errors": ["Missing metric_type"]}
    ]
    result = MetricsBatchResult(success_count=8, failure_count=2, failures=failures)
    assert result.success_count == 8
    assert result.failure_count == 2
    assert len(result.failures) == 2
    assert result.failures[0]["errors"][0] == "Missing timestamp"
    assert result.failures[1]["errors"][0] == "Missing metric_type"


def test_metrics_data_dict_conversion():
    """Test converting MetricsData to and from dict."""
    # Create metrics data
    timestamp = datetime(2025, 4, 10, 10, 0, 0)
    metrics = [
        Metric(name="load_time", value=1.23, unit="seconds", tags={"page": "home"}),
        Metric(name="user_count", value=1, tags={"user_type": "new"})
    ]
    
    metrics_data = MetricsData(
        timestamp=timestamp,
        metric_type="page_view",
        metrics=metrics,
        source="web",
        context={"user_agent": "Mozilla/5.0"}
    )
    
    # Convert to dict
    data_dict = metrics_data.dict()
    
    # Check dict values
    assert data_dict["timestamp"] == timestamp
    assert data_dict["metric_type"] == "page_view"
    assert len(data_dict["metrics"]) == 2
    assert data_dict["metrics"][0]["name"] == "load_time"
    assert data_dict["metrics"][0]["value"] == 1.23
    assert data_dict["metrics"][0]["unit"] == "seconds"
    assert data_dict["metrics"][0]["tags"] == {"page": "home"}
    assert data_dict["source"] == "web"
    assert data_dict["context"] == {"user_agent": "Mozilla/5.0"}
    
    # Convert back to MetricsData
    from pydantic import parse_obj_as
    new_metrics_data = parse_obj_as(MetricsData, data_dict)
    
    # Check values
    assert new_metrics_data.timestamp == timestamp
    assert new_metrics_data.metric_type == "page_view"
    assert len(new_metrics_data.metrics) == 2
    assert new_metrics_data.metrics[0].name == "load_time"
    assert new_metrics_data.metrics[0].value == 1.23
    assert new_metrics_data.metrics[0].unit == "seconds"
    assert new_metrics_data.metrics[0].tags == {"page": "home"}
    assert new_metrics_data.source == "web"
    assert new_metrics_data.context == {"user_agent": "Mozilla/5.0"}
