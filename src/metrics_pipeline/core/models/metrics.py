"""Core metrics data models."""
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class Metric(BaseModel):
    """Individual metric data point."""
    
    name: str = Field(..., description="Name of the metric")
    value: float = Field(..., description="Value of the metric")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    tags: Dict[str, str] = Field(default_factory=dict, description="Tags associated with the metric")


class MetricsData(BaseModel):
    """Container for a set of metrics data."""
    
    timestamp: datetime = Field(..., description="Timestamp when metrics were collected")
    metric_type: str = Field(..., description="Type of metrics (e.g., 'page_view', 'click', 'add_to_cart')")
    metrics: List[Metric] = Field(..., description="List of metrics")
    source: Optional[str] = Field(None, description="Source of the metrics")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context information")
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        """Parse timestamp from string if needed."""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class MetricsValidationResult(BaseModel):
    """Result of metrics data validation."""
    
    valid: bool = Field(..., description="Whether the metrics data is valid")
    errors: Optional[List[str]] = Field(None, description="List of validation errors if any")


class MetricsBatchResult(BaseModel):
    """Result of batch metrics ingestion."""
    
    success_count: int = Field(..., description="Number of successfully ingested metrics")
    failure_count: int = Field(..., description="Number of failed metrics")
    failures: List[Dict[str, Any]] = Field(default_factory=list, description="Details of failed metrics")


class HealthCheckResult(BaseModel):
    """Result of adapter health check."""
    
    status: str = Field(..., description="Health status: 'healthy', 'degraded', or 'unhealthy'")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional health check details")
