# API Reference

This document provides detailed information about the APIs exposed by the Metrics Pipeline components.

## Core Models

### Metric

Represents a single metric data point.

```python
class Metric(BaseModel):
    name: str                      # Name of the metric
    value: float                   # Value of the metric
    unit: Optional[str] = None     # Unit of measurement
    tags: Dict[str, str] = {}      # Tags associated with the metric
```

### MetricsData

Container for a set of metrics data.

```python
class MetricsData(BaseModel):
    timestamp: datetime            # Timestamp when metrics were collected
    metric_type: str               # Type of metrics (e.g., 'page_view', 'click')
    metrics: List[Metric]          # List of metrics
    source: Optional[str] = None   # Source of the metrics
    context: Optional[Dict[str, Any]] = None  # Additional context information
```

### MetricsValidationResult

Result of metrics data validation.

```python
class MetricsValidationResult(BaseModel):
    valid: bool                    # Whether the metrics data is valid
    errors: Optional[List[str]] = None  # List of validation errors if any
```

### MetricsBatchResult

Result of batch metrics ingestion.

```python
class MetricsBatchResult(BaseModel):
    success_count: int             # Number of successfully ingested metrics
    failure_count: int             # Number of failed metrics
    failures: List[Dict[str, Any]] = []  # Details of failed metrics
```

### HealthCheckResult

Result of adapter health check.

```python
class HealthCheckResult(BaseModel):
    status: str                    # Health status: 'healthy', 'degraded', or 'unhealthy'
    details: Dict[str, Any] = {}   # Additional health check details
```

## Adapter Interfaces

### IngestionAdapter

Base interface for all ingestion adapters.

```python
class IngestionAdapter(ABC):
    @abstractmethod
    async def ingest(self, metrics_data: Dict[str, Any]) -> bool:
        """Ingest metrics data into the pipeline."""
        pass
    
    @abstractmethod
    async def batch_ingest(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Ingest a batch of metrics data into the pipeline."""
        pass
    
    @abstractmethod
    async def validate(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate metrics data before ingestion."""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the ingestion adapter."""
        pass
```

### StorageAdapter

Base interface for all storage adapters.

```python
class StorageAdapter(ABC):
    @abstractmethod
    async def store(self, metrics_data: MetricsData) -> bool:
        """Store metrics data in the storage backend."""
        pass
    
    @abstractmethod
    async def batch_store(self, metrics_batch: List[MetricsData]) -> Dict[str, Any]:
        """Store a batch of metrics data in the storage backend."""
        pass
    
    @abstractmethod
    async def query(
        self,
        metric_type: Optional[str] = None,
        metric_names: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        tags: Optional[Dict[str, str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[MetricsData]:
        """Query metrics data from the storage backend."""
        pass
    
    @abstractmethod
    async def aggregate(
        self,
        metric_type: str,
        metric_name: str,
        aggregation: str,  # 'sum', 'avg', 'min', 'max', 'count'
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        group_by: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Aggregate metrics data from the storage backend."""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the storage adapter."""
        pass
```

### VisualizationAdapter

Base interface for all visualization adapters.

```python
class VisualizationAdapter(ABC):
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the visualization backend."""
        pass
    
    @abstractmethod
    async def create_dashboard(
        self, 
        name: str, 
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new dashboard in the visualization backend."""
        pass
    
    @abstractmethod
    async def create_visualization(
        self,
        dashboard_id: str,
        name: str,
        visualization_type: str,  # 'line', 'bar', 'pie', 'table', etc.
        query: Dict[str, Any],
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new visualization in the specified dashboard."""
        pass
    
    @abstractmethod
    async def get_dashboards(self) -> List[Dict[str, Any]]:
        """Get all dashboards from the visualization backend."""
        pass
    
    @abstractmethod
    async def get_visualizations(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """Get all visualizations for the specified dashboard."""
        pass
    
    @abstractmethod
    async def export_dashboard(self, dashboard_id: str, format: str) -> bytes:
        """Export a dashboard in the specified format."""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the visualization adapter."""
        pass
```

## MetricsPipeline

Main pipeline class for processing metrics data.

```python
class MetricsPipeline:
    def __init__(
        self,
        ingestion_adapter: IngestionAdapter,
        storage_adapter: StorageAdapter,
        batch_size: int = 100,
        processing_interval: float = 5.0
    ):
        """Initialize the metrics pipeline."""
        pass
    
    async def process_metrics(self, metrics_data: Dict[str, Any]) -> bool:
        """Process a single metrics data item."""
        pass
    
    async def process_batch(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of metrics data."""
        pass
    
    async def start_processing(self):
        """Start the continuous processing of metrics data."""
        pass
    
    async def stop_processing(self):
        """Stop the continuous processing of metrics data."""
        pass
    
    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the pipeline."""
        pass
```
