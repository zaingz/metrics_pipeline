# Usage Guide

This guide provides instructions and examples for using the Metrics Pipeline in various scenarios.

## Installation

### Using pip

```bash
pip install metrics-pipeline
```

### From source

```bash
git clone https://github.com/zaingz/metrics-pipeline.git
cd metrics-pipeline
pip install -e .
```

## Basic Usage

### Setting Up the Pipeline

```python
import asyncio
from metrics_pipeline.adapters.ingestion import SQSIngestionAdapter
from metrics_pipeline.adapters.storage import ClickHouseStorageAdapter
from metrics_pipeline.core.pipeline import MetricsPipeline

async def main():
    # Initialize adapters
    ingestion_adapter = SQSIngestionAdapter(
        queue_name="metrics-queue",
        region_name="us-east-1"
    )
    
    storage_adapter = ClickHouseStorageAdapter(
        host="localhost",
        port=9000,
        user="default",
        password="default",
        database="metrics"
    )
    
    # Create pipeline
    pipeline = MetricsPipeline(
        ingestion_adapter=ingestion_adapter,
        storage_adapter=storage_adapter
    )
    
    # Start processing
    await pipeline.start_processing()
    
    # Keep the pipeline running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        # Stop processing on keyboard interrupt
        await pipeline.stop_processing()

if __name__ == "__main__":
    asyncio.run(main())
```

### Sending Metrics

```python
import asyncio
import datetime
from metrics_pipeline.adapters.ingestion import HTTPIngestionAdapter

async def send_metrics():
    # Initialize adapter
    adapter = HTTPIngestionAdapter(
        api_url="https://api.example.com/metrics"
    )
    
    # Create metrics data
    metrics_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "metric_type": "page_view",
        "metrics": [
            {
                "name": "load_time",
                "value": 1.23,
                "unit": "seconds",
                "tags": {
                    "page": "home",
                    "device": "mobile"
                }
            },
            {
                "name": "user_count",
                "value": 1,
                "tags": {
                    "user_type": "new"
                }
            }
        ]
    }
    
    # Send metrics
    success = await adapter.ingest(metrics_data)
    print(f"Metrics sent: {success}")

if __name__ == "__main__":
    asyncio.run(send_metrics())
```

### Querying Metrics

```python
import asyncio
import datetime
from metrics_pipeline.adapters.storage import ClickHouseStorageAdapter

async def query_metrics():
    # Initialize adapter
    adapter = ClickHouseStorageAdapter(
        host="localhost",
        port=9000,
        user="default",
        password="default",
        database="metrics"
    )
    
    # Query metrics
    metrics = await adapter.query(
        metric_type="page_view",
        metric_names=["load_time"],
        start_time=datetime.datetime.now() - datetime.timedelta(days=7),
        end_time=datetime.datetime.now(),
        tags={"device": "mobile"},
        limit=10
    )
    
    # Print results
    for metric_data in metrics:
        print(f"Timestamp: {metric_data.timestamp}")
        print(f"Type: {metric_data.metric_type}")
        for metric in metric_data.metrics:
            print(f"  {metric.name}: {metric.value} {metric.unit or ''}")
            print(f"  Tags: {metric.tags}")
        print()

if __name__ == "__main__":
    asyncio.run(query_metrics())
```

### Creating Visualizations

```python
import asyncio
from metrics_pipeline.adapters.visualization import MetabaseVisualizationAdapter

async def create_dashboard():
    # Initialize adapter
    adapter = MetabaseVisualizationAdapter(
        url="http://localhost:3000",
        username="admin@example.com",
        password="password",
        database_id=1
    )
    
    # Connect to Metabase
    connected = await adapter.connect()
    if not connected:
        print("Failed to connect to Metabase")
        return
    
    # Create dashboard
    dashboard = await adapter.create_dashboard(
        name="Page Performance Dashboard",
        description="Dashboard for monitoring page performance metrics"
    )
    
    # Create visualizations
    viz1 = await adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Page Load Time",
        visualization_type="line",
        query={
            "sql": """
                SELECT 
                    timestamp, 
                    metric_value as "Load Time"
                FROM metrics
                WHERE metric_type = 'page_view'
                AND metric_name = 'load_time'
                ORDER BY timestamp
            """,
            "dimensions": ["timestamp"],
            "metrics": ["Load Time"]
        },
        description="Average page load time over time"
    )
    
    viz2 = await adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="User Count by Device",
        visualization_type="bar",
        query={
            "sql": """
                SELECT 
                    tags['device'] as device,
                    sum(metric_value) as "User Count"
                FROM metrics
                WHERE metric_type = 'page_view'
                AND metric_name = 'user_count'
                GROUP BY device
            """,
            "dimensions": ["device"],
            "metrics": ["User Count"]
        },
        description="User count by device type"
    )
    
    print(f"Dashboard created: {dashboard['url']}")
    print(f"Visualization 1: {viz1['url']}")
    print(f"Visualization 2: {viz2['url']}")

if __name__ == "__main__":
    asyncio.run(create_dashboard())
```

## Extending the Pipeline

### Creating a Custom Ingestion Adapter

```python
from typing import Any, Dict, List, Optional
from metrics_pipeline.adapters.ingestion import IngestionAdapter

class CustomIngestionAdapter(IngestionAdapter):
    """Custom ingestion adapter implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the custom ingestion adapter."""
        self.config = config
    
    async def ingest(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Ingest metrics data using custom logic.
        
        Args:
            metrics_data: Dictionary containing metrics data to ingest
            
        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        # Implement custom ingestion logic
        try:
            # Validate data
            validation_result = await self.validate(metrics_data)
            if not validation_result["valid"]:
                return False
            
            # Custom ingestion logic here
            # ...
            
            return True
        except Exception as e:
            print(f"Error ingesting metrics data: {e}")
            return False
    
    async def batch_ingest(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Ingest a batch of metrics data using custom logic.
        
        Args:
            metrics_batch: List of dictionaries containing metrics data to ingest
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch ingestion
        """
        # Implement custom batch ingestion logic
        result = {
            "success_count": 0,
            "failure_count": 0,
            "failures": []
        }
        
        for metrics_data in metrics_batch:
            success = await self.ingest(metrics_data)
            if success:
                result["success_count"] += 1
            else:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data,
                    "errors": ["Ingestion failed"]
                })
        
        return result
    
    async def validate(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate metrics data before ingestion.
        
        Args:
            metrics_data: Dictionary containing metrics data to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        # Implement custom validation logic
        errors = []
        
        # Check required fields
        required_fields = ["timestamp", "metric_type", "metrics"]
        for field in required_fields:
            if field not in metrics_data:
                errors.append(f"Missing required field: {field}")
        
        # Custom validation rules
        # ...
        
        return {
            "valid": len(errors) == 0,
            "errors": errors if errors else None
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the custom ingestion adapter.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        # Implement custom health check logic
        try:
            # Custom health check logic here
            # ...
            
            return {
                "status": "healthy",
                "details": {
                    "type": "custom",
                    "config": self.config
                }
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "details": {
                    "error": str(e),
                    "type": "custom",
                    "config": self.config
                }
            }
```

### Creating a Custom Storage Adapter

```python
from datetime import datetime
from typing import Any, Dict, List, Optional
from metrics_pipeline.adapters.storage import StorageAdapter
from metrics_pipeline.core.models import MetricsData

class CustomStorageAdapter(StorageAdapter):
    """Custom storage adapter implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the custom storage adapter."""
        self.config = config
        self.storage = []  # Example in-memory storage
    
    async def store(self, metrics_data: MetricsData) -> bool:
        """
        Store metrics data using custom logic.
        
        Args:
            metrics_data: MetricsData object containing metrics to store
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            # Custom storage logic here
            self.storage.append(metrics_data)
            return True
        except Exception as e:
            print(f"Error storing metrics data: {e}")
            return False
    
    async def batch_store(self, metrics_batch: List[MetricsData]) -> Dict[str, Any]:
        """
        Store a batch of metrics data using custom logic.
        
        Args:
            metrics_batch: List of MetricsData objects to store
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch storage
        """
        result = {
            "success_count": 0,
            "failure_count": 0,
            "failures": []
        }
        
        for metrics_data in metrics_batch:
            success = await self.store(metrics_data)
            if success:
                result["success_count"] += 1
            else:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data.dict(),
                    "errors": ["Storage failed"]
                })
        
        return result
    
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
        """
        Query metrics data using custom logic.
        
        Args:
            metric_type: Optional filter by metric type
            metric_names: Optional list of metric names to filter by
            start_time: Optional start time for time range filter
            end_time: Optional end time for time range filter
            tags: Optional tags to filter by
            limit: Maximum number of results to return
            offset: Offset for pagination
            
        Returns:
            List[MetricsData]: List of metrics data matching the query
        """
        # Implement custom query logic
        filtered_data = self.storage
        
        # Apply filters
        if metric_type:
            filtered_data = [data for data in filtered_data if data.metric_type == metric_type]
        
        # Apply other filters...
        
        # Apply pagination
        paginated_data = filtered_data[offset:offset + limit]
        
        return paginated_data
    
    async def aggregate(
        self,
        metric_type: str,
        metric_name: str,
        aggregation: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        group_by: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Aggregate metrics data using custom logic.
        
        Args:
            metric_type: Type of metrics to aggregate
            metric_name: Name of the metric to aggregate
            aggregation: Type of aggregation to perform
            start_time: Optional start time for time range filter
            end_time: Optional end time for time range filter
            group_by: Optional list of fields to group by
            tags: Optional tags to filter by
            
        Returns:
            Dict[str, Any]: Aggregation results
        """
        # Implement custom aggregation logic
        # ...
        
        return {
            "aggregation": aggregation,
            "metric_type": metric_type,
            "metric_name": metric_name,
            "value": 0  # Replace with actual aggregation result
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the custom storage adapter.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        return {
            "status": "healthy",
            "details": {
                "type": "custom",
                "config": self.config,
                "metrics_count": len(self.storage)
            }
        }
```

## Configuration

### Environment Variables

The Metrics Pipeline supports configuration via environment variables:

- `METRICS_PIPELINE_LOG_LEVEL`: Logging level (default: INFO)
- `METRICS_PIPELINE_BATCH_SIZE`: Batch size for processing (default: 100)
- `METRICS_PIPELINE_PROCESSING_INTERVAL`: Interval between processing batches in seconds (default: 5.0)

### AWS Configuration

For AWS services:

- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_ENDPOINT_URL`: Custom endpoint URL for LocalStack

### ClickHouse Configuration

For ClickHouse storage:

- `CLICKHOUSE_HOST`: ClickHouse host (default: localhost)
- `CLICKHOUSE_PORT`: ClickHouse port (default: 9000)
- `CLICKHOUSE_USER`: ClickHouse user (default: default)
- `CLICKHOUSE_PASSWORD`: ClickHouse password (default: empty)
- `CLICKHOUSE_DATABASE`: ClickHouse database (default: default)

### Metabase Configuration

For Metabase visualization:

- `METABASE_URL`: Metabase URL
- `METABASE_USERNAME`: Metabase username
- `METABASE_PASSWORD`: Metabase password
- `METABASE_DATABASE_ID`: Metabase database ID
