"""In-memory implementation of the storage adapter for testing and development."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict

from metrics_pipeline.adapters.storage.base import StorageAdapter
from metrics_pipeline.core.models.metrics import Metric, MetricsData

logger = logging.getLogger(__name__)

class InMemoryStorageAdapter(StorageAdapter):
    """Storage adapter that uses in-memory storage for metrics data."""
    
    def __init__(self):
        """Initialize the in-memory storage adapter."""
        self.storage = []
    
    async def store(self, metrics_data: MetricsData) -> bool:
        """
        Store metrics data in memory.
        
        Args:
            metrics_data: MetricsData object containing metrics to store
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            self.storage.append(metrics_data)
            logger.info(f"Stored metrics data in memory: {metrics_data.metric_type}")
            return True
        except Exception as e:
            logger.error(f"Error storing metrics data: {e}")
            return False
    
    async def batch_store(self, metrics_batch: List[MetricsData]) -> Dict[str, Any]:
        """
        Store a batch of metrics data in memory.
        
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
            try:
                self.storage.append(metrics_data)
                result["success_count"] += 1
            except Exception as e:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data.dict(),
                    "errors": [str(e)]
                })
        
        logger.info(f"Batch stored {result['success_count']} metrics data in memory")
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
        Query metrics data from memory.
        
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
        filtered_data = self.storage
        
        # Apply filters
        if metric_type:
            filtered_data = [data for data in filtered_data if data.metric_type == metric_type]
        
        if metric_names:
            filtered_data = [
                data for data in filtered_data 
                if any(metric.name in metric_names for metric in data.metrics)
            ]
        
        if start_time:
            filtered_data = [data for data in filtered_data if data.timestamp >= start_time]
        
        if end_time:
            filtered_data = [data for data in filtered_data if data.timestamp <= end_time]
        
        if tags:
            filtered_data = [
                data for data in filtered_data
                if any(
                    all(
                        tag_key in metric.tags and metric.tags[tag_key] == tag_value
                        for tag_key, tag_value in tags.items()
                    )
                    for metric in data.metrics
                )
            ]
        
        # Sort by timestamp descending
        filtered_data = sorted(filtered_data, key=lambda x: x.timestamp, reverse=True)
        
        # Apply pagination
        paginated_data = filtered_data[offset:offset + limit]
        
        logger.info(f"Retrieved {len(paginated_data)} metrics from memory")
        return paginated_data
    
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
        """
        Aggregate metrics data from memory.
        
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
        # Validate aggregation type
        valid_aggregations = {"sum", "avg", "min", "max", "count"}
        if aggregation not in valid_aggregations:
            raise ValueError(f"Invalid aggregation type: {aggregation}. Must be one of {valid_aggregations}")
        
        # Filter data
        filtered_data = self.storage
        
        if metric_type:
            filtered_data = [data for data in filtered_data if data.metric_type == metric_type]
        
        if start_time:
            filtered_data = [data for data in filtered_data if data.timestamp >= start_time]
        
        if end_time:
            filtered_data = [data for data in filtered_data if data.timestamp <= end_time]
        
        # Extract metrics with matching name
        metrics = []
        for data in filtered_data:
            for metric in data.metrics:
                if metric.name == metric_name:
                    # Check tags if provided
                    if tags:
                        if all(tag_key in metric.tags and metric.tags[tag_key] == tag_value for tag_key, tag_value in tags.items()):
                            metrics.append((data, metric))
                    else:
                        metrics.append((data, metric))
        
        # Group by specified fields
        if group_by:
            valid_group_by = {"metric_type", "metric_unit", "source"}
            invalid_fields = set(group_by) - valid_group_by
            if invalid_fields:
                raise ValueError(f"Invalid group_by fields: {invalid_fields}. Must be one of {valid_group_by}")
            
            # Group metrics
            grouped_metrics = defaultdict(list)
            for data, metric in metrics:
                key_parts = []
                for field in group_by:
                    if field == "metric_type":
                        key_parts.append(data.metric_type)
                    elif field == "metric_unit":
                        key_parts.append(metric.unit or "")
                    elif field == "source":
                        key_parts.append(data.source or "")
                
                key = tuple(key_parts)
                grouped_metrics[key].append(metric.value)
            
            # Calculate aggregation for each group
            results = []
            for key, values in grouped_metrics.items():
                if aggregation == "sum":
                    agg_value = sum(values)
                elif aggregation == "avg":
                    agg_value = sum(values) / len(values) if values else 0
                elif aggregation == "min":
                    agg_value = min(values) if values else 0
                elif aggregation == "max":
                    agg_value = max(values) if values else 0
                elif aggregation == "count":
                    agg_value = len(values)
                
                result = {}
                for i, field in enumerate(group_by):
                    result[field] = key[i]
                result["value"] = agg_value
                results.append(result)
            
            return {
                "aggregation": aggregation,
                "metric_type": metric_type,
                "metric_name": metric_name,
                "results": results
            }
        else:
            # Calculate aggregation for all metrics
            values = [metric.value for _, metric in metrics]
            
            if aggregation == "sum":
                agg_value = sum(values) if values else 0
            elif aggregation == "avg":
                agg_value = sum(values) / len(values) if values else 0
            elif aggregation == "min":
                agg_value = min(values) if values else 0
            elif aggregation == "max":
                agg_value = max(values) if values else 0
            elif aggregation == "count":
                agg_value = len(values)
            
            return {
                "aggregation": aggregation,
                "metric_type": metric_type,
                "metric_name": metric_name,
                "value": agg_value
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the in-memory storage.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        return {
            "status": "healthy",
            "details": {
                "storage_type": "in-memory",
                "metrics_count": len(self.storage)
            }
        }
