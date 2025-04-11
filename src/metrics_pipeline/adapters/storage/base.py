"""Base interfaces for storage adapters."""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from metrics_pipeline.core.models.metrics import Metric, MetricsData


class StorageAdapter(ABC):
    """Base interface for all storage adapters."""
    
    @abstractmethod
    async def store(self, metrics_data: MetricsData) -> bool:
        """
        Store metrics data in the storage backend.
        
        Args:
            metrics_data: MetricsData object containing metrics to store
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def batch_store(self, metrics_batch: List[MetricsData]) -> Dict[str, Any]:
        """
        Store a batch of metrics data in the storage backend.
        
        Args:
            metrics_batch: List of MetricsData objects to store
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch storage
                {
                    "success_count": int,
                    "failure_count": int,
                    "failures": List[Dict[str, Any]]  # Failed items with error info
                }
        """
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
        """
        Query metrics data from the storage backend.
        
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
        """
        Aggregate metrics data from the storage backend.
        
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
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the storage adapter.
        
        Returns:
            Dict[str, Any]: Health check results
                {
                    "status": str,  # "healthy", "degraded", or "unhealthy"
                    "details": Dict[str, Any]  # Additional details about the health check
                }
        """
        pass
