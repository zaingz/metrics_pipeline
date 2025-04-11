"""ClickHouse implementation of the storage adapter."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from metrics_pipeline.adapters.storage.base import StorageAdapter
from metrics_pipeline.core.models.metrics import Metric, MetricsData

logger = logging.getLogger(__name__)

class ClickHouseStorageAdapter(StorageAdapter):
    """Storage adapter that uses ClickHouse for metrics storage."""
    
    def __init__(
        self, 
        host: str = "localhost",
        port: int = 9000,
        user: str = "default",
        password: str = "",
        database: str = "default",
        table: str = "metrics",
        create_table_if_not_exists: bool = True
    ):
        """
        Initialize the ClickHouse storage adapter.
        
        Args:
            host: ClickHouse server host
            port: ClickHouse server port
            user: ClickHouse user
            password: ClickHouse password
            database: ClickHouse database
            table: ClickHouse table
            create_table_if_not_exists: Whether to create the table if it doesn't exist
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        
        # Initialize ClickHouse client
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        # Create table if it doesn't exist
        if create_table_if_not_exists:
            self._create_table_if_not_exists()
    
    def _create_table_if_not_exists(self):
        """Create the metrics table if it doesn't exist."""
        try:
            self.client.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    timestamp DateTime,
                    metric_type String,
                    metric_name String,
                    metric_value Float64,
                    metric_unit String,
                    tags Map(String, String),
                    source String,
                    context String
                ) ENGINE = MergeTree()
                ORDER BY (timestamp, metric_type, metric_name)
            """)
            logger.info(f"Table {self.table} created or already exists")
        except ClickHouseError as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    async def store(self, metrics_data: MetricsData) -> bool:
        """
        Store metrics data in ClickHouse.
        
        Args:
            metrics_data: MetricsData object containing metrics to store
            
        Returns:
            bool: True if storage was successful, False otherwise
        """
        try:
            # Convert metrics data to rows
            rows = self._convert_metrics_data_to_rows(metrics_data)
            
            # Insert rows into ClickHouse
            self.client.execute(
                f"""
                INSERT INTO {self.table} (
                    timestamp, metric_type, metric_name, metric_value, 
                    metric_unit, tags, source, context
                ) VALUES
                """,
                rows
            )
            
            logger.info(f"Stored {len(rows)} metrics in ClickHouse")
            return True
        except Exception as e:
            logger.error(f"Error storing metrics data: {e}")
            return False
    
    async def batch_store(self, metrics_batch: List[MetricsData]) -> Dict[str, Any]:
        """
        Store a batch of metrics data in ClickHouse.
        
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
        
        all_rows = []
        
        # Convert all metrics data to rows
        for metrics_data in metrics_batch:
            try:
                rows = self._convert_metrics_data_to_rows(metrics_data)
                all_rows.extend(rows)
                result["success_count"] += 1
            except Exception as e:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data.dict(),
                    "errors": [str(e)]
                })
        
        # Insert all rows into ClickHouse
        if all_rows:
            try:
                self.client.execute(
                    f"""
                    INSERT INTO {self.table} (
                        timestamp, metric_type, metric_name, metric_value, 
                        metric_unit, tags, source, context
                    ) VALUES
                    """,
                    all_rows
                )
                
                logger.info(f"Stored {len(all_rows)} metrics in ClickHouse")
            except Exception as e:
                logger.error(f"Error batch storing metrics data: {e}")
                # Mark all as failed
                result["failure_count"] = len(metrics_batch)
                result["success_count"] = 0
                result["failures"] = [
                    {
                        "data": metrics_data.dict(),
                        "errors": [str(e)]
                    }
                    for metrics_data in metrics_batch
                ]
        
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
        Query metrics data from ClickHouse.
        
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
        # Build query conditions
        conditions = []
        params = {}
        
        if metric_type:
            conditions.append("metric_type = %(metric_type)s")
            params["metric_type"] = metric_type
        
        if metric_names:
            conditions.append("metric_name IN %(metric_names)s")
            params["metric_names"] = tuple(metric_names)
        
        if start_time:
            conditions.append("timestamp >= %(start_time)s")
            params["start_time"] = start_time
        
        if end_time:
            conditions.append("timestamp <= %(end_time)s")
            params["end_time"] = end_time
        
        if tags:
            for key, value in tags.items():
                conditions.append(f"tags['{key}'] = %(tag_{key})s")
                params[f"tag_{key}"] = value
        
        # Build WHERE clause
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # Execute query
        try:
            query = f"""
                SELECT 
                    timestamp, 
                    metric_type, 
                    metric_name, 
                    metric_value, 
                    metric_unit, 
                    tags, 
                    source, 
                    context
                FROM {self.table}
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT {limit}
                OFFSET {offset}
            """
            
            rows = self.client.execute(query, params)
            
            # Convert rows to MetricsData objects
            result = self._convert_rows_to_metrics_data(rows)
            
            logger.info(f"Retrieved {len(result)} metrics from ClickHouse")
            return result
        except Exception as e:
            logger.error(f"Error querying metrics data: {e}")
            return []
    
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
        Aggregate metrics data from ClickHouse.
        
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
        
        # Map aggregation to ClickHouse function
        agg_function = {
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "count": "count"
        }[aggregation]
        
        # Build query conditions
        conditions = [
            "metric_type = %(metric_type)s",
            "metric_name = %(metric_name)s"
        ]
        params = {
            "metric_type": metric_type,
            "metric_name": metric_name
        }
        
        if start_time:
            conditions.append("timestamp >= %(start_time)s")
            params["start_time"] = start_time
        
        if end_time:
            conditions.append("timestamp <= %(end_time)s")
            params["end_time"] = end_time
        
        if tags:
            for key, value in tags.items():
                conditions.append(f"tags['{key}'] = %(tag_{key})s")
                params[f"tag_{key}"] = value
        
        # Build WHERE clause
        where_clause = " AND ".join(conditions)
        
        # Build GROUP BY clause
        group_by_clause = ""
        if group_by:
            valid_group_by = {"metric_type", "metric_name", "metric_unit", "source"}
            invalid_fields = set(group_by) - valid_group_by
            if invalid_fields:
                raise ValueError(f"Invalid group_by fields: {invalid_fields}. Must be one of {valid_group_by}")
            
            group_by_clause = f"GROUP BY {', '.join(group_by)}"
        
        # Execute query
        try:
            query = f"""
                SELECT 
                    {group_by_clause + ',' if group_by else ''}
                    {agg_function}(metric_value) as agg_value
                FROM {self.table}
                WHERE {where_clause}
                {group_by_clause}
            """
            
            rows = self.client.execute(query, params)
            
            # Format results
            if group_by:
                result = {
                    "aggregation": aggregation,
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "results": [
                        {
                            **{group_by[i]: row[i] for i in range(len(group_by))},
                            "value": row[-1]
                        }
                        for row in rows
                    ]
                }
            else:
                result = {
                    "aggregation": aggregation,
                    "metric_type": metric_type,
                    "metric_name": metric_name,
                    "value": rows[0][0] if rows else 0
                }
            
            logger.info(f"Aggregated metrics from ClickHouse: {aggregation}({metric_name})")
            return result
        except Exception as e:
            logger.error(f"Error aggregating metrics data: {e}")
            return {
                "aggregation": aggregation,
                "metric_type": metric_type,
                "metric_name": metric_name,
                "error": str(e)
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the ClickHouse connection.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        try:
            # Check if we can execute a simple query
            result = self.client.execute("SELECT 1")
            
            if result and result[0][0] == 1:
                return {
                    "status": "healthy",
                    "details": {
                        "host": self.host,
                        "port": self.port,
                        "database": self.database,
                        "table": self.table
                    }
                }
            else:
                return {
                    "status": "degraded",
                    "details": {
                        "host": self.host,
                        "port": self.port,
                        "database": self.database,
                        "table": self.table,
                        "message": "Unexpected result from health check query"
                    }
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "details": {
                    "host": self.host,
                    "port": self.port,
                    "database": self.database,
                    "table": self.table,
                    "error": str(e)
                }
            }
    
    def _convert_metrics_data_to_rows(self, metrics_data: MetricsData) -> List[tuple]:
        """
        Convert MetricsData object to rows for ClickHouse insertion.
        
        Args:
            metrics_data: MetricsData object to convert
            
        Returns:
            List[tuple]: List of tuples for ClickHouse insertion
        """
        rows = []
        
        for metric in metrics_data.metrics:
            # Convert context to JSON string if present
            context_str = None
            if metrics_data.context:
                import json
                context_str = json.dumps(metrics_data.context)
            
            row = (
                metrics_data.timestamp,
                metrics_data.metric_type,
                metric.name,
                metric.value,
                metric.unit or "",
                metric.tags,
                metrics_data.source or "",
                context_str or ""
            )
            
            rows.append(row)
        
        return rows
    
    def _convert_rows_to_metrics_data(self, rows: List[tuple]) -> List[MetricsData]:
        """
        Convert ClickHouse rows to MetricsData objects.
        
        Args:
            rows: List of tuples from ClickHouse query
            
        Returns:
            List[MetricsData]: List of MetricsData objects
        """
        # Group rows by timestamp and metric_type
        grouped_rows = {}
        for row in rows:
            timestamp, metric_type, metric_name, metric_value, metric_unit, tags, source, context_str = row
            
            key = (timestamp, metric_type, source)
            if key not in grouped_rows:
                grouped_rows[key] = {
                    "timestamp": timestamp,
                    "metric_type": metric_type,
                    "source": source if source else None,
                    "context": None,
                    "metrics": []
                }
                
                # Parse context if present
                if context_str:
                    import json
                    try:
                        grouped_rows[key]["context"] = json.loads(context_str)
                    except:
                        pass
            
            # Add metric
            grouped_rows[key]["metrics"].append({
                "name": metric_name,
                "value": metric_value,
                "unit": metric_unit if metric_unit else None,
                "tags": tags
            })
        
        # Convert to MetricsData objects
        result = []
        for data in grouped_rows.values():
            metrics = [Metric(**m) for m in data["metrics"]]
            metrics_data = MetricsData(
                timestamp=data["timestamp"],
                metric_type=data["metric_type"],
                metrics=metrics,
                source=data["source"],
                context=data["context"]
            )
            result.append(metrics_data)
        
        return result
