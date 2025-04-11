"""Enhanced metrics pipeline processor with performance optimizations."""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

from metrics_pipeline.adapters.ingestion import IngestionAdapter
from metrics_pipeline.adapters.storage import StorageAdapter
from metrics_pipeline.core.models.metrics import MetricsData
from metrics_pipeline.utils.performance import BatchProcessor, SimpleCache, async_timed, with_retry

logger = logging.getLogger(__name__)

class MetricsPipeline:
    """
    Main pipeline class for processing metrics data.
    
    This class coordinates the flow of metrics data from ingestion to storage,
    with optimizations for batch processing and error handling.
    """
    
    def __init__(
        self,
        ingestion_adapter: IngestionAdapter,
        storage_adapter: StorageAdapter,
        batch_size: int = 100,
        processing_interval: float = 5.0,
        cache_ttl: int = 300,
        retry_count: int = 3
    ):
        """
        Initialize the metrics pipeline.
        
        Args:
            ingestion_adapter: Adapter for ingesting metrics data
            storage_adapter: Adapter for storing metrics data
            batch_size: Maximum number of metrics to process in a batch
            processing_interval: Interval between processing batches in seconds
            cache_ttl: Time-to-live for cache entries in seconds
            retry_count: Number of retries for failed operations
        """
        self.ingestion_adapter = ingestion_adapter
        self.storage_adapter = storage_adapter
        self.batch_size = batch_size
        self.processing_interval = processing_interval
        self.retry_count = retry_count
        
        self._processing_task: Optional[asyncio.Task] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._processed_ids: Set[str] = set()
        self._cache = SimpleCache(ttl=cache_ttl)
        self._batch_processor = BatchProcessor(
            process_batch_func=self._process_metrics_batch,
            batch_size=batch_size,
            batch_timeout=processing_interval
        )
    
    @async_timed
    async def process_metrics(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Process a single metrics data item.
        
        Args:
            metrics_data: Dictionary containing metrics data to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        # Generate a cache key based on the metrics data
        cache_key = f"validate_{hash(str(metrics_data))}"
        
        # Check cache for validation result
        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            validation_result = cached_result
        else:
            # Validate metrics data
            validation_result = await self.ingestion_adapter.validate(metrics_data)
            # Cache the validation result
            self._cache.set(cache_key, validation_result)
        
        if not validation_result["valid"]:
            logger.warning(f"Invalid metrics data: {validation_result['errors']}")
            return False
        
        # Convert to MetricsData object
        try:
            metrics_obj = MetricsData.parse_obj(metrics_data)
        except Exception as e:
            logger.error(f"Error parsing metrics data: {e}")
            return False
        
        # Store metrics data with retry
        try:
            success = await with_retry(
                self.storage_adapter.store,
                metrics_obj,
                retries=self.retry_count
            )
            return success
        except Exception as e:
            logger.error(f"Error storing metrics data after retries: {e}")
            return False
    
    @async_timed
    async def process_batch(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of metrics data.
        
        Args:
            metrics_batch: List of dictionaries containing metrics data to process
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch processing
        """
        # Add each item to the batch processor
        for metrics_data in metrics_batch:
            await self._batch_processor.add_item(metrics_data)
        
        # Force processing of the current batch
        return await self._batch_processor._process_batch()
    
    async def _process_metrics_batch(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of metrics data internally.
        
        Args:
            metrics_batch: List of dictionaries containing metrics data to process
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch processing
        """
        result = {
            "success_count": 0,
            "failure_count": 0,
            "failures": []
        }
        
        valid_metrics: List[MetricsData] = []
        
        # Validate all metrics first
        for metrics_data in metrics_batch:
            # Generate a cache key based on the metrics data
            cache_key = f"validate_{hash(str(metrics_data))}"
            
            # Check cache for validation result
            cached_result = self._cache.get(cache_key)
            if cached_result is not None:
                validation_result = cached_result
            else:
                # Validate metrics data
                validation_result = await self.ingestion_adapter.validate(metrics_data)
                # Cache the validation result
                self._cache.set(cache_key, validation_result)
            
            if validation_result["valid"]:
                try:
                    metrics_obj = MetricsData.parse_obj(metrics_data)
                    valid_metrics.append(metrics_obj)
                except Exception as e:
                    result["failure_count"] += 1
                    result["failures"].append({
                        "data": metrics_data,
                        "errors": [f"Error parsing metrics data: {e}"]
                    })
            else:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data,
                    "errors": validation_result["errors"]
                })
        
        # Store valid metrics in a batch
        if valid_metrics:
            try:
                storage_result = await with_retry(
                    self.storage_adapter.batch_store,
                    valid_metrics,
                    retries=self.retry_count
                )
                
                result["success_count"] = storage_result["success_count"]
                result["failure_count"] += storage_result["failure_count"]
                result["failures"].extend(storage_result["failures"])
            except Exception as e:
                logger.error(f"Error batch storing metrics data after retries: {e}")
                result["failure_count"] += len(valid_metrics)
                for metrics in valid_metrics:
                    result["failures"].append({
                        "data": metrics.dict(),
                        "errors": [f"Storage error: {e}"]
                    })
        
        return result
    
    async def start_processing(self):
        """Start the continuous processing of metrics data."""
        if self._processing_task is not None:
            logger.warning("Processing task is already running")
            return
        
        # Start the batch processor
        await self._batch_processor.start()
        
        # Start the processing task
        self._processing_task = asyncio.create_task(self._process_queue())
        logger.info("Started metrics processing")
    
    async def stop_processing(self):
        """Stop the continuous processing of metrics data."""
        if self._processing_task is None:
            logger.warning("No processing task is running")
            return
        
        # Stop the batch processor
        await self._batch_processor.stop()
        
        # Cancel the processing task
        self._processing_task.cancel()
        try:
            await self._processing_task
        except asyncio.CancelledError:
            pass
        
        self._processing_task = None
        logger.info("Stopped metrics processing")
    
    async def _process_queue(self):
        """Process metrics data from the queue continuously."""
        while True:
            try:
                # Get metrics data from the queue
                metrics_data = await self._queue.get()
                
                # Process metrics data
                await self._batch_processor.add_item(metrics_data)
                
                # Mark task as done
                self._queue.task_done()
            except asyncio.CancelledError:
                logger.info("Processing task cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing metrics data from queue: {e}")
    
    @async_timed
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the pipeline and its components.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        # Check ingestion adapter health
        try:
            ingestion_health = await with_retry(
                self.ingestion_adapter.health_check,
                retries=1,
                delay=0.5
            )
        except Exception as e:
            ingestion_health = {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
        
        # Check storage adapter health
        try:
            storage_health = await with_retry(
                self.storage_adapter.health_check,
                retries=1,
                delay=0.5
            )
        except Exception as e:
            storage_health = {
                "status": "unhealthy",
                "details": {"error": str(e)}
            }
        
        # Determine overall status
        if ingestion_health["status"] == "unhealthy" or storage_health["status"] == "unhealthy":
            status = "unhealthy"
        elif ingestion_health["status"] == "degraded" or storage_health["status"] == "degraded":
            status = "degraded"
        else:
            status = "healthy"
        
        # Clean up expired cache entries
        removed_entries = self._cache.cleanup()
        
        return {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": {
                "ingestion": ingestion_health,
                "storage": storage_health,
                "queue_size": self._queue.qsize(),
                "cache_entries_removed": removed_entries
            }
        }
