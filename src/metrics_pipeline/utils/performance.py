"""
Performance optimization module for the metrics pipeline.
This module provides utilities and configurations to optimize the performance of the metrics pipeline.
"""
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Callable, TypeVar, Coroutine
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')

# Batch processing configuration
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT = 5.0  # seconds

# Caching configuration
DEFAULT_CACHE_TTL = 300  # seconds
DEFAULT_CACHE_SIZE = 1000  # items

class BatchProcessor:
    """
    Utility for efficient batch processing of metrics.
    
    This class collects items until either the batch size is reached or
    the batch timeout expires, then processes them in a single batch.
    """
    
    def __init__(
        self, 
        process_batch_func: Callable[[List[Any]], Coroutine[Any, Any, Dict[str, Any]]],
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout: float = DEFAULT_BATCH_TIMEOUT
    ):
        """
        Initialize the batch processor.
        
        Args:
            process_batch_func: Async function to process a batch of items
            batch_size: Maximum number of items in a batch
            batch_timeout: Maximum time to wait before processing a partial batch
        """
        self.process_batch_func = process_batch_func
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.items: List[Any] = []
        self.last_process_time = time.time()
        self.processing_lock = asyncio.Lock()
        self.running = False
        self.task: Optional[asyncio.Task] = None
    
    async def add_item(self, item: Any) -> Dict[str, Any]:
        """
        Add an item to the batch and process the batch if needed.
        
        Args:
            item: Item to add to the batch
            
        Returns:
            Dict[str, Any]: Result of batch processing if triggered, empty dict otherwise
        """
        async with self.processing_lock:
            self.items.append(item)
            
            if len(self.items) >= self.batch_size:
                return await self._process_batch()
            
            current_time = time.time()
            if current_time - self.last_process_time >= self.batch_timeout and self.items:
                return await self._process_batch()
            
            return {}
    
    async def _process_batch(self) -> Dict[str, Any]:
        """
        Process the current batch of items.
        
        Returns:
            Dict[str, Any]: Result of batch processing
        """
        if not self.items:
            return {}
        
        items_to_process = self.items.copy()
        self.items = []
        self.last_process_time = time.time()
        
        try:
            result = await self.process_batch_func(items_to_process)
            return result
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return {"success_count": 0, "failure_count": len(items_to_process), "error": str(e)}
    
    async def start(self) -> None:
        """Start the background task for processing batches on timeout."""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._background_processor())
    
    async def stop(self) -> None:
        """Stop the background task and process any remaining items."""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        # Process any remaining items
        async with self.processing_lock:
            if self.items:
                await self._process_batch()
    
    async def _background_processor(self) -> None:
        """Background task that processes batches on timeout."""
        while self.running:
            current_time = time.time()
            
            async with self.processing_lock:
                if self.items and current_time - self.last_process_time >= self.batch_timeout:
                    await self._process_batch()
            
            # Sleep for a short time to avoid busy waiting
            await asyncio.sleep(0.1)


class SimpleCache:
    """
    Simple in-memory cache with TTL support.
    
    This class provides a simple caching mechanism to avoid redundant operations.
    """
    
    def __init__(self, ttl: int = DEFAULT_CACHE_TTL, max_size: int = DEFAULT_CACHE_SIZE):
        """
        Initialize the cache.
        
        Args:
            ttl: Time-to-live for cache entries in seconds
            max_size: Maximum number of items in the cache
        """
        self.ttl = ttl
        self.max_size = max_size
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Optional[Any]: Cached value or None if not found or expired
        """
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        if time.time() > entry["expiry"]:
            # Entry has expired
            del self.cache[key]
            return None
        
        return entry["value"]
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional custom TTL for this entry
        """
        # If cache is full, remove the oldest entry
        if len(self.cache) >= self.max_size and key not in self.cache:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k]["expiry"])
            del self.cache[oldest_key]
        
        expiry = time.time() + (ttl if ttl is not None else self.ttl)
        self.cache[key] = {"value": value, "expiry": expiry}
    
    def delete(self, key: str) -> None:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
        """
        if key in self.cache:
            del self.cache[key]
    
    def clear(self) -> None:
        """Clear all entries from the cache."""
        self.cache.clear()
    
    def cleanup(self) -> int:
        """
        Remove expired entries from the cache.
        
        Returns:
            int: Number of entries removed
        """
        current_time = time.time()
        expired_keys = [
            key for key, entry in self.cache.items() 
            if current_time > entry["expiry"]
        ]
        
        for key in expired_keys:
            del self.cache[key]
        
        return len(expired_keys)


def async_timed(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
    """
    Decorator to measure and log the execution time of async functions.
    
    Args:
        func: Async function to time
        
    Returns:
        Callable: Wrapped function that logs execution time
    """
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            end_time = time.time()
            duration = end_time - start_time
            logger.debug(f"{func.__name__} took {duration:.4f} seconds")
    
    return wrapper


def create_connection_pool(
    create_connection_func: Callable[[], Coroutine[Any, Any, Any]],
    min_size: int = 5,
    max_size: int = 20
) -> Dict[str, Any]:
    """
    Create a connection pool configuration for database adapters.
    
    Args:
        create_connection_func: Async function to create a new connection
        min_size: Minimum number of connections in the pool
        max_size: Maximum number of connections in the pool
        
    Returns:
        Dict[str, Any]: Connection pool configuration
    """
    return {
        "create_connection": create_connection_func,
        "min_size": min_size,
        "max_size": max_size,
        "recycle": 3600,  # Recycle connections after 1 hour
        "on_connect": None,
        "on_disconnect": None
    }


async def with_retry(
    func: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    **kwargs: Any
) -> T:
    """
    Execute an async function with retry logic.
    
    Args:
        func: Async function to execute
        *args: Arguments to pass to the function
        retries: Maximum number of retries
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for subsequent retries
        exceptions: Tuple of exceptions to catch and retry
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        T: Result of the function
        
    Raises:
        Exception: The last exception raised by the function after all retries
    """
    last_exception = None
    current_delay = delay
    
    for attempt in range(retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                logger.warning(
                    f"Attempt {attempt + 1}/{retries + 1} failed: {e}. "
                    f"Retrying in {current_delay:.2f} seconds..."
                )
                await asyncio.sleep(current_delay)
                current_delay *= backoff
            else:
                logger.error(f"All {retries + 1} attempts failed. Last error: {e}")
    
    raise last_exception
