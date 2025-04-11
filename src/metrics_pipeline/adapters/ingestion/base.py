"""Base interfaces for ingestion adapters."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class IngestionAdapter(ABC):
    """Base interface for all ingestion adapters."""
    
    @abstractmethod
    async def ingest(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Ingest metrics data into the pipeline.
        
        Args:
            metrics_data: Dictionary containing metrics data to ingest
            
        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def batch_ingest(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Ingest a batch of metrics data into the pipeline.
        
        Args:
            metrics_batch: List of dictionaries containing metrics data to ingest
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch ingestion
                {
                    "success_count": int,
                    "failure_count": int,
                    "failures": List[Dict[str, Any]]  # Failed items with error info
                }
        """
        pass
    
    @abstractmethod
    async def validate(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate metrics data before ingestion.
        
        Args:
            metrics_data: Dictionary containing metrics data to validate
            
        Returns:
            Dict[str, Any]: Validation results
                {
                    "valid": bool,
                    "errors": Optional[List[str]]
                }
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the ingestion adapter.
        
        Returns:
            Dict[str, Any]: Health check results
                {
                    "status": str,  # "healthy", "degraded", or "unhealthy"
                    "details": Dict[str, Any]  # Additional details about the health check
                }
        """
        pass
