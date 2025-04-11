"""HTTP API implementation of the ingestion adapter."""
import json
import logging
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import RequestException

from metrics_pipeline.adapters.ingestion.base import IngestionAdapter
from metrics_pipeline.core.models.metrics import MetricsData, MetricsValidationResult

logger = logging.getLogger(__name__)

class HTTPIngestionAdapter(IngestionAdapter):
    """Ingestion adapter that uses HTTP API for sending metrics data."""
    
    def __init__(
        self, 
        api_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
        verify_ssl: bool = True
    ):
        """
        Initialize the HTTP ingestion adapter.
        
        Args:
            api_url: URL of the API endpoint
            headers: Optional HTTP headers to include in requests
            timeout: Request timeout in seconds
            verify_ssl: Whether to verify SSL certificates
        """
        self.api_url = api_url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout
        self.verify_ssl = verify_ssl
    
    async def ingest(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Ingest metrics data via HTTP API.
        
        Args:
            metrics_data: Dictionary containing metrics data to ingest
            
        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        try:
            # Validate data before sending
            validation_result = await self.validate(metrics_data)
            if not validation_result["valid"]:
                logger.error(f"Invalid metrics data: {validation_result['errors']}")
                return False
            
            # Send data to API
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=metrics_data,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            
            # Check response
            response.raise_for_status()
            
            logger.info(f"Metrics data sent to API: {response.status_code}")
            return True
        except RequestException as e:
            logger.error(f"Error ingesting metrics data via HTTP: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error ingesting metrics data: {e}")
            return False
    
    async def batch_ingest(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Ingest a batch of metrics data via HTTP API.
        
        Args:
            metrics_batch: List of dictionaries containing metrics data to ingest
            
        Returns:
            Dict[str, Any]: Dictionary containing results of batch ingestion
        """
        result = {
            "success_count": 0,
            "failure_count": 0,
            "failures": []
        }
        
        for metrics_data in metrics_batch:
            # Validate data before sending
            validation_result = await self.validate(metrics_data)
            if not validation_result["valid"]:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data,
                    "errors": validation_result["errors"]
                })
                continue
                
            try:
                # Send data to API
                response = requests.post(
                    self.api_url,
                    headers=self.headers,
                    json=metrics_data,
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )
                
                # Check response
                response.raise_for_status()
                
                result["success_count"] += 1
            except RequestException as e:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data,
                    "errors": [str(e)]
                })
            except Exception as e:
                result["failure_count"] += 1
                result["failures"].append({
                    "data": metrics_data,
                    "errors": [f"Unexpected error: {str(e)}"]
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
        errors = []
        
        # Check required fields
        required_fields = ["timestamp", "metric_type", "metrics"]
        for field in required_fields:
            if field not in metrics_data:
                errors.append(f"Missing required field: {field}")
        
        # Validate metrics array if present
        if "metrics" in metrics_data and isinstance(metrics_data["metrics"], list):
            for idx, metric in enumerate(metrics_data["metrics"]):
                if not isinstance(metric, dict):
                    errors.append(f"Metric at index {idx} is not a dictionary")
                    continue
                    
                # Check required metric fields
                metric_required_fields = ["name", "value"]
                for field in metric_required_fields:
                    if field not in metric:
                        errors.append(f"Metric at index {idx} missing required field: {field}")
        elif "metrics" in metrics_data:
            errors.append("'metrics' field must be an array")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors if errors else None
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the HTTP API.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        try:
            # Try to connect to the API
            response = requests.get(
                self.api_url,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            
            if response.status_code < 400:
                return {
                    "status": "healthy",
                    "details": {
                        "api_url": self.api_url,
                        "status_code": response.status_code
                    }
                }
            else:
                return {
                    "status": "degraded",
                    "details": {
                        "api_url": self.api_url,
                        "status_code": response.status_code,
                        "message": f"API returned status code {response.status_code}"
                    }
                }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "details": {
                    "api_url": self.api_url,
                    "error": str(e)
                }
            }
