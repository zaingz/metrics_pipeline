"""AWS SQS implementation of the ingestion adapter."""
import json
import logging
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from metrics_pipeline.adapters.ingestion.base import IngestionAdapter
from metrics_pipeline.core.models.metrics import MetricsData, MetricsValidationResult

logger = logging.getLogger(__name__)

class SQSIngestionAdapter(IngestionAdapter):
    """Ingestion adapter that uses AWS SQS for message queuing."""
    
    def __init__(
        self, 
        queue_name: str,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ):
        """
        Initialize the SQS ingestion adapter.
        
        Args:
            queue_name: Name of the SQS queue
            region_name: AWS region name
            endpoint_url: Optional endpoint URL for LocalStack
            aws_access_key_id: Optional AWS access key ID
            aws_secret_access_key: Optional AWS secret access key
        """
        self.queue_name = queue_name
        self.region_name = region_name
        
        # Initialize SQS client
        sqs_kwargs = {
            "region_name": region_name
        }
        
        if endpoint_url:
            sqs_kwargs["endpoint_url"] = endpoint_url
            
        if aws_access_key_id and aws_secret_access_key:
            sqs_kwargs["aws_access_key_id"] = aws_access_key_id
            sqs_kwargs["aws_secret_access_key"] = aws_secret_access_key
            
        self.sqs = boto3.client("sqs", **sqs_kwargs)
        
        # Get queue URL
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            self.queue_url = response["QueueUrl"]
        except ClientError as e:
            logger.error(f"Error getting queue URL: {e}")
            raise
    
    async def ingest(self, metrics_data: Dict[str, Any]) -> bool:
        """
        Ingest metrics data into SQS queue.
        
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
            
            # Send message to SQS
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(metrics_data)
            )
            
            logger.info(f"Message sent to SQS: {response['MessageId']}")
            return True
        except Exception as e:
            logger.error(f"Error ingesting metrics data: {e}")
            return False
    
    async def batch_ingest(self, metrics_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Ingest a batch of metrics data into SQS queue.
        
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
        
        # SQS batch send limit is 10 messages
        batch_size = 10
        
        for i in range(0, len(metrics_batch), batch_size):
            batch_chunk = metrics_batch[i:i+batch_size]
            entries = []
            
            # Prepare batch entries
            for idx, metrics_data in enumerate(batch_chunk):
                # Validate data before sending
                validation_result = await self.validate(metrics_data)
                if not validation_result["valid"]:
                    result["failure_count"] += 1
                    result["failures"].append({
                        "data": metrics_data,
                        "errors": validation_result["errors"]
                    })
                    continue
                
                entry = {
                    "Id": str(i + idx),
                    "MessageBody": json.dumps(metrics_data)
                }
                entries.append(entry)
            
            if not entries:
                continue
                
            try:
                # Send batch to SQS
                response = self.sqs.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                # Process successful messages
                result["success_count"] += len(response.get("Successful", []))
                
                # Process failed messages
                for failed in response.get("Failed", []):
                    result["failure_count"] += 1
                    
                    # Find the original data for this failed message
                    failed_id = int(failed["Id"])
                    original_idx = failed_id - i
                    if 0 <= original_idx < len(batch_chunk):
                        result["failures"].append({
                            "data": batch_chunk[original_idx],
                            "errors": [f"{failed['Code']}: {failed['Message']}"]
                        })
                    
            except Exception as e:
                logger.error(f"Error in batch ingestion: {e}")
                # Mark all entries in this batch as failed
                for metrics_data in batch_chunk:
                    result["failure_count"] += 1
                    result["failures"].append({
                        "data": metrics_data,
                        "errors": [str(e)]
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
        Check the health of the SQS queue.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        try:
            # Check if we can access the queue
            self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["ApproximateNumberOfMessages"]
            )
            
            return {
                "status": "healthy",
                "details": {
                    "queue_url": self.queue_url,
                    "region": self.region_name
                }
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "details": {
                    "error": str(e),
                    "queue_url": self.queue_url,
                    "region": self.region_name
                }
            }
