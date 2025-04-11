"""
Example script for running a metrics pipeline with AWS SQS ingestion and ClickHouse storage.
This demonstrates the AWS integration of the metrics pipeline.
"""
import asyncio
import logging
import datetime
import random
import json
import os
from typing import Dict, Any, List

from metrics_pipeline.adapters.ingestion.sqs import SQSIngestionAdapter
from metrics_pipeline.adapters.storage.clickhouse import ClickHouseStorageAdapter
from metrics_pipeline.adapters.visualization.metabase import MetabaseVisualizationAdapter
from metrics_pipeline.core.pipeline.processor import MetricsPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Sample page names for generating random metrics
PAGE_NAMES = ["home", "products", "cart", "checkout", "profile", "about", "contact"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
USER_TYPES = ["new", "returning"]
BUTTON_NAMES = ["add_to_cart", "checkout", "login", "register", "submit", "search"]

# AWS Configuration
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SQS_QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME", "metrics-queue")
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", None)  # For LocalStack

# ClickHouse Configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "metrics")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "metrics")

# Metabase Configuration
METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
METABASE_USERNAME = os.environ.get("METABASE_USERNAME", "admin@example.com")
METABASE_PASSWORD = os.environ.get("METABASE_PASSWORD", "password")
METABASE_DATABASE_ID = int(os.environ.get("METABASE_DATABASE_ID", "1"))

async def generate_random_metrics(count: int = 1) -> List[Dict[str, Any]]:
    """Generate random metrics data for testing."""
    metrics_batch = []
    
    for _ in range(count):
        # Randomly choose metric type
        metric_type = random.choice(["page_view", "click", "form_submit"])
        
        # Generate metrics based on type
        if metric_type == "page_view":
            metrics = [
                {
                    "name": "load_time",
                    "value": round(random.uniform(0.5, 5.0), 2),
                    "unit": "seconds",
                    "tags": {
                        "page": random.choice(PAGE_NAMES),
                        "device": random.choice(DEVICE_TYPES)
                    }
                },
                {
                    "name": "user_count",
                    "value": 1,
                    "tags": {
                        "user_type": random.choice(USER_TYPES)
                    }
                }
            ]
        elif metric_type == "click":
            metrics = [
                {
                    "name": "button_click",
                    "value": 1,
                    "tags": {
                        "button": random.choice(BUTTON_NAMES),
                        "page": random.choice(PAGE_NAMES),
                        "device": random.choice(DEVICE_TYPES)
                    }
                }
            ]
        else:  # form_submit
            metrics = [
                {
                    "name": "form_submit_time",
                    "value": round(random.uniform(0.2, 2.0), 2),
                    "unit": "seconds",
                    "tags": {
                        "form": "contact_form",
                        "page": "contact",
                        "device": random.choice(DEVICE_TYPES)
                    }
                }
            ]
        
        # Create metrics data
        metrics_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "metric_type": metric_type,
            "metrics": metrics,
            "source": "example_script",
            "context": {
                "user_agent": "Mozilla/5.0 (Example)",
                "ip": f"192.168.1.{random.randint(1, 255)}"
            }
        }
        
        metrics_batch.append(metrics_data)
    
    return metrics_batch

async def send_metrics_to_sqs(sqs_adapter: SQSIngestionAdapter, duration_seconds: int = 60, interval_seconds: float = 1.0):
    """Send metrics to SQS for a specified duration."""
    logger.info(f"Starting metrics sending to SQS for {duration_seconds} seconds")
    
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration_seconds)
    
    while datetime.datetime.now() < end_time:
        # Generate 1-5 random metrics
        metrics_batch = await generate_random_metrics(count=random.randint(1, 5))
        
        # Send metrics batch to SQS
        result = await sqs_adapter.batch_ingest(metrics_batch)
        
        logger.info(f"Sent batch to SQS: {result['success_count']} succeeded, {result['failure_count']} failed")
        
        # Wait for the next interval
        await asyncio.sleep(interval_seconds)
    
    logger.info("Metrics sending to SQS completed")

async def process_metrics_from_sqs(pipeline: MetricsPipeline, duration_seconds: int = 60):
    """Process metrics from SQS for a specified duration."""
    logger.info(f"Starting metrics processing from SQS for {duration_seconds} seconds")
    
    # Start the pipeline processing
    await pipeline.start_processing()
    
    # Let it run for the specified duration
    await asyncio.sleep(duration_seconds)
    
    # Stop the pipeline processing
    await pipeline.stop_processing()
    
    logger.info("Metrics processing from SQS completed")

async def create_metabase_dashboard(storage_adapter: ClickHouseStorageAdapter, viz_adapter: MetabaseVisualizationAdapter):
    """Create a Metabase dashboard with visualizations based on the collected metrics."""
    logger.info("Creating Metabase dashboard and visualizations")
    
    # Connect to Metabase
    connected = await viz_adapter.connect()
    if not connected:
        logger.error("Failed to connect to Metabase")
        return None
    
    # Create dashboard
    dashboard = await viz_adapter.create_dashboard(
        name="AWS Metrics Dashboard",
        description="Example dashboard for AWS metrics visualization"
    )
    logger.info(f"Created dashboard: {dashboard['name']} (ID: {dashboard['id']})")
    
    # Create page view visualization
    await viz_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Page Load Time by Device",
        visualization_type="line",
        query={
            "sql": """
                SELECT 
                    toStartOfMinute(timestamp) as minute, 
                    tags['device'] as device,
                    avg(metric_value) as avg_load_time
                FROM metrics
                WHERE metric_type = 'page_view'
                AND metric_name = 'load_time'
                GROUP BY minute, device
                ORDER BY minute
            """,
            "dimensions": ["minute", "device"],
            "metrics": ["avg_load_time"]
        },
        description="Average page load time by device type"
    )
    
    # Create click visualization
    await viz_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="Button Clicks by Page",
        visualization_type="bar",
        query={
            "sql": """
                SELECT 
                    tags['page'] as page,
                    tags['button'] as button,
                    count(*) as click_count
                FROM metrics
                WHERE metric_type = 'click'
                AND metric_name = 'button_click'
                GROUP BY page, button
                ORDER BY click_count DESC
                LIMIT 10
            """,
            "dimensions": ["page", "button"],
            "metrics": ["click_count"]
        },
        description="Number of button clicks by page"
    )
    
    # Create user count visualization
    await viz_adapter.create_visualization(
        dashboard_id=dashboard["id"],
        name="User Count by Type",
        visualization_type="pie",
        query={
            "sql": """
                SELECT 
                    tags['user_type'] as user_type,
                    sum(metric_value) as user_count
                FROM metrics
                WHERE metric_type = 'page_view'
                AND metric_name = 'user_count'
                GROUP BY user_type
            """,
            "dimensions": ["user_type"],
            "metrics": ["user_count"]
        },
        description="User count by user type"
    )
    
    logger.info(f"Dashboard created with 3 visualizations")
    
    return dashboard

async def query_clickhouse_metrics(storage_adapter: ClickHouseStorageAdapter):
    """Query and display metrics from ClickHouse."""
    logger.info("Querying metrics from ClickHouse")
    
    # Query page view metrics
    page_views = await storage_adapter.query(
        metric_type="page_view",
        start_time=datetime.datetime.now() - datetime.timedelta(hours=1),
        end_time=datetime.datetime.now()
    )
    
    logger.info(f"Found {len(page_views)} page view metrics")
    
    # Query click metrics
    clicks = await storage_adapter.query(
        metric_type="click",
        start_time=datetime.datetime.now() - datetime.timedelta(hours=1),
        end_time=datetime.datetime.now()
    )
    
    logger.info(f"Found {len(clicks)} click metrics")
    
    # Aggregate load time by device
    load_time_by_device = await storage_adapter.aggregate(
        metric_type="page_view",
        metric_name="load_time",
        aggregation="avg",
        group_by=["tags.device"]
    )
    
    logger.info("Average load time by device:")
    if "results" in load_time_by_device:
        for result in load_time_by_device["results"]:
            logger.info(f"  {result['tags.device']}: {result['value']:.2f} seconds")
    
    # Aggregate clicks by button
    clicks_by_button = await storage_adapter.aggregate(
        metric_type="click",
        metric_name="button_click",
        aggregation="count",
        group_by=["tags.button"]
    )
    
    logger.info("Clicks by button:")
    if "results" in clicks_by_button:
        for result in clicks_by_button["results"]:
            logger.info(f"  {result['tags.button']}: {result['value']} clicks")

async def main():
    """Main function to run the example."""
    logger.info("Starting AWS metrics pipeline example")
    
    # Create adapters
    sqs_adapter = SQSIngestionAdapter(
        queue_name=SQS_QUEUE_NAME,
        region_name=AWS_REGION,
        endpoint_url=AWS_ENDPOINT_URL
    )
    
    clickhouse_adapter = ClickHouseStorageAdapter(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        table=CLICKHOUSE_TABLE,
        create_table_if_not_exists=True
    )
    
    metabase_adapter = MetabaseVisualizationAdapter(
        url=METABASE_URL,
        username=METABASE_USERNAME,
        password=METABASE_PASSWORD,
        database_id=METABASE_DATABASE_ID
    )
    
    # Create pipeline
    pipeline = MetricsPipeline(
        ingestion_adapter=sqs_adapter,
        storage_adapter=clickhouse_adapter,
        batch_size=10,
        processing_interval=1.0
    )
    
    try:
        # Check health of all components
        sqs_health = await sqs_adapter.health_check()
        clickhouse_health = await clickhouse_adapter.health_check()
        
        logger.info(f"SQS health: {sqs_health['status']}")
        logger.info(f"ClickHouse health: {clickhouse_health['status']}")
        
        if sqs_health['status'] != 'healthy' or clickhouse_health['status'] != 'healthy':
            logger.error("One or more components are not healthy. Please check configuration.")
            return
        
        # Send metrics to SQS for 30 seconds
        await send_metrics_to_sqs(sqs_adapter, duration_seconds=30, interval_seconds=1.0)
        
        # Process metrics from SQS for 60 seconds
        await process_metrics_from_sqs(pipeline, duration_seconds=60)
        
        # Query and display metrics
        await query_clickhouse_metrics(clickhouse_adapter)
        
        # Create Metabase dashboard and visualizations
        dashboard = await create_metabase_dashboard(clickhouse_adapter, metabase_adapter)
        
        if dashboard:
            logger.info(f"Example completed successfully. Dashboard URL: {dashboard['url']}")
        else:
            logger.warning("Example completed but dashboard creation failed.")
        
    except Exception as e:
        logger.error(f"Error running example: {e}")
    
    logger.info("AWS metrics pipeline example completed")

if __name__ == "__main__":
    asyncio.run(main())
