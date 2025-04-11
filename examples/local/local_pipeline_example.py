"""
Example script for running a local metrics pipeline with HTTP ingestion and in-memory storage.
This demonstrates the basic functionality of the metrics pipeline in a local environment.
"""
import asyncio
import logging
import datetime
import random
import json
from typing import Dict, Any, List

from metrics_pipeline.adapters.ingestion.http import HTTPIngestionAdapter
from metrics_pipeline.adapters.storage.memory import InMemoryStorageAdapter
from metrics_pipeline.adapters.visualization.mock import MockVisualizationAdapter
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

async def simulate_metrics_ingestion(pipeline: MetricsPipeline, duration_seconds: int = 60, interval_seconds: float = 1.0):
    """Simulate metrics ingestion for a specified duration."""
    logger.info(f"Starting metrics simulation for {duration_seconds} seconds")
    
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration_seconds)
    
    while datetime.datetime.now() < end_time:
        # Generate 1-5 random metrics
        metrics_batch = await generate_random_metrics(count=random.randint(1, 5))
        
        # Process metrics batch
        result = await pipeline.process_batch(metrics_batch)
        
        logger.info(f"Processed batch: {result['success_count']} succeeded, {result['failure_count']} failed")
        
        # Wait for the next interval
        await asyncio.sleep(interval_seconds)
    
    logger.info("Metrics simulation completed")

async def create_dashboard(storage_adapter: InMemoryStorageAdapter, viz_adapter: MockVisualizationAdapter):
    """Create a dashboard with visualizations based on the collected metrics."""
    logger.info("Creating dashboard and visualizations")
    
    # Connect to visualization backend
    await viz_adapter.connect()
    
    # Create dashboard
    dashboard = await viz_adapter.create_dashboard(
        name="Metrics Dashboard",
        description="Example dashboard for metrics visualization"
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
                    timestamp, 
                    tags['device'] as device,
                    metric_value as load_time
                FROM metrics
                WHERE metric_type = 'page_view'
                AND metric_name = 'load_time'
                ORDER BY timestamp
            """,
            "dimensions": ["timestamp", "device"],
            "metrics": ["load_time"]
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
    
    # Export dashboard
    export_data = await viz_adapter.export_dashboard(dashboard["id"], "json")
    export_json = json.loads(export_data.decode("utf-8"))
    
    logger.info(f"Dashboard created with {len(export_json['visualizations'])} visualizations")
    
    return dashboard

async def query_and_display_metrics(storage_adapter: InMemoryStorageAdapter):
    """Query and display metrics from the storage adapter."""
    logger.info("Querying metrics from storage")
    
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
    logger.info("Starting local metrics pipeline example")
    
    # Create adapters
    http_adapter = HTTPIngestionAdapter(
        api_url="http://localhost:8000/metrics",  # This is a mock URL for demonstration
        headers={"Content-Type": "application/json"},
        timeout=5
    )
    
    memory_adapter = InMemoryStorageAdapter()
    
    viz_adapter = MockVisualizationAdapter()
    
    # Create pipeline
    pipeline = MetricsPipeline(
        ingestion_adapter=http_adapter,
        storage_adapter=memory_adapter,
        batch_size=10,
        processing_interval=0.1
    )
    
    # Override the HTTP adapter's methods for local testing
    # This allows us to bypass actual HTTP requests
    http_adapter.ingest = lambda data: True
    http_adapter.validate = lambda data: {"valid": True, "errors": None}
    
    try:
        # Simulate metrics ingestion for 10 seconds
        await simulate_metrics_ingestion(pipeline, duration_seconds=10, interval_seconds=0.5)
        
        # Query and display metrics
        await query_and_display_metrics(memory_adapter)
        
        # Create dashboard and visualizations
        dashboard = await create_dashboard(memory_adapter, viz_adapter)
        
        logger.info(f"Example completed successfully. Dashboard URL: {dashboard['url']}")
        
    except Exception as e:
        logger.error(f"Error running example: {e}")
    
    logger.info("Local metrics pipeline example completed")

if __name__ == "__main__":
    asyncio.run(main())
