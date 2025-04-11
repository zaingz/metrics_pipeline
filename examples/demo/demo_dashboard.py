"""
Demo script for showcasing the metrics pipeline functionality.
This script demonstrates a complete end-to-end workflow with a web dashboard.
"""
import asyncio
import logging
import datetime
import random
import json
import os
import argparse
from typing import Dict, Any, List, Optional
import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from metrics_pipeline.adapters.ingestion.http import HTTPIngestionAdapter
from metrics_pipeline.adapters.storage.memory import InMemoryStorageAdapter
from metrics_pipeline.adapters.visualization.mock import MockVisualizationAdapter
from metrics_pipeline.core.pipeline.processor import MetricsPipeline
from metrics_pipeline.core.models.metrics import Metric, MetricsData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Metrics Pipeline Demo")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create adapters
memory_adapter = InMemoryStorageAdapter()
viz_adapter = MockVisualizationAdapter()

# Create HTTP adapter for the demo API
http_adapter = HTTPIngestionAdapter(
    api_url="http://localhost:8000/api/metrics",
    headers={"Content-Type": "application/json"},
    timeout=5
)

# Create pipeline
pipeline = MetricsPipeline(
    ingestion_adapter=http_adapter,
    storage_adapter=memory_adapter,
    batch_size=10,
    processing_interval=1.0
)

# Sample data for generating random metrics
PAGE_NAMES = ["home", "products", "cart", "checkout", "profile", "about", "contact"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
USER_TYPES = ["new", "returning"]
BUTTON_NAMES = ["add_to_cart", "checkout", "login", "register", "submit", "search"]

# Dashboard data
dashboard_id = None
dashboard_url = None

# Connect templates
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def startup_event():
    """Initialize the pipeline and create dashboard on startup."""
    global dashboard_id, dashboard_url
    
    # Connect to visualization backend
    await viz_adapter.connect()
    
    # Create dashboard
    dashboard = await viz_adapter.create_dashboard(
        name="Demo Dashboard",
        description="Real-time metrics dashboard for demo"
    )
    dashboard_id = dashboard["id"]
    dashboard_url = dashboard["url"]
    
    # Create visualizations
    await viz_adapter.create_visualization(
        dashboard_id=dashboard_id,
        name="Page Views by Device",
        visualization_type="bar",
        query={
            "sql": """
                SELECT 
                    tags['device'] as device,
                    count(*) as view_count
                FROM metrics
                WHERE metric_type = 'page_view'
                GROUP BY device
            """,
            "dimensions": ["device"],
            "metrics": ["view_count"]
        }
    )
    
    await viz_adapter.create_visualization(
        dashboard_id=dashboard_id,
        name="Button Clicks",
        visualization_type="pie",
        query={
            "sql": """
                SELECT 
                    tags['button'] as button,
                    count(*) as click_count
                FROM metrics
                WHERE metric_type = 'click'
                GROUP BY button
            """,
            "dimensions": ["button"],
            "metrics": ["click_count"]
        }
    )
    
    # Start the pipeline
    await pipeline.start_processing()
    logger.info("Metrics pipeline started")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop the pipeline on shutdown."""
    await pipeline.stop_processing()
    logger.info("Metrics pipeline stopped")

@app.get("/")
async def get_dashboard(request: Request):
    """Render the dashboard page."""
    return templates.TemplateResponse(
        "dashboard.html", 
        {"request": request, "dashboard_id": dashboard_id}
    )

@app.post("/api/metrics")
async def ingest_metrics(metrics_data: Dict[str, Any]):
    """API endpoint for ingesting metrics."""
    # Override the HTTP adapter's methods for the demo
    # This allows us to process metrics directly
    success = await pipeline.process_metrics(metrics_data)
    
    if success:
        return JSONResponse(status_code=200, content={"status": "success"})
    else:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid metrics data"})

@app.get("/api/metrics")
async def get_metrics(
    metric_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 100
):
    """API endpoint for querying metrics."""
    # Convert string timestamps to datetime if provided
    start_dt = None
    end_dt = None
    
    if start_time:
        start_dt = datetime.datetime.fromisoformat(start_time)
    
    if end_time:
        end_dt = datetime.datetime.fromisoformat(end_time)
    
    # Query metrics from storage
    metrics = await memory_adapter.query(
        metric_type=metric_type,
        start_time=start_dt,
        end_time=end_dt,
        limit=limit
    )
    
    # Convert to dict for JSON response
    result = [m.dict() for m in metrics]
    
    return JSONResponse(status_code=200, content=result)

@app.get("/api/dashboard")
async def get_dashboard_data():
    """API endpoint for getting dashboard data."""
    if not dashboard_id:
        return JSONResponse(status_code=404, content={"status": "error", "message": "Dashboard not found"})
    
    # Get dashboard visualizations
    visualizations = await viz_adapter.get_visualizations(dashboard_id)
    
    # Get metrics counts
    page_views = await memory_adapter.query(metric_type="page_view")
    clicks = await memory_adapter.query(metric_type="click")
    
    # Get aggregated data
    device_breakdown = await memory_adapter.aggregate(
        metric_type="page_view",
        metric_name="user_count",
        aggregation="sum",
        group_by=["tags.device"]
    )
    
    button_breakdown = await memory_adapter.aggregate(
        metric_type="click",
        metric_name="button_click",
        aggregation="count",
        group_by=["tags.button"]
    )
    
    return JSONResponse(status_code=200, content={
        "dashboard_id": dashboard_id,
        "dashboard_url": dashboard_url,
        "visualizations": visualizations,
        "metrics_summary": {
            "page_view_count": len(page_views),
            "click_count": len(clicks),
            "device_breakdown": device_breakdown.get("results", []),
            "button_breakdown": button_breakdown.get("results", [])
        }
    })

@app.post("/api/simulate")
async def simulate_metrics(background_tasks: BackgroundTasks, duration: int = 30, rate: float = 1.0):
    """API endpoint for simulating metrics data."""
    background_tasks.add_task(simulate_metrics_task, duration, rate)
    return JSONResponse(status_code=200, content={
        "status": "success", 
        "message": f"Started metrics simulation for {duration} seconds at {rate} metrics per second"
    })

async def simulate_metrics_task(duration: int = 30, rate: float = 1.0):
    """Background task for simulating metrics data."""
    logger.info(f"Starting metrics simulation for {duration} seconds at {rate} metrics per second")
    
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=duration)
    
    while datetime.datetime.now() < end_time:
        # Generate random metrics
        metric_type = random.choice(["page_view", "click"])
        
        if metric_type == "page_view":
            metrics_data = {
                "timestamp": datetime.datetime.now().isoformat(),
                "metric_type": "page_view",
                "metrics": [
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
                ],
                "source": "demo_simulation"
            }
        else:  # click
            metrics_data = {
                "timestamp": datetime.datetime.now().isoformat(),
                "metric_type": "click",
                "metrics": [
                    {
                        "name": "button_click",
                        "value": 1,
                        "tags": {
                            "button": random.choice(BUTTON_NAMES),
                            "page": random.choice(PAGE_NAMES),
                            "device": random.choice(DEVICE_TYPES)
                        }
                    }
                ],
                "source": "demo_simulation"
            }
        
        # Process metrics
        await pipeline.process_metrics(metrics_data)
        
        # Wait according to rate
        await asyncio.sleep(1.0 / rate)
    
    logger.info("Metrics simulation completed")

def create_templates():
    """Create template files for the demo."""
    os.makedirs("templates", exist_ok=True)
    
    # Create dashboard.html template
    dashboard_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Metrics Pipeline Demo</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {
                padding-top: 20px;
                background-color: #f8f9fa;
            }
            .card {
                margin-bottom: 20px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            .metric-value {
                font-size: 2rem;
                font-weight: bold;
            }
            .refresh-btn {
                position: absolute;
                top: 10px;
                right: 10px;
            }
            .chart-container {
                position: relative;
                height: 250px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="row mb-4">
                <div class="col">
                    <h1 class="display-4">Metrics Pipeline Demo</h1>
                    <p class="lead">Real-time metrics dashboard showcasing the metrics pipeline functionality</p>
                </div>
                <div class="col-auto">
                    <button id="simulate-btn" class="btn btn-primary">Simulate Traffic</button>
                </div>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Page Views</h5>
                            <p class="metric-value" id="page-view-count">0</p>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Button Clicks</h5>
                            <p class="metric-value" id="click-count">0</p>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Last Updated</h5>
                            <p class="metric-value" id="last-updated">-</p>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Page Views by Device</h5>
                            <div class="chart-container">
                                <canvas id="device-chart"></canvas>
                            </div>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Button Clicks</h5>
                            <div class="chart-container">
                                <canvas id="button-chart"></canvas>
                            </div>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="row">
                <div class="col">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Recent Metrics</h5>
                            <div class="table-responsive">
                                <table class="table table-striped">
                                    <thead>
                                        <tr>
                                            <th>Timestamp</th>
                                            <th>Type</th>
                                            <th>Metric</th>
                                            <th>Value</th>
                                            <th>Tags</th>
                                        </tr>
                                    </thead>
                                    <tbody id="metrics-table">
                                        <tr>
                                            <td colspan="5" class="text-center">No data available</td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <button class="btn btn-sm btn-outline-secondary refresh-btn" onclick="refreshData()">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
                                    <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
                                    <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            // Chart objects
            let deviceChart = null;
            let buttonChart = null;
            
            // Initialize charts
            function initCharts() {
                const deviceCtx = document.getElementById('device-chart').getContext('2d');
                deviceChart = new Chart(deviceCtx, {
                    type: 'bar',
                    data: {
                        labels: [],
                        datasets: [{
                            label: 'Page Views by Device',
                            data: [],
                            backgroundColor: [
                                'rgba(54, 162, 235, 0.5)',
                                'rgba(255, 99, 132, 0.5)',
                                'rgba(255, 206, 86, 0.5)'
                            ],
                            borderColor: [
                                'rgba(54, 162, 235, 1)',
                                'rgba(255, 99, 132, 1)',
                                'rgba(255, 206, 86, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true,
                                ticks: {
                                    precision: 0
                                }
                            }
                        }
                    }
                });
                
                const buttonCtx = document.getElementById('button-chart').getContext('2d');
                buttonChart = new Chart(buttonCtx, {
                    type: 'pie',
                    data: {
                        labels: [],
                        datasets: [{
                            label: 'Button Clicks',
                            data: [],
                            backgroundColor: [
                                'rgba(54, 162, 235, 0.5)',
                                'rgba(255, 99, 132, 0.5)',
                                'rgba(255, 206, 86, 0.5)',
                                'rgba(75, 192, 192, 0.5)',
                                'rgba(153, 102, 255, 0.5)',
                                'rgba(255, 159, 64, 0.5)'
                            ],
                            borderColor: [
                                'rgba(54, 162, 235, 1)',
                                'rgba(255, 99, 132, 1)',
                                'rgba(255, 206, 86, 1)',
                                'rgba(75, 192, 192, 1)',
                                'rgba(153, 102, 255, 1)',
                                'rgba(255, 159, 64, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false
                    }
                });
            }
            
            // Refresh dashboard data
            async function refreshData() {
                try {
                    const response = await fetch('/api/dashboard');
                    const data = await response.json();
                    
                    // Update summary metrics
                    document.getElementById('page-view-count').textContent = data.metrics_summary.page_view_count;
                    document.getElementById('click-count').textContent = data.metrics_summary.click_count;
                    document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
                    
                    // Update device chart
                    if (data.metrics_summary.device_breakdown && data.metrics_summary.device_breakdown.length > 0) {
                        const deviceLabels = data.metrics_summary.device_breakdown.map(item => item['tags.device']);
                        const deviceData = data.metrics_summary.device_breakdown.map(item => item.value);
                        
                        deviceChart.data.labels = deviceLabels;
                        deviceChart.data.datasets[0].data = deviceData;
                        deviceChart.update();
                    }
                    
                    // Update button chart
                    if (data.metrics_summary.button_breakdown && data.metrics_summary.button_breakdown.length > 0) {
                        const buttonLabels = data.metrics_summary.button_breakdown.map(item => item['tags.button']);
                        const buttonData = data.metrics_summary.button_breakdown.map(item => item.value);
                        
                        buttonChart.data.labels = buttonLabels;
                        buttonChart.data.datasets[0].data = buttonData;
                        buttonChart.update();
                    }
                    
                    // Fetch recent metrics
                    const metricsResponse = await fetch('/api/metrics?limit=10');
                    const metricsData = await metricsResponse.json();
                    
                    // Update metrics table
                    const tableBody = document.getElementById('metrics-table');
                    tableBody.innerHTML = '';
                    
                    if (metricsData.length === 0) {
                        tableBody.innerHTML = '<tr><td colspan="5" class="text-center">No data available</td></tr>';
                    } else {
                        metricsData.forEach(metricData => {
                            metricData.metrics.forEach(metric => {
                                const row = document.createElement('tr');
                                
                                // Format timestamp
                                const timestamp = new Date(metricData.timestamp);
                                const formattedTime = timestamp.toLocaleTimeString();
                                
                                // Format tags
                                const tags = Object.entries(metric.tags || {})
                                    .map(([key, value]) => `${key}: ${value}`)
                                    .join(', ');
                                
                                row.innerHTML = `
                                    <td>${formattedTime}</td>
                                    <td>${metricData.metric_type}</td>
                                    <td>${metric.name}</td>
                                    <td>${metric.value} ${metric.unit || ''}</td>
                                    <td>${tags}</td>
                                `;
                                
                                tableBody.appendChild(row);
                            });
                        });
                    }
                    
                } catch (error) {
                    console.error('Error refreshing data:', error);
                }
            }
            
            // Simulate traffic
            async function simulateTraffic() {
                try {
                    const response = await fetch('/api/simulate', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            duration: 60,
                            rate: 2.0
                        })
                    });
                    
                    const data = await response.json();
                    console.log(data.message);
                    
                    // Disable button during simulation
                    const simulateBtn = document.getElementById('simulate-btn');
                    simulateBtn.disabled = true;
                    simulateBtn.textContent = 'Simulation Running...';
                    
                    // Re-enable after simulation completes
                    setTimeout(() => {
                        simulateBtn.disabled = false;
                        simulateBtn.textContent = 'Simulate Traffic';
                        refreshData();
                    }, 60000);
                    
                    // Refresh data periodically during simulation
                    const refreshInterval = setInterval(refreshData, 5000);
                    setTimeout(() => clearInterval(refreshInterval), 65000);
                    
                } catch (error) {
                    console.error('Error simulating traffic:', error);
                }
            }
            
            // Initialize on page load
            document.addEventListener('DOMContentLoaded', () => {
                initCharts();
                refreshData();
                
                // Set up simulate button
                document.getElementById('simulate-btn').addEventListener('click', simulateTraffic);
                
                // Refresh data every 30 seconds
                setInterval(refreshData, 30000);
            });
        </script>
    </body>
    </html>
    """
    
    with open("templates/dashboard.html", "w") as f:
        f.write(dashboard_html)

def main():
    """Main function to run the demo."""
    parser = argparse.ArgumentParser(description="Metrics Pipeline Demo")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the demo server on")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to run the demo server on")
    args = parser.parse_args()
    
    # Create template files
    create_templates()
    
    # Run the FastAPI app
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
