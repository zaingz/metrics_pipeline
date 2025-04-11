"""Mock implementation of the visualization adapter for testing."""
import logging
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from metrics_pipeline.adapters.visualization.base import VisualizationAdapter

logger = logging.getLogger(__name__)

class MockVisualizationAdapter(VisualizationAdapter):
    """Visualization adapter that uses in-memory storage for testing."""
    
    def __init__(self):
        """Initialize the mock visualization adapter."""
        self.dashboards = {}  # dashboard_id -> dashboard
        self.visualizations = {}  # dashboard_id -> [visualizations]
        self.connected = False
    
    async def connect(self) -> bool:
        """
        Connect to the mock visualization backend.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        self.connected = True
        logger.info("Connected to mock visualization backend")
        return True
    
    async def create_dashboard(
        self, 
        name: str, 
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new dashboard in the mock backend.
        
        Args:
            name: Name of the dashboard
            description: Optional description of the dashboard
            metadata: Optional metadata for the dashboard
            
        Returns:
            Dict[str, Any]: Dictionary containing dashboard information
        """
        if not self.connected:
            await self.connect()
        
        dashboard_id = f"dashboard_{len(self.dashboards) + 1}"
        dashboard = {
            "id": dashboard_id,
            "name": name,
            "description": description or "",
            "metadata": metadata or {},
            "created_at": datetime.now().isoformat(),
            "url": f"mock://dashboard/{dashboard_id}"
        }
        
        self.dashboards[dashboard_id] = dashboard
        self.visualizations[dashboard_id] = []
        
        logger.info(f"Created mock dashboard: {name} (ID: {dashboard_id})")
        return dashboard
    
    async def create_visualization(
        self,
        dashboard_id: str,
        name: str,
        visualization_type: str,
        query: Dict[str, Any],
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new visualization in the specified dashboard.
        
        Args:
            dashboard_id: ID of the dashboard to add the visualization to
            name: Name of the visualization
            visualization_type: Type of visualization
            query: Query to generate the visualization data
            description: Optional description of the visualization
            metadata: Optional metadata for the visualization
            
        Returns:
            Dict[str, Any]: Dictionary containing visualization information
        """
        if not self.connected:
            await self.connect()
        
        if dashboard_id not in self.dashboards:
            logger.error(f"Dashboard not found: {dashboard_id}")
            return {"error": f"Dashboard not found: {dashboard_id}"}
        
        visualization_id = f"viz_{len(self.visualizations.get(dashboard_id, [])) + 1}"
        visualization = {
            "id": visualization_id,
            "dashboard_id": dashboard_id,
            "name": name,
            "visualization_type": visualization_type,
            "query": query,
            "description": description or "",
            "metadata": metadata or {},
            "created_at": datetime.now().isoformat(),
            "url": f"mock://visualization/{visualization_id}"
        }
        
        if dashboard_id not in self.visualizations:
            self.visualizations[dashboard_id] = []
        
        self.visualizations[dashboard_id].append(visualization)
        
        logger.info(f"Created mock visualization: {name} (ID: {visualization_id})")
        return visualization
    
    async def get_dashboards(self) -> List[Dict[str, Any]]:
        """
        Get all dashboards from the mock backend.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing dashboard information
        """
        if not self.connected:
            await self.connect()
        
        return list(self.dashboards.values())
    
    async def get_visualizations(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """
        Get all visualizations for the specified dashboard.
        
        Args:
            dashboard_id: ID of the dashboard to get visualizations for
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing visualization information
        """
        if not self.connected:
            await self.connect()
        
        if dashboard_id not in self.dashboards:
            logger.error(f"Dashboard not found: {dashboard_id}")
            return []
        
        return self.visualizations.get(dashboard_id, [])
    
    async def export_dashboard(self, dashboard_id: str, format: str) -> bytes:
        """
        Export a dashboard in the specified format.
        
        Args:
            dashboard_id: ID of the dashboard to export
            format: Format to export the dashboard in ('pdf', 'png', 'json')
            
        Returns:
            bytes: Dashboard data in the specified format
        """
        if not self.connected:
            await self.connect()
        
        if dashboard_id not in self.dashboards:
            logger.error(f"Dashboard not found: {dashboard_id}")
            return b""
        
        if format.lower() == "json":
            dashboard_data = {
                "dashboard": self.dashboards[dashboard_id],
                "visualizations": self.visualizations.get(dashboard_id, [])
            }
            return json.dumps(dashboard_data).encode("utf-8")
        else:
            # Mock PDF or PNG export
            return f"Mock {format.upper()} export for dashboard {dashboard_id}".encode("utf-8")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the mock visualization adapter.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        return {
            "status": "healthy" if self.connected else "unhealthy",
            "details": {
                "type": "mock",
                "dashboards_count": len(self.dashboards),
                "visualizations_count": sum(len(vizs) for vizs in self.visualizations.values())
            }
        }
