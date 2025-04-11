"""Base interfaces for visualization adapters."""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from metrics_pipeline.core.models.metrics import MetricsData


class VisualizationAdapter(ABC):
    """Base interface for all visualization adapters."""
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Connect to the visualization backend.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def create_dashboard(
        self, 
        name: str, 
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new dashboard in the visualization backend.
        
        Args:
            name: Name of the dashboard
            description: Optional description of the dashboard
            metadata: Optional metadata for the dashboard
            
        Returns:
            Dict[str, Any]: Dictionary containing dashboard information
                {
                    "id": str,
                    "name": str,
                    "url": str,
                    ...
                }
        """
        pass
    
    @abstractmethod
    async def create_visualization(
        self,
        dashboard_id: str,
        name: str,
        visualization_type: str,  # 'line', 'bar', 'pie', 'table', etc.
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
                {
                    "id": str,
                    "name": str,
                    "dashboard_id": str,
                    ...
                }
        """
        pass
    
    @abstractmethod
    async def get_dashboards(self) -> List[Dict[str, Any]]:
        """
        Get all dashboards from the visualization backend.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing dashboard information
        """
        pass
    
    @abstractmethod
    async def get_visualizations(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """
        Get all visualizations for the specified dashboard.
        
        Args:
            dashboard_id: ID of the dashboard to get visualizations for
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing visualization information
        """
        pass
    
    @abstractmethod
    async def export_dashboard(self, dashboard_id: str, format: str) -> bytes:
        """
        Export a dashboard in the specified format.
        
        Args:
            dashboard_id: ID of the dashboard to export
            format: Format to export the dashboard in ('pdf', 'png', 'json', etc.)
            
        Returns:
            bytes: Dashboard data in the specified format
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the visualization adapter.
        
        Returns:
            Dict[str, Any]: Health check results
                {
                    "status": str,  # "healthy", "degraded", or "unhealthy"
                    "details": Dict[str, Any]  # Additional details about the health check
                }
        """
        pass
