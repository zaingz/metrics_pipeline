"""Metabase implementation of the visualization adapter."""
import json
import logging
import requests
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from metrics_pipeline.adapters.visualization.base import VisualizationAdapter

logger = logging.getLogger(__name__)

class MetabaseVisualizationAdapter(VisualizationAdapter):
    """Visualization adapter that uses Metabase for metrics visualization."""
    
    def __init__(
        self, 
        url: str,
        username: str,
        password: str,
        database_id: Optional[int] = None,
        session_timeout: int = 3600  # 1 hour
    ):
        """
        Initialize the Metabase visualization adapter.
        
        Args:
            url: URL of the Metabase instance
            username: Metabase username
            password: Metabase password
            database_id: Optional ID of the database to use
            session_timeout: Session timeout in seconds
        """
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.database_id = database_id
        self.session_timeout = session_timeout
        self.session_token = None
        self.session_expiry = None
    
    async def connect(self) -> bool:
        """
        Connect to the Metabase instance.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        try:
            # Check if session is still valid
            if self.session_token and self.session_expiry and datetime.now() < self.session_expiry:
                return True
            
            # Authenticate with Metabase
            response = requests.post(
                f"{self.url}/api/session",
                json={
                    "username": self.username,
                    "password": self.password
                }
            )
            
            response.raise_for_status()
            data = response.json()
            
            self.session_token = data.get("id")
            if not self.session_token:
                logger.error("Failed to get session token from Metabase")
                return False
            
            # Set session expiry
            self.session_expiry = datetime.now().timestamp() + self.session_timeout
            
            # Get database ID if not provided
            if not self.database_id:
                databases = await self._get_databases()
                if databases:
                    self.database_id = databases[0]["id"]
            
            logger.info(f"Connected to Metabase at {self.url}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Metabase: {e}")
            return False
    
    async def create_dashboard(
        self, 
        name: str, 
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new dashboard in Metabase.
        
        Args:
            name: Name of the dashboard
            description: Optional description of the dashboard
            metadata: Optional metadata for the dashboard
            
        Returns:
            Dict[str, Any]: Dictionary containing dashboard information
        """
        try:
            # Ensure we're connected
            if not await self.connect():
                raise Exception("Failed to connect to Metabase")
            
            # Create dashboard
            response = requests.post(
                f"{self.url}/api/dashboard",
                headers=self._get_headers(),
                json={
                    "name": name,
                    "description": description or "",
                    "parameters": [],
                    "collection_id": None
                }
            )
            
            response.raise_for_status()
            dashboard = response.json()
            
            # Add metadata if provided
            if metadata:
                dashboard_id = dashboard["id"]
                await self._update_dashboard_metadata(dashboard_id, metadata)
            
            # Add dashboard URL
            dashboard["url"] = f"{self.url}/dashboard/{dashboard['id']}"
            
            logger.info(f"Created dashboard: {name} (ID: {dashboard['id']})")
            return dashboard
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return {
                "error": str(e),
                "name": name
            }
    
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
        try:
            # Ensure we're connected
            if not await self.connect():
                raise Exception("Failed to connect to Metabase")
            
            # Ensure database_id is set
            if not self.database_id:
                raise Exception("Database ID is not set")
            
            # Map visualization type to Metabase display type
            display_type = self._map_visualization_type(visualization_type)
            
            # Create question (card)
            card_data = {
                "name": name,
                "display": display_type,
                "visualization_settings": {},
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": query.get("sql", ""),
                        "template-tags": {}
                    },
                    "database": self.database_id
                },
                "description": description or "",
                "collection_id": None
            }
            
            # Add visualization-specific settings
            if visualization_type in ["line", "bar", "area"]:
                card_data["visualization_settings"] = {
                    "graph.dimensions": query.get("dimensions", []),
                    "graph.metrics": query.get("metrics", [])
                }
            
            response = requests.post(
                f"{self.url}/api/card",
                headers=self._get_headers(),
                json=card_data
            )
            
            response.raise_for_status()
            card = response.json()
            
            # Add card to dashboard
            response = requests.post(
                f"{self.url}/api/dashboard/{dashboard_id}/cards",
                headers=self._get_headers(),
                json={
                    "cardId": card["id"],
                    "row": 0,
                    "col": 0,
                    "sizeX": 6,
                    "sizeY": 4
                }
            )
            
            response.raise_for_status()
            dashboard_card = response.json()
            
            # Add metadata if provided
            if metadata:
                await self._update_card_metadata(card["id"], metadata)
            
            # Combine card and dashboard_card information
            result = {
                "id": card["id"],
                "dashboard_id": dashboard_id,
                "dashboard_card_id": dashboard_card["id"],
                "name": name,
                "url": f"{self.url}/question/{card['id']}"
            }
            
            logger.info(f"Created visualization: {name} (ID: {card['id']})")
            return result
        except Exception as e:
            logger.error(f"Error creating visualization: {e}")
            return {
                "error": str(e),
                "name": name,
                "dashboard_id": dashboard_id
            }
    
    async def get_dashboards(self) -> List[Dict[str, Any]]:
        """
        Get all dashboards from Metabase.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing dashboard information
        """
        try:
            # Ensure we're connected
            if not await self.connect():
                raise Exception("Failed to connect to Metabase")
            
            # Get dashboards
            response = requests.get(
                f"{self.url}/api/dashboard",
                headers=self._get_headers()
            )
            
            response.raise_for_status()
            dashboards = response.json()
            
            # Add URLs
            for dashboard in dashboards:
                dashboard["url"] = f"{self.url}/dashboard/{dashboard['id']}"
            
            return dashboards
        except Exception as e:
            logger.error(f"Error getting dashboards: {e}")
            return []
    
    async def get_visualizations(self, dashboard_id: str) -> List[Dict[str, Any]]:
        """
        Get all visualizations for the specified dashboard.
        
        Args:
            dashboard_id: ID of the dashboard to get visualizations for
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing visualization information
        """
        try:
            # Ensure we're connected
            if not await self.connect():
                raise Exception("Failed to connect to Metabase")
            
            # Get dashboard
            response = requests.get(
                f"{self.url}/api/dashboard/{dashboard_id}",
                headers=self._get_headers()
            )
            
            response.raise_for_status()
            dashboard = response.json()
            
            # Extract cards
            cards = []
            for card in dashboard.get("ordered_cards", []):
                card_data = card.get("card", {})
                cards.append({
                    "id": card_data.get("id"),
                    "dashboard_id": dashboard_id,
                    "dashboard_card_id": card.get("id"),
                    "name": card_data.get("name"),
                    "description": card_data.get("description"),
                    "url": f"{self.url}/question/{card_data.get('id')}"
                })
            
            return cards
        except Exception as e:
            logger.error(f"Error getting visualizations: {e}")
            return []
    
    async def export_dashboard(self, dashboard_id: str, format: str) -> bytes:
        """
        Export a dashboard in the specified format.
        
        Args:
            dashboard_id: ID of the dashboard to export
            format: Format to export the dashboard in ('pdf', 'png', 'json')
            
        Returns:
            bytes: Dashboard data in the specified format
        """
        try:
            # Ensure we're connected
            if not await self.connect():
                raise Exception("Failed to connect to Metabase")
            
            if format.lower() == "json":
                # Export dashboard as JSON
                response = requests.get(
                    f"{self.url}/api/dashboard/{dashboard_id}",
                    headers=self._get_headers()
                )
                
                response.raise_for_status()
                dashboard = response.json()
                
                return json.dumps(dashboard).encode("utf-8")
            elif format.lower() in ["pdf", "png"]:
                # Export dashboard as PDF or PNG
                # Note: This is a simplified implementation and may not work with all Metabase versions
                response = requests.post(
                    f"{self.url}/api/dashboard/{dashboard_id}/export",
                    headers=self._get_headers(),
                    json={
                        "format": format.lower()
                    }
                )
                
                response.raise_for_status()
                return response.content
            else:
                raise ValueError(f"Unsupported export format: {format}")
        except Exception as e:
            logger.error(f"Error exporting dashboard: {e}")
            return b""
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the Metabase connection.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        try:
            # Try to connect
            if not await self.connect():
                return {
                    "status": "unhealthy",
                    "details": {
                        "url": self.url,
                        "message": "Failed to connect to Metabase"
                    }
                }
            
            # Check if we can get dashboards
            response = requests.get(
                f"{self.url}/api/dashboard",
                headers=self._get_headers()
            )
            
            if response.status_code >= 400:
                return {
                    "status": "degraded",
                    "details": {
                        "url": self.url,
                        "status_code": response.status_code,
                        "message": "Failed to get dashboards"
                    }
                }
            
            return {
                "status": "healthy",
                "details": {
                    "url": self.url,
                    "database_id": self.database_id
                }
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "details": {
                    "url": self.url,
                    "error": str(e)
                }
            }
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Metabase API requests."""
        return {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.session_token
        }
    
    async def _get_databases(self) -> List[Dict[str, Any]]:
        """Get all databases from Metabase."""
        try:
            response = requests.get(
                f"{self.url}/api/database",
                headers=self._get_headers()
            )
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting databases: {e}")
            return []
    
    async def _update_dashboard_metadata(self, dashboard_id: str, metadata: Dict[str, Any]) -> bool:
        """Update dashboard metadata."""
        try:
            response = requests.put(
                f"{self.url}/api/dashboard/{dashboard_id}",
                headers=self._get_headers(),
                json={
                    "metadata": metadata
                }
            )
            
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error updating dashboard metadata: {e}")
            return False
    
    async def _update_card_metadata(self, card_id: int, metadata: Dict[str, Any]) -> bool:
        """Update card metadata."""
        try:
            response = requests.put(
                f"{self.url}/api/card/{card_id}",
                headers=self._get_headers(),
                json={
                    "metadata": metadata
                }
            )
            
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Error updating card metadata: {e}")
            return False
    
    def _map_visualization_type(self, visualization_type: str) -> str:
        """Map visualization type to Metabase display type."""
        mapping = {
            "line": "line",
            "bar": "bar",
            "pie": "pie",
            "table": "table",
            "number": "scalar",
            "map": "map",
            "scatter": "scatter",
            "area": "area",
            "funnel": "funnel"
        }
        
        return mapping.get(visualization_type.lower(), "table")
