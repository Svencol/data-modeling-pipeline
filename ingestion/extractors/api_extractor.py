"""
API Extractor Module

Extracts data from REST APIs with support for pagination,
authentication, and rate limiting.
"""

from typing import Optional, Any
import pandas as pd
import requests
import time

from . import BaseExtractor, ExtractionError


class APIExtractor(BaseExtractor):
    """Extractor for REST API sources."""
    
    def __init__(
        self,
        base_url: str,
        endpoint: str,
        source_name: str,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        auth: Optional[tuple[str, str]] = None,
        pagination_key: Optional[str] = None,
        data_key: Optional[str] = None,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize API extractor.
        
        Args:
            base_url: Base URL for the API
            endpoint: API endpoint path
            source_name: Name for the data source
            headers: Optional HTTP headers
            params: Optional query parameters
            auth: Optional (username, password) tuple for basic auth
            pagination_key: Key for next page URL in response (if paginated)
            data_key: Key to extract data array from response
            rate_limit_delay: Seconds to wait between requests
        """
        super().__init__(source_name=source_name)
        
        self.base_url = base_url.rstrip('/')
        self.endpoint = endpoint.lstrip('/')
        self.headers = headers or {}
        self.params = params or {}
        self.auth = auth
        self.pagination_key = pagination_key
        self.data_key = data_key
        self.rate_limit_delay = rate_limit_delay
    
    @property
    def url(self) -> str:
        """Construct full URL."""
        return f"{self.base_url}/{self.endpoint}"
    
    def _make_request(self, url: str) -> dict:
        """
        Make HTTP request with error handling.
        
        Args:
            url: URL to request
            
        Returns:
            JSON response as dictionary
            
        Raises:
            ExtractionError: If request fails
        """
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=self.params,
                auth=self.auth,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise ExtractionError(f"API request failed: {e}")
    
    def _extract_data(self, response: dict) -> list[dict]:
        """Extract data array from response."""
        if self.data_key:
            return response.get(self.data_key, [])
        return response if isinstance(response, list) else [response]
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from API, handling pagination if configured.
        
        Returns:
            DataFrame containing the API data
            
        Raises:
            ExtractionError: If extraction fails
        """
        self.logger.info(f"Extracting data from {self.url}")
        
        all_data = []
        url = self.url
        page = 1
        
        while url:
            self.logger.debug(f"Fetching page {page}")
            response = self._make_request(url)
            data = self._extract_data(response)
            all_data.extend(data)
            
            # Check for next page
            if self.pagination_key and self.pagination_key in response:
                url = response[self.pagination_key]
                page += 1
                time.sleep(self.rate_limit_delay)
            else:
                url = None
        
        self.logger.info(f"Extracted {len(all_data)} records from API")
        
        if not all_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        return self.add_metadata(df)
