import base64
import json

from dagster import ConfigurableResource
from google.oauth2 import service_account
from googleapiclient.discovery import build


class GoogleSearchConsoleResource(ConfigurableResource):
    """Resource for connecting to Google Search Console API using service account credentials."""

    service_account_json_base64: str
    site_url: str

    def get_service(self):
        """Create and return a Google Search Console API service object."""
        credentials_dict = json.loads(
            base64.b64decode(self.service_account_json_base64).decode("utf-8")
        )
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )

        service = build("webmasters", "v3", credentials=credentials)
        return service

    def search_analytics_query(
        self,
        dimensions: list,
        start_date: str,
        end_date: str,
        search_type: str = "web",
        data_state: str = "final",
        row_limit: int = 25000,
    ):
        """Execute a search analytics query against Google Search Console API.

        Args:
            dimensions: List of dimensions to group by (e.g., ['query', 'date'])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            search_type: Type of search ('web', 'image', 'video', 'news')
            data_state: Data state ('final' or 'all')
            row_limit: Maximum number of rows to return

        Returns:
            List of rows from the API response
        """
        service = self.get_service()

        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "searchType": search_type,
            "dataState": data_state,
            "rowLimit": row_limit,
        }

        response = service.searchanalytics().query(siteUrl=self.site_url, body=request).execute()

        return response.get("rows", [])

    def search_analytics_query_with_pagination(
        self,
        dimensions: list,
        start_date: str,
        end_date: str,
        search_type: str = "web",
        data_state: str = "final",
        row_limit: int = 25000,
        start_row: int = 0,
    ):
        """Execute a search analytics query with pagination support.

        Args:
            dimensions: List of dimensions to group by (e.g., ['query', 'date'])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            search_type: Type of search ('web', 'image', 'video', 'news')
            data_state: Data state ('final' or 'all')
            row_limit: Maximum number of rows to return per request
            start_row: Starting row index for pagination

        Returns:
            List of rows from the API response
        """
        service = self.get_service()

        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "searchType": search_type,
            "dataState": data_state,
            "rowLimit": row_limit,
            "startRow": start_row,
        }

        response = service.searchanalytics().query(siteUrl=self.site_url, body=request).execute()

        return response.get("rows", [])
