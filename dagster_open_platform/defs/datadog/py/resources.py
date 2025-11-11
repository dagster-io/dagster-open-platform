"""Resources for Datadog components."""

import os

import dagster as dg
import requests
from dagster import ConfigurableResource
from dagster.components import definitions


class DatadogBillingResource(ConfigurableResource):
    """Resource for authenticating with Datadog billing APIs."""

    api_key: str
    app_key: str
    site: str = "api.datadoghq.com"

    @property
    def base_url(self) -> str:
        """Get the base URL for the Datadog API."""
        return f"https://{self.site}"

    def _get_headers(self) -> dict[str, str]:
        """Get headers for Datadog API requests."""
        return {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
            "Accept": "application/json",
        }

    def get_historical_cost(self, start_month: str, end_month: str) -> dict:
        """Get historical cost data from Datadog API.

        Historical cost data is available from the 16th of the following month.
        Use this for finalized billing data.

        Args:
            start_month: Start month in YYYY-MM-DD format
            end_month: End month in YYYY-MM-DD format

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/api/v2/usage/historical_cost"
        params = {
            "start_month": start_month,
            "end_month": end_month,
        }

        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json()

    def get_estimated_cost(self, start_month: str, end_month: str) -> dict:
        """Get estimated cost data from Datadog API.

        Estimated cost is for current/upcoming month.

        Args:
            start_month: Start month in YYYY-MM-DD format
            end_month: End month in YYYY-MM-DD format

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/api/v2/usage/estimated_cost"
        params = {
            "start_month": start_month,
            "end_month": end_month,
        }

        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json()


@definitions
def defs() -> dg.Definitions:
    """Create Definitions with Datadog resources."""
    return dg.Definitions(
        resources={
            "datadog_billing": DatadogBillingResource(
                api_key=dg.EnvVar("DATADOG_BILLING_API_KEY"),
                app_key=dg.EnvVar("DATADOG_BILLING_APP_KEY"),
                # site defaults to "api.datadoghq.com" if DATADOG_SITE env var is not set
                site=os.getenv("DATADOG_BILLING_SITE", "api.datadoghq.com"),
            ),
        }
    )
