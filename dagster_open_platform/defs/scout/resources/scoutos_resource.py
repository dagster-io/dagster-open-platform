import datetime
import json
from collections.abc import Generator
from typing import Any

import requests
from dagster import ConfigurableResource, Definitions, EnvVar, get_dagster_logger
from dagster.components import definitions
from scoutos import Scout, Workflow


class ScoutosResource(ConfigurableResource):
    """Resource for interacting with the ScoutOS API using the official SDK."""

    api_key: str
    workflow_id: str = ""  # Optional - empty string means get all workflows

    @property
    def headers(self):
        return {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    def _get_client(self) -> Scout:
        """Get the ScoutOS SDK client."""
        return Scout(api_key=self.api_key)

    def write_documents(
        self, collection_id: str, table_id: str, documents: list[dict]
    ) -> dict[str, Any]:
        """Writes documents to the ScoutOS API."""
        request_url = f"https://api.scoutos.com/v2/collections/{collection_id}/tables/{table_id}/documents?await_completion=false"
        payload = json.dumps(documents)
        response = requests.request("POST", request_url, data=payload, headers=self.headers)
        return response.json()

    def get_workflows(self) -> list[Workflow]:
        """Returns all workflows using the ScoutOS SDK."""
        client = self._get_client()
        try:
            response = client.workflows.list()
            # Handle response format - could be direct list or wrapped in data
            if hasattr(response, "data"):
                return response.data
            elif isinstance(response, list):
                return response
            elif isinstance(response, dict) and "data" in response:
                return response["data"]
            return []
        except Exception as e:
            get_dagster_logger().error(f"Error fetching workflows: {e}")
            return []

    def get_runs_stream(
        self, startdate: datetime.datetime, enddate: datetime.datetime
    ) -> Generator[dict[str, Any], None, None]:
        """Returns workflow runs as a generator for streaming processing."""
        if not self.workflow_id:
            # Get runs for all workflows
            workflows = self.get_workflows()
            for workflow in workflows:
                if workflow_id := workflow.workflow_id:
                    yield from self._get_runs_for_workflow(workflow_id, startdate, enddate)
        else:
            # Get runs for specific workflow
            yield from self._get_runs_for_workflow(self.workflow_id, startdate, enddate)

    def _get_runs_for_workflow(
        self,
        workflow_id: str,
        startdate: datetime.datetime,
        enddate: datetime.datetime,
        cursor: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Get runs for a specific workflow using the ScoutOS SDK."""
        client = self._get_client()
        current_cursor = cursor

        while True:
            try:
                response = client.workflow_logs.list_logs(
                    workflow_id=workflow_id,
                    start_date=startdate.strftime("%Y-%m-%d"),
                    end_date=enddate.strftime("%Y-%m-%d"),
                    status="completed",
                    cursor=current_cursor,
                )

                has_more = False
                for chunk in response:
                    # parse json string to dict
                    data = json.loads(chunk.json())
                    if data.get("has_more"):
                        has_more = True
                        current_cursor = data.get("next_cursor")
                    yield data

                # If no more pages, break the loop
                if not has_more:
                    break

            except Exception as e:
                get_dagster_logger().error(f"Error fetching runs for workflow {workflow_id}: {e}")
                break


@definitions
def defs():
    return Definitions(
        resources={
            "scoutos": ScoutosResource(api_key=EnvVar("SCOUTOS_API_KEY")),
        },
    )
