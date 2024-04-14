import json
from typing import Any, Dict, List

import gql
import requests
from dagster import (
    ConfigurableResource,
    get_dagster_logger,
)
from gql.transport.requests import RequestsHTTPTransport

from ..utils.github_gql_queries import (
    GITHUB_DISCUSSIONS_QUERY,
    GITHUB_ISSUES_QUERY,
)


class ScoutosResource(ConfigurableResource):
    """Resource for interacting with the ScoutOS API which hosts our Support Bot."""

    api_key: str

    def write_documents(self, collection_id: str, documents: list[dict]) -> Dict[str, Any]:
        """Writes documents to the ScoutOS API."""
        request_url = f"https://api.scoutos.com/v1/collections/{collection_id}/files"
        headers_list = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        payload = json.dumps({"files": documents})
        response = requests.request("POST", request_url, data=payload, headers=headers_list)
        return response.json()


class GithubResource(ConfigurableResource):
    """Resource for fetching Github issues and discussions."""

    github_token: str

    def client(self):
        return gql.Client(
            schema=None,
            transport=RequestsHTTPTransport(
                url="https://api.github.com/graphql",
                headers={
                    "Authorization": f"Bearer {self.github_token}",
                },
                retries=3,
            ),
            fetch_schema_from_transport=True,
        )

    def get_issues(self, start_date="2023-01-01", end_date="2023-12-31") -> List[dict]:
        issues_query_str = GITHUB_ISSUES_QUERY.replace("START_DATE", start_date).replace(
            "END_DATE", end_date
        )
        return self._fetch_results(issues_query_str, "issues")

    def get_discussions(self, start_date="2023-01-01", end_date="2023-12-31") -> List[dict]:
        discussion_query_str = GITHUB_DISCUSSIONS_QUERY.replace("START_DATE", start_date).replace(
            "END_DATE", end_date
        )
        return self._fetch_results(discussion_query_str, "discussions")

    def _fetch_results(self, query_str: str, object_type: str) -> List[dict]:
        log = get_dagster_logger()
        client = self.client()
        cursor = None
        results = []
        while True:
            log.info(f"Fetching results from Github: {object_type} with cursor: {cursor}")
            query = gql.gql(
                query_str.replace("CURSOR_PLACEHOLDER", f'"{cursor}"' if cursor else "null"),
            )
            result = client.execute(query)
            search = result["search"]
            edges = search["edges"]
            for node in edges:
                results.append(node["node"])
            log.info(f"Total results: {len(results)}")
            if not search["pageInfo"]["hasNextPage"]:
                break
            cursor = search["pageInfo"]["endCursor"]
        return results
