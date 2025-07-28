import gql
from dagster import ConfigurableResource, Definitions, EnvVar, get_dagster_logger
from dagster.components import definitions
from dagster_open_platform.utils.github_gql_queries import (
    GITHUB_DISCUSSIONS_QUERY,
    GITHUB_ISSUES_QUERY,
)
from gql.transport.requests import RequestsHTTPTransport


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

    def get_issues(self, start_date="2023-01-01", end_date="2023-12-31") -> list[dict]:
        issues_query_str = GITHUB_ISSUES_QUERY.replace("START_DATE", start_date).replace(
            "END_DATE", end_date
        )
        return self._fetch_results(issues_query_str, "issues")

    def get_discussions(self, start_date="2023-01-01", end_date="2023-12-31") -> list[dict]:
        discussion_query_str = GITHUB_DISCUSSIONS_QUERY.replace("START_DATE", start_date).replace(
            "END_DATE", end_date
        )
        return self._fetch_results(discussion_query_str, "discussions")

    def _fetch_results(self, query_str: str, object_type: str) -> list[dict]:
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


@definitions
def defs():
    return Definitions(
        resources={
            "github": GithubResource(
                github_token=EnvVar("GITHUB_TOKEN")
            ),  # Token defined in the Devtools Github account
        },
    )
