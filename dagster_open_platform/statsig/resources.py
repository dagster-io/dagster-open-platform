from typing import TYPE_CHECKING

import dagster as dg
import requests

if TYPE_CHECKING:
    from statsig.statsig import StatsigEvent


class StatsigResource(dg.ConfigurableResource):
    api_key: str

    def write_events(self, events: list["StatsigEvent"]):
        response = requests.post(
            "https://events.statsigapi.net/v1/log_event",
            headers={"statsig-api-key": self.api_key},
            json=dict(events=[event.to_dict() for event in events]),
        )
        response.raise_for_status()
        return response


class DatadogMetricsResource(dg.ConfigurableResource):
    api_key: str
    app_key: str

    def query_metrics_for_last_value(self, metric_query: str, start: int, end: int):
        # note: this isn't a paginated API so I'm not sure if there's a limit to the number of series
        # currently it looks like it'll go over 1000 at least
        response = requests.get(
            "https://api.datadog.com/api/v1/query",
            params={
                "api_key": self.api_key,
                "application_key": self.app_key,
                "from": start,
                "to": end,
                "query": metric_query,
            },
        )
        response.raise_for_status()
        # scope is organization:public_id
        return {
            series["scope"].split(":")[-1]: series["pointlist"][-1][1]
            for series in response.json()["series"]
        }
