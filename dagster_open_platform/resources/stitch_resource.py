import logging
import sys
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, cast
from urllib.parse import urljoin

import requests
from dagster import (
    AssetKey,
    AssetsDefinition,
    ConfigurableResource,
    Failure,
    MaterializeResult,
    RetryPolicy,
    _check as check,
    get_dagster_logger,
    multi_asset,
)
from dagster._core.definitions.asset_spec import AssetSpec
from dagster._utils.cached_method import cached_method
from pydantic import Field

SYNC_ABORT_TIME = 60 * 10


class StitchResource(ConfigurableResource):
    stitch_client_id: str = Field(description="Client ID of the Stitch account")
    access_token: str = Field(description="Token used to authenticate with Stitch")

    request_max_retries: int = Field(
        default=3,
        description=(
            "The maximum number of times requests to the Stitch API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        default=0.25,
        description="Time (in seconds) to wait between each request retry.",
    )

    @property
    def api_base_url(self) -> str:
        return "https://api.stitchdata.com/"

    @property
    @cached_method
    def _log(self) -> logging.Logger:
        return get_dagster_logger()

    def _get_request_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self, method: str, endpoint: str, data: Optional[str] = None
    ) -> requests.Response:
        url = urljoin(self.api_base_url, endpoint)

        num_retries = 0
        while True:
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._get_request_headers(),
                    data=data,
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                self._log.error("Request to Stitch API failed: %s", e)
                if num_retries == self.request_max_retries:
                    break
                num_retries += 1
                time.sleep(self.request_retry_delay)

        raise Failure(f"Max retries ({self.request_max_retries}) exceeded with url: {url}.")

    def fetch_sync_logs(self, job_name: str) -> str:
        url = urljoin(self.api_base_url, f"/v4/{self.stitch_client_id}/extractions/{job_name}")

        response = self._make_request("GET", url)

        return response.text

    def make_request_json(
        self, method: str, endpoint: str, data: Optional[str] = None
    ) -> Mapping[str, Any]:
        """Creates and sends a request to the desired Stitch Connector API endpoint.

        Args:
            method (str): The http method to use for this request (e.g. "POST", "GET", "PATCH").
            endpoint (str): The Stitch API endpoint to send this request to.
            data (Optional[str]): JSON-formatted data string to be included in the request.

        Returns:
            Dict[str, Any]: Parsed json data from the response to this request
        """
        result = self._make_request(method, endpoint, data)
        return result.json()

    def start_sync(self, source_id: str) -> str:
        job_name = cast(
            str, self.make_request_json("POST", f"/v4/sources/{source_id}/sync")["job_name"]
        )
        self._log.info(f"Sync initialized for source_id={source_id} with job name {job_name}")
        return job_name

    def poll_sync(
        self,
        source_id: str,
        job_name: str,
        poll_interval: float = 30,
    ) -> Any:
        time_without_job = 0
        while True:
            extractions = cast(
                Dict[str, Any],
                self.make_request_json("GET", f"/v4/{self.stitch_client_id}/extractions"),
            )
            job = next((j for j in extractions["data"] if j["job_name"] == job_name), None)

            phase = "no update"
            if job:
                discovery_status = job["discovery_exit_status"]
                if discovery_status != 0 and discovery_status is not None:
                    raise Failure(
                        f"Stitch discovery phase failed: {job['discovery_error_message']}"
                    )

                tap_status = job["tap_exit_status"]
                if tap_status != 0 and tap_status is not None:
                    raise Failure(f"Stitch tap phase failed: {job['tap_error_message']}")

                target_status = job["target_exit_status"]
                if target_status != 0 and target_status is not None:
                    raise Failure(f"Stitch target phase failed: {job['target_error_message']}")

                phase = (
                    "discovery phase"
                    if discovery_status is None
                    else (
                        "tap phase"
                        if tap_status is None
                        else "target phase"
                        if target_status is None
                        else "complete"
                    )
                )
            else:
                if time_without_job > SYNC_ABORT_TIME:
                    raise Failure(
                        f"Stitch sync job {job_name} not found after {SYNC_ABORT_TIME} seconds"
                    )
                time_without_job += poll_interval

            self._log.info(f"Polling for job {job_name}, status: {phase}")

            if phase == "complete":
                break

            # Sleep for the configured time interval before polling again.
            time.sleep(poll_interval)


def build_stitch_assets(
    source_id: str,
    destination_tables: List[str],
    asset_key_prefix: Optional[Sequence[str]] = None,
    op_tags: Optional[Mapping[str, Any]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    table_to_metadata: Optional[Callable[[str], Mapping[str, Any]]] = None,
    group_name: Optional[str] = None,
) -> List[AssetsDefinition]:
    asset_key_prefix = check.opt_sequence_param(asset_key_prefix, "asset_key_prefix", of_type=str)

    tracked_asset_keys = {
        table: AssetKey([*asset_key_prefix, *table.split(".")]) for table in destination_tables
    }

    @multi_asset(
        name=f"stitch_sync_{source_id}",
        specs=[
            AssetSpec(
                key=asset_key,
                metadata=table_to_metadata(table) if table_to_metadata else None,
                group_name=group_name if group_name else None,
            )
            for table, asset_key in tracked_asset_keys.items()
        ],
        compute_kind="stitch",
        op_tags=op_tags,
        retry_policy=retry_policy,
    )
    def _assets(stitch: StitchResource) -> Any:
        job_name = None
        try:
            job_name = stitch.start_sync(source_id)
            stitch.poll_sync(source_id, job_name)
        finally:
            try:
                if job_name:
                    logs = stitch.fetch_sync_logs(job_name=job_name)
                    for line in logs.split("\n"):
                        sys.stdout.write(line + "\n")
                        sys.stdout.flush()
            except (requests.RequestException, Failure):
                pass

        streams = cast(
            List[Dict[str, Any]],
            stitch.make_request_json("GET", f"/v4/sources/{source_id}/streams"),
        )

        for table, asset_key in tracked_asset_keys.items():
            stream_data = next((s for s in streams if s["stream_name"] == table), None)
            yield MaterializeResult(
                asset_key=asset_key,
                metadata=(
                    {
                        "row_count": stream_data["metadata"]["row-count"],
                        **(table_to_metadata(table) if table_to_metadata else {}),
                    }
                    if stream_data
                    else None
                ),
            )

    return [_assets]
