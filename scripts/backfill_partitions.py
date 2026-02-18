#!/usr/bin/env python3
"""Script to launch chunked partition backfills for Dagster assets.

This script hits the Dagster GraphQL API to launch multiple runs for an asset,
with each run materializing a specified number of partitions (chunk size).

Example usage:
    # Basic usage with local Dagster instance
    python backfill_partitions.py \
        --asset-key "purina.oss_analytics.dagster_pypi_downloads" \
        --start-partition "2024-01-01-00:00" \
        --end-partition "2024-01-31-00:00" \
        --chunk-size 7

    # With Dagster Cloud authentication
    python backfill_partitions.py \
        --asset-key "purina.oss_analytics.dagster_pypi_downloads" \
        --start-partition "2024-01-01-00:00" \
        --end-partition "2024-01-31-00:00" \
        --chunk-size 7 \
        --host "https://myorg.dagster.cloud/prod" \
        --port 443 \
        --api-token "your-api-token-here"

    # Using environment variables
    export DAGSTER_CLOUD_API_TOKEN="your-api-token-here"
    export DAGSTER_HOST="https://myorg.dagster.cloud/prod"
    export DAGSTER_PORT="443"
    python backfill_partitions.py \
        --asset-key "purina.oss_analytics.dagster_pypi_downloads" \
        --start-partition "2024-01-01-00:00" \
        --end-partition "2024-01-31-00:00" \
        --chunk-size 7

    # For multi-part asset keys, use dot notation or JSON:
    python backfill_partitions.py \
        --asset-key '["database", "schema", "table"]' \
        --start-partition "2024-01-01-00:00" \
        --end-partition "2024-12-31-00:00" \
        --chunk-size 10
"""

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import urljoin

import requests


class DagsterAPIClient:
    """Client for interacting with Dagster's GraphQL API."""

    def __init__(self, host: str, port: int = 3000, api_token: str | None = None):
        """Initialize the Dagster API client.

        Args:
            host: Dagster host URL (e.g., "http://localhost")
            port: Dagster port (default: 3000)
            api_token: Optional Dagster Cloud API token for authentication
        """
        self.base_url = f"{host}:{port}"
        self.graphql_url = urljoin(self.base_url, "/graphql")
        self.api_token = api_token

    def execute_query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query against the Dagster API.

        Args:
            query: GraphQL query string
            variables: Optional variables for the query

        Returns:
            Response data from the API

        Raises:
            Exception: If the API returns an error
        """
        headers = {"Content-Type": "application/json"}

        # Add authentication header if API token is provided
        if self.api_token:
            headers["Dagster-Cloud-Api-Token"] = self.api_token

        response = requests.post(
            self.graphql_url,
            json={"query": query, "variables": variables or {}},
            headers=headers,
        )
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        return data

    def get_partition_keys(
        self, asset_key: list[str], start_partition: str, end_partition: str
    ) -> list[str]:
        """Get partition keys for an asset within a range.

        Args:
            asset_key: Asset key as a list of strings
            start_partition: Start partition key
            end_partition: End partition key

        Returns:
            List of partition keys in the range
        """
        query = """
        query GetPartitionKeys($assetKey: AssetKeyInput!) {
            assetNodeOrError(assetKey: $assetKey) {
                __typename
                ... on AssetNode {
                    partitionKeysByDimension {
                        name
                        partitionKeys
                    }
                }
            }
        }
        """
        variables = {"assetKey": {"path": asset_key}}

        result = self.execute_query(query, variables)
        asset_node = result["data"]["assetNodeOrError"]

        if asset_node["__typename"] != "AssetNode":
            raise Exception(f"Asset not found or error: {asset_node}")

        # Get all partition keys (assuming single dimension for now)
        partition_data = asset_node.get("partitionKeysByDimension", [])
        if not partition_data:
            raise Exception(f"Asset {asset_key} is not partitioned")

        all_partitions = partition_data[0]["partitionKeys"]

        # Filter to the requested range
        try:
            start_idx = all_partitions.index(start_partition)
            end_idx = all_partitions.index(end_partition)
        except ValueError as e:
            raise Exception(f"Partition not found: {e}")

        if start_idx > end_idx:
            raise Exception("Start partition must come before end partition")

        return all_partitions[start_idx : end_idx + 1]

    def launch_partition_backfill(
        self,
        asset_key: list[str],
        partition_keys: list[str],
        title: str | None = None,
    ) -> str:
        """Launch a backfill run for specific partitions of an asset.

        Args:
            asset_key: Asset key as a list of strings
            partition_keys: List of partition keys to materialize
            title: Optional title for the backfill

        Returns:
            Backfill ID
        """
        mutation = """
        mutation LaunchPartitionBackfill(
            $backfillParams: LaunchBackfillParams!
        ) {
            launchPartitionBackfill(backfillParams: $backfillParams) {
                __typename
                ... on LaunchBackfillSuccess {
                    backfillId
                }
                ... on PartitionSetNotFoundError {
                    message
                }
                ... on PythonError {
                    message
                }
            }
        }
        """

        variables = {
            "backfillParams": {
                "partitionNames": partition_keys,
                "assetSelection": [{"path": asset_key}],
                "title": title or f"Backfill {partition_keys[0]} to {partition_keys[-1]}",
                "fromFailure": False,
                "allPartitions": False,
            }
        }

        result = self.execute_query(mutation, variables)
        response = result["data"]["launchPartitionBackfill"]

        if response["__typename"] != "LaunchBackfillSuccess":
            raise Exception(f"Failed to launch backfill: {response}")

        return response["backfillId"]


def parse_asset_key(asset_key_str: str) -> list[str]:
    """Parse asset key from string format.

    Supports:
    - Dot notation: "database.schema.table"
    - JSON array: '["database", "schema", "table"]'

    Args:
        asset_key_str: Asset key as string

    Returns:
        Asset key as list of strings
    """
    # Try parsing as JSON first
    if asset_key_str.startswith("["):
        try:
            key = json.loads(asset_key_str)
            if not isinstance(key, list):
                raise ValueError("Asset key JSON must be an array")
            return key
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON asset key: {asset_key_str}")

    # Otherwise split by dots
    return asset_key_str.split(".")


def chunk_list(items: list[str], chunk_size: int) -> list[list[str]]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def main():
    parser = argparse.ArgumentParser(
        description="Launch chunked partition backfills for Dagster assets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--asset-key",
        required=True,
        help='Asset key in dot notation (e.g., "db.schema.table") or JSON (e.g., \'["db", "schema", "table"]\')',
    )
    parser.add_argument(
        "--start-partition",
        required=True,
        help="Start partition key (e.g., '2024-01-01-00:00')",
    )
    parser.add_argument(
        "--end-partition",
        required=True,
        help="End partition key (e.g., '2024-01-31-00:00')",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        required=True,
        help="Number of partitions to include in each run",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("DAGSTER_HOST", "http://localhost"),
        help="Dagster host URL (default: http://localhost or $DAGSTER_HOST)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DAGSTER_PORT", "3000")),
        help="Dagster port (default: 3000 or $DAGSTER_PORT)",
    )
    parser.add_argument(
        "--api-token",
        default=os.environ.get("DAGSTER_CLOUD_API_TOKEN"),
        help="Dagster Cloud API token for authentication (default: $DAGSTER_CLOUD_API_TOKEN)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print chunks without launching backfills",
    )

    args = parser.parse_args()

    # Parse asset key
    try:
        asset_key = parse_asset_key(args.asset_key)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Asset key: {asset_key}")
    print(f"Partition range: {args.start_partition} to {args.end_partition}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Dagster URL: {args.host}:{args.port}")
    if args.api_token:
        print("Using API token for authentication")
    print()

    # Initialize client
    client = DagsterAPIClient(args.host, args.port, args.api_token)

    # Get partition keys
    print("Fetching partition keys...")
    try:
        partition_keys = client.get_partition_keys(
            asset_key, args.start_partition, args.end_partition
        )
    except Exception as e:
        print(f"Error fetching partitions: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(partition_keys)} partitions")
    print()

    # Chunk the partitions
    chunks = chunk_list(partition_keys, args.chunk_size)
    print(f"Will launch {len(chunks)} backfill(s):")
    for i, chunk in enumerate(chunks, 1):
        print(f"  Chunk {i}: {chunk[0]} to {chunk[-1]} ({len(chunk)} partitions)")
    print()

    if args.dry_run:
        print("Dry run mode - not launching backfills")
        return

    # Confirm before launching
    response = input("Launch these backfills? (y/N): ")
    if response.lower() != "y":
        print("Cancelled")
        return

    # Launch backfills
    print()
    print("Launching backfills...")
    backfill_ids = []

    for i, chunk in enumerate(chunks, 1):
        try:
            title = f"Chunked backfill {i}/{len(chunks)}: {chunk[0]} to {chunk[-1]}"
            backfill_id = client.launch_partition_backfill(asset_key, chunk, title)
            backfill_ids.append(backfill_id)
            print(f"  ✓ Launched chunk {i}: {backfill_id}")
        except Exception as e:
            print(f"  ✗ Failed to launch chunk {i}: {e}", file=sys.stderr)

    print()
    print(f"Successfully launched {len(backfill_ids)}/{len(chunks)} backfills")
    if backfill_ids:
        print("\nBackfill IDs:")
        for backfill_id in backfill_ids:
            print(f"  {backfill_id}")


if __name__ == "__main__":
    main()
