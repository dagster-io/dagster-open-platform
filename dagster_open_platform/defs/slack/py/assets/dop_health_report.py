"""Dagster Asset Health Reporter.

This module defines a Dagster asset that generates and sends comprehensive
asset health reports to the #bot-purina Slack channel when materialized.
"""

from collections import defaultdict
from typing import Any

import dagster as dg
import requests
from dagster import ConfigurableResource, MaterializeResult, asset
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError

# GraphQL Query for fetching asset health data
ASSET_HEALTH_QUERY = """
query AssetHealthQuery {
  assetsOrError {
    ... on AssetConnection {
      nodes {
        id
        definition {
          repository {
            location {
              name
            }
          }
        }
        key {
          path
        }
        assetMaterializations(limit: 1) {
          timestamp
        }
        assetHealth {
          assetHealth
          materializationStatus
          materializationStatusMetadata {
            ... on AssetHealthMaterializationDegradedPartitionedMeta {
              numMissingPartitions
              numFailedPartitions
              totalNumPartitions
            }
            ... on AssetHealthMaterializationHealthyPartitionedMeta {
              numMissingPartitions
              totalNumPartitions
            }
            ... on AssetHealthMaterializationDegradedNotPartitionedMeta {
              failedRunId
            }
          }
          assetChecksStatus
          assetChecksStatusMetadata {
            ... on AssetHealthCheckDegradedMeta {
              numFailedChecks
              numWarningChecks
              totalNumChecks
            }
            ... on AssetHealthCheckWarningMeta {
              numWarningChecks
              totalNumChecks
            }
            ... on AssetHealthCheckUnknownMeta {
              numNotExecutedChecks
              totalNumChecks
            }
          }
          freshnessStatus
          freshnessStatusMetadata {
            lastMaterializedTimestamp
          }
        }
      }
    }
    ... on PythonError {
      message
      stack
      errorChain {
        isExplicitLink
        error {
          message
          stack
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}
"""


class DagsterHealthReportConfig(ConfigurableResource):
    """Configuration resource for Dagster health reporting."""

    deployment_url: str | None = dg.EnvVar("DOP_HEALTH_REPORT_DEPLOYMENT_URL").get_value()
    dagster_token: str | None = dg.EnvVar("DOP_HEALTH_REPORT_DAGSTER_TOKEN").get_value()
    slack_webhook_url: str | None = dg.EnvVar("DOP_HEALTH_REPORT_SLACK_WEBHOOK_URL").get_value()
    code_location: str | None = dg.EnvVar("DOP_HEALTH_REPORT_CODE_LOCATION").get_value()


def create_dagster_client(deployment_url: str, token: str) -> DagsterGraphQLClient:
    """Create and return a Dagster GraphQL client."""
    if token:
        # Dagster+ deployment
        headers = {"Dagster-Cloud-Api-Token": token}
        return DagsterGraphQLClient(deployment_url, headers=headers, use_https=True)
    else:
        # External deployment without token
        return DagsterGraphQLClient(deployment_url, use_https=True)


def fetch_asset_health_data(client: DagsterGraphQLClient) -> dict[str, Any]:
    """Execute the asset health query and return the result."""
    try:
        result = client._execute(ASSET_HEALTH_QUERY)  # noqa: SLF001
        return result
    except DagsterGraphQLClientError as e:
        raise Exception(f"Failed to fetch asset health data: {e}")


def analyze_asset_health(data: dict[str, Any], code_location: str | None = None) -> dict[str, Any]:
    """Analyze the asset health data and return summary statistics."""
    if "assetsOrError" not in data:
        raise Exception("Invalid response format")

    assets_or_error = data["assetsOrError"]

    # Handle GraphQL errors
    if assets_or_error.get("__typename") == "PythonError":
        error_msg = assets_or_error.get("message", "Unknown error")
        raise Exception(f"GraphQL query failed: {error_msg}")

    if "nodes" not in assets_or_error:
        raise Exception("No asset nodes found in response")

    assets = assets_or_error["nodes"]

    # Filter by code location if specified
    if code_location:
        filtered_assets = []
        for asset in assets:
            definition = asset.get("definition")
            if definition and definition.get("repository"):
                location = definition["repository"].get("location")
                if location and location.get("name") == code_location:
                    filtered_assets.append(asset)
        assets = filtered_assets

    # Initialize counters
    stats = {
        "total_assets": len(assets),
        "health_status": defaultdict(int),
        "materialization_status": defaultdict(int),
        "checks_status": defaultdict(int),
        "freshness_status": defaultdict(int),
        "partitioned_assets": 0,
        "assets_with_checks": 0,
        "failed_runs": [],
        "degraded_assets": [],
        "detailed_stats": {
            "partitioned": {
                "missing_partitions": 0,
                "failed_partitions": 0,
                "total_partitions": 0,
            },
            "checks": {
                "failed_checks": 0,
                "warning_checks": 0,
                "not_executed_checks": 0,
                "total_checks": 0,
            },
        },
    }

    # Process each asset
    for asset in assets:
        asset_key = "/".join(asset["key"]["path"])
        asset_health = asset.get("assetHealth")

        if not asset_health:
            continue

        # Overall health status
        overall_health = asset_health.get("assetHealth")
        if overall_health:
            stats["health_status"][overall_health] += 1
            # Collect degraded assets
            if overall_health == "DEGRADED":
                stats["degraded_assets"].append(asset_key)

        # Materialization status
        mat_status = asset_health.get("materializationStatus")
        if mat_status:
            stats["materialization_status"][mat_status] += 1

        # Process materialization metadata
        mat_metadata = asset_health.get("materializationStatusMetadata")
        if mat_metadata:
            if "numMissingPartitions" in mat_metadata:
                stats["partitioned_assets"] += 1
                stats["detailed_stats"]["partitioned"]["missing_partitions"] += mat_metadata.get(
                    "numMissingPartitions", 0
                )
                stats["detailed_stats"]["partitioned"]["total_partitions"] += mat_metadata.get(
                    "totalNumPartitions", 0
                )
                if "numFailedPartitions" in mat_metadata:
                    stats["detailed_stats"]["partitioned"]["failed_partitions"] += mat_metadata.get(
                        "numFailedPartitions", 0
                    )
            elif "failedRunId" in mat_metadata:
                failed_run_id = mat_metadata.get("failedRunId")
                if failed_run_id:
                    stats["failed_runs"].append((asset_key, failed_run_id))

        # Asset checks status
        checks_status = asset_health.get("assetChecksStatus")
        if checks_status:
            stats["checks_status"][checks_status] += 1
            stats["assets_with_checks"] += 1

        # Process checks metadata
        checks_metadata = asset_health.get("assetChecksStatusMetadata")
        if checks_metadata:
            key_mapping = {
                "numFailedChecks": "failed_checks",
                "numWarningChecks": "warning_checks",
                "numNotExecutedChecks": "not_executed_checks",
                "totalNumChecks": "total_checks",
            }
            for key, stats_key in key_mapping.items():
                if key in checks_metadata:
                    stats["detailed_stats"]["checks"][stats_key] += checks_metadata[key]

        # Freshness status
        freshness_status = asset_health.get("freshnessStatus")
        if freshness_status:
            stats["freshness_status"][freshness_status] += 1

    return stats


def format_slack_report(
    stats: dict[str, Any], code_location: str | None = None, deployment_url: str | None = None
) -> dict[str, Any]:
    """Format the asset health statistics into a Slack message."""
    # Build blocks for the message
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"<https://{deployment_url}|Go to Dagster>"},
        }
    ]

    # Health status breakdown
    if stats["health_status"]:
        health_text = ""
        for status, count in stats["health_status"].items():
            percentage = (count / stats["total_assets"]) * 100 if stats["total_assets"] > 0 else 0
            emoji = (
                ":large_green_circle:"
                if status == "HEALTHY"
                else ":white_circle:"
                if status == "UNKNOWN"
                else ":red_circle:"
            )
            health_text += f"{emoji} *{status}:* {count:,} ({percentage:.1f}%)\n"

        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Health Status Breakdown:*\n{health_text}"},
            }
        )

    # Degraded assets (show top 5)
    if stats["degraded_assets"]:
        degraded_text = ""
        for asset_key in stats["degraded_assets"][:5]:
            degraded_text += f"• `{asset_key}`\n"
        if len(stats["degraded_assets"]) > 5:
            degraded_text += f"and {len(stats['degraded_assets']) - 5} more"

        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*🚨 Degraded Assets:*\n{degraded_text}"},
            }
        )

    return {"blocks": blocks}


def send_to_slack(webhook_url: str, message: dict[str, Any]) -> bool:
    """Send formatted message to Slack webhook."""
    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to send to Slack: {e}")


@asset(
    name="asset_health_report_slack",
    group_name="monitoring",
    description="Sends Dagster asset health report to #bot-purina Slack channel",
    required_resource_keys={"dagster_health_report_config"},
    tags={"dagster/kind/slack": ""},
    automation_condition=dg.AutomationCondition.cron_tick_passed("0 13 * * *"),
)
def asset_health_report_slack(context: dg.AssetExecutionContext) -> MaterializeResult:
    """Dagster asset that generates and sends asset health reports to Slack.

    When materialized, this asset:
    1. Connects to the configured Dagster deployment
    2. Fetches asset health data via GraphQL
    3. Analyzes the health statistics
    4. Formats a report for Slack
    5. Sends the report to the configured Slack channel

    Returns MaterializeResult with metadata about the report sent.
    """
    # Get configuration from resources
    config = context.resources.dagster_health_report_config

    try:
        context.log.info(f"Starting asset health report for deployment: {config.deployment_url}")

        if config.code_location:
            context.log.info(f"Filtering by code location: {config.code_location}")

        # Create Dagster GraphQL client
        context.log.info("Creating Dagster GraphQL client...")
        client = create_dagster_client(config.deployment_url, config.dagster_token)

        # Fetch asset health data
        context.log.info("Fetching asset health data...")
        data = fetch_asset_health_data(client)

        # Analyze health statistics
        context.log.info("Analyzing asset health data...")
        stats = analyze_asset_health(data, config.code_location)

        # Format Slack message
        context.log.info("Formatting Slack report...")
        slack_message = format_slack_report(stats, config.code_location, config.deployment_url)

        # Send to Slack
        context.log.info("Sending report to Slack...")
        send_to_slack(config.slack_webhook_url, slack_message)

        context.log.info("✅ Asset health report sent to Slack successfully!")

        # Return MaterializeResult with metadata
        return MaterializeResult(
            metadata={
                "deployment_url": config.deployment_url,
                "code_location": config.code_location or "all",
                "total_assets": stats["total_assets"],
                "healthy_assets": stats["health_status"].get("HEALTHY", 0),
                "degraded_assets": stats["health_status"].get("DEGRADED", 0),
                "unknown_assets": stats["health_status"].get("UNKNOWN", 0),
                "degraded_assets_count": len(stats["degraded_assets"]),
                "report_sent_to_slack": True,
            }
        )

    except Exception as e:
        context.log.error(f"Failed to generate and send asset health report: {e}")
        # Re-raise the exception to properly fail the asset materialization
        raise e


# Resource definitions for different environments
@dg.resource
def dagster_health_report_config() -> DagsterHealthReportConfig:
    """Production configuration for Dagster health reporting."""
    return DagsterHealthReportConfig(
        deployment_url=dg.EnvVar("DOP_HEALTH_REPORT_DEPLOYMENT_URL").get_value(),
        code_location=dg.EnvVar("DOP_HEALTH_REPORT_CODE_LOCATION").get_value(),
        dagster_token=dg.EnvVar("DOP_HEALTH_REPORT_DAGSTER_TOKEN").get_value(),
        slack_webhook_url=dg.EnvVar("DOP_HEALTH_REPORT_SLACK_WEBHOOK_URL").get_value(),
    )


# Define the Definitions object for this Dagster code location
defs = dg.Definitions(
    assets=[asset_health_report_slack],
    resources={"dagster_health_report_config": dagster_health_report_config},
)
