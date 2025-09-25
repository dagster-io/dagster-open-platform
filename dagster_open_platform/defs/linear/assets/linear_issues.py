"""Linear assets for processing issues from fivetran data."""

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.linear.partitions import linear_issues_partition_def
from dagster_snowflake import SnowflakeResource


@dg.asset(
    partitions_def=linear_issues_partition_def,
    group_name="linear",
    kinds={"linear"},
    description="Process Linear issues from fivetran data, partitioned by issue ID",
    deps=[["fivetran", "linear", "issue"]],
)
def compass_linear_issues(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Process a single Linear issue by querying fivetran data and logging details."""
    issue_id = context.partition_key
    context.log.info(f"Processing Linear issue: {issue_id}")

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Query for the specific issue details
        query = """
        SELECT
            id,
            title,
            description,
            project_id,
            state_id,
            assignee_id,
            creator_id,
            created_at,
            updated_at,
            completed_at,
            priority,
            estimate
        FROM fivetran.linear.issue
        WHERE id = %s
        AND NOT _fivetran_deleted
        """

        cursor.execute(query, (issue_id,))
        issue_data = cursor.fetchone()

        if not issue_data:
            context.log.warning(f"Issue {issue_id} not found in fivetran data")
            return dg.MaterializeResult(
                metadata={
                    "issue_id": dg.MetadataValue.text(issue_id),
                    "status": dg.MetadataValue.text("not_found"),
                }
            )

        # Unpack the issue data
        (
            id_,
            title,
            description,
            project_id,
            state_id,
            assignee_id,
            creator_id,
            created_at,
            updated_at,
            completed_at,
            priority,
            estimate,
        ) = issue_data

        # Log the issue details
        context.log.info(f"Issue Title: {title}")
        context.log.info(f"Issue Description: {description or 'No description'}")
        context.log.info(f"Project ID: {project_id}")
        context.log.info(f"State ID: {state_id}")
        context.log.info(f"Assignee ID: {assignee_id or 'Unassigned'}")
        context.log.info(f"Priority: {priority}")
        context.log.info(f"Estimate: {estimate}")
        context.log.info(f"Created: {created_at}")
        context.log.info(f"Updated: {updated_at}")
        context.log.info(f"Completed: {completed_at or 'Not completed'}")

        return dg.MaterializeResult(
            metadata={
                "issue_id": dg.MetadataValue.text(issue_id),
                "title": dg.MetadataValue.text(title),
                "description": dg.MetadataValue.text(description),
                "project_id": dg.MetadataValue.text(str(project_id)),
                "priority": dg.MetadataValue.int(priority)
                if priority
                else dg.MetadataValue.text("None"),
                "estimate": dg.MetadataValue.int(estimate)
                if estimate
                else dg.MetadataValue.text("None"),
                "is_completed": dg.MetadataValue.bool(bool(completed_at)),
            }
        )


@definitions
def defs():
    """Linear asset definitions."""
    return dg.Definitions(
        assets=[compass_linear_issues],
    )
