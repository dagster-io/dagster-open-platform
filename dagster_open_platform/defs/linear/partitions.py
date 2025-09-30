import os

import dagster as dg
from dagster import Definitions, SensorEvaluationContext, SensorResult, get_dagster_logger, sensor
from dagster.components import definitions
from dagster_snowflake import SnowflakeResource

linear_issues_partition_def = dg.DynamicPartitionsDefinition(name="linear_issues")

log = get_dagster_logger()

# Target project ID for the Linear issues we want to partition by
LINEAR_PROJECT_ID = os.getenv("LINEAR_PROJECT_ID")


@sensor(description="Sensor to manage partitions for Linear issues in the target project")
def linear_issues_partition_sensor(context: SensorEvaluationContext, snowflake: SnowflakeResource):
    """This sensor queries the fivetran.linear.issues table to:
    1. Add new partitions for issues in the target project that don't have partitions yet.
    2. Remove partitions for issues marked as Done (completed_at is not null).
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Get current partition keys
        current_partition_keys = set(
            linear_issues_partition_def.get_partition_keys(
                dynamic_partitions_store=context.instance
            )
        )

        log.info(f"Current partition keys count: {len(current_partition_keys)}")

        # Query for all issues in the target project that are not deleted
        # and get their IDs and completion status. We only want issues that have the 'ready-to-draft' label.
        query = """
        SELECT
            issue.id,
            completed_at IS NOT NULL as is_completed,
            label.name as label_name
        FROM fivetran.linear.issue
            LEFT JOIN fivetran.linear.issue_label on issue.id = issue_label.issue_id
            LEFT JOIN fivetran.linear.label on issue_label.label_id = label.id
        WHERE project_id = %s AND label.name = 'ready-to-draft'
        AND NOT issue._fivetran_deleted
        """

        cursor.execute(query, (LINEAR_PROJECT_ID,))
        issues_data = cursor.fetchall()

        log.info(f"Found {len(issues_data)} issues in project {LINEAR_PROJECT_ID}")

        # Separate active issues (not completed) and completed issues
        active_issue_ids = set()
        completed_issue_ids = set()

        for issue_id, is_completed, label_name in issues_data:
            if is_completed:
                completed_issue_ids.add(issue_id)
            else:
                active_issue_ids.add(issue_id)

        log.info(
            f"Active issues: {len(active_issue_ids)}, Completed issues: {len(completed_issue_ids)}"
        )

        # Determine which partitions to add and remove
        partitions_to_add = active_issue_ids - current_partition_keys
        partitions_to_remove = current_partition_keys & completed_issue_ids

        log.info(f"Partitions to add: {len(partitions_to_add)}")
        log.info(f"Partitions to remove: {len(partitions_to_remove)}")

        requests = []

        # Add new partitions for active issues
        if partitions_to_add:
            requests.append(linear_issues_partition_def.build_add_request(list(partitions_to_add)))

        # Remove partitions for completed issues
        if partitions_to_remove:
            requests.append(
                linear_issues_partition_def.build_delete_request(list(partitions_to_remove))
            )

        return SensorResult(dynamic_partitions_requests=requests)


@definitions
def defs():
    return Definitions(sensors=[linear_issues_partition_sensor])
