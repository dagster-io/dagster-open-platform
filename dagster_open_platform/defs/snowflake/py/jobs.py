from dagster import Config, ResourceParam, job, op
from dagster_snowflake import SnowflakeConnection


class DatabaseCloneConfig(Config):
    pull_request_id: str


@op
def drop_database_clone(
    config: DatabaseCloneConfig,
    snowflake: ResourceParam[SnowflakeConnection],
):
    """Drops a clone of the Purina Snowflake database associated with a Pull Request,
    based on the pull request id provided in the config.
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"CALL UTIL_DB.PUBLIC.CLEANUP_DATABASE_CLONE('PURINA', '{config.pull_request_id}')"
        )
        cursor.execute(
            f"CALL UTIL_DB.PUBLIC.CLEANUP_DATABASE_CLONE('DWH_REPORTING', '{config.pull_request_id}')"
        )


@job(
    config={
        "ops": {
            "drop_database_clone": {
                "config": {
                    "pull_request_id": {"env": "DAGSTER_CLOUD_PULL_REQUEST_ID"},
                }
            }
        },
    },
    description="""Drops a clone of the Purina Snowflake database associated with a Pull Request.
This is automatically run when a Pull Request is closed via GitHub Action. The `drop_old_purina_clones`
sensor will also periodically run to clean up any clones that may have been overlooked.
""",
)
def drop_database_clones() -> None:
    drop_database_clone()
