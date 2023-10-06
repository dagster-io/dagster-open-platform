import os


def get_environment() -> str:
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1":
        return "BRANCH"
    if os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod":
        return "PROD"
    return "LOCAL"


def get_database_for_environment() -> str:
    env = get_environment()
    if env == "BRANCH":
        return f"PURINA_CLONE_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}"
    if env == "PROD":
        return "PURINA"
    return "SANDBOX"


def get_schema_for_environment(default_schema: str) -> str:
    """Returns the schema to use in the current environment. If the environment is LOCAL, it will use
    the SANDBOX_SCHEMA_NAME environment variable, otherwise will use a schema name that you pass to it.

    SANDBOX_SCHEMA_NAME is an environment variable you'll need to define locally.
    It is likely your first name. See the SANDBOX database in Snowflake for more details.
    """
    env = get_environment()
    if env == "LOCAL":
        return os.getenv("SANDBOX_SCHEMA_NAME", default_schema)
    return default_schema


def get_dbt_target():
    env = get_environment()
    if env == "BRANCH":
        return "branch_deployment"
    if env == "PROD":
        return "prod"
    return os.getenv("DBT_TARGET")
