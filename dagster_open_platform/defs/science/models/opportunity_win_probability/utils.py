import os

from dagster_open_platform.utils.environment_helpers import get_environment


def _database_from_env():
    if get_environment() == "PROD":
        return "DWH_SCIENCE"
    elif get_environment() == "BRANCH":
        return f"DWH_SCIENCE_CLONE_{os.environ.get('DAGSTER_CLOUD_PULL_REQUEST_ID')}"
    else:
        return "SANDBOX"


def _model_data_schema_from_env():
    if get_environment() == "PROD" or get_environment() == "BRANCH":
        return "MODEL_DATA"
    else:
        return os.environ.get("SNOWFLAKE_SCHEMA")


def _prediction_schema_from_env():
    if get_environment() == "PROD" or get_environment() == "BRANCH":
        return "MODEL_PREDICTIONS"
    else:
        return os.environ.get("SNOWFLAKE_SCHEMA")
