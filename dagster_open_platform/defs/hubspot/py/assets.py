# assets/hubspot_contacts.py

import requests
from dagster import AssetExecutionContext, AutomationCondition, Definitions, asset
from dagster.components import definitions
from dagster_open_platform.defs.hubspot.py.resources import HubSpotResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

HUBSPOT_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts"


def fetch_hubspot_contact_ids(access_token: str) -> list[str]:
    contact_ids = []
    url = HUBSPOT_BASE_URL
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    params = {"limit": 100, "properties": "hs_object_id", "archived": False}

    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        for contact in data.get("results", []):
            contact_ids.append(contact["id"])

        paging = data.get("paging")
        if paging and "next" in paging:
            url = paging["next"]["link"]
            params = {}  # clear params because link includes all
        else:
            break

    return contact_ids


@asset(
    key=["hubspot", "full_refresh", "contact_ids"],
    group_name="hubspot",
    description="Fetch contact IDs from HubSpot and load into Snowflake (full refresh).",
    automation_condition=AutomationCondition.on_cron(cron_schedule="0 0 * * *"),
)
def hubspot_contacts(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    hubspot: HubSpotResource,
) -> None:
    """Fetch contact IDs from HubSpot and load into Snowflake (full refresh)."""
    context.log.info("Fetching contact IDs from HubSpot...")
    contact_ids = fetch_hubspot_contact_ids(hubspot.access_token)
    context.log.info(f"Retrieved {len(contact_ids)} contact IDs.")

    database_name, schema_name, table_name = context.asset_key.path
    database_name = get_database_for_environment(database_name)
    schema_name = get_schema_for_environment(schema_name)

    context.log.info(f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}")

    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            context.log.info(
                f"Creating table {database_name}.{schema_name}.{table_name} if it doesn't exist..."
            )
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
                    hubspot_contact_id STRING
                )
            """)
            context.log.info(f"Truncating table {database_name}.{schema_name}.{table_name}...")
            cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

            insert_query = f"INSERT INTO {database_name}.{schema_name}.{table_name} (hubspot_contact_id) VALUES (%s)"
            contact_data = [(cid,) for cid in contact_ids]

            context.log.info(f"Inserting {len(contact_data)} records...")
            cursor.executemany(insert_query, contact_data)

    context.log.info("Snowflake load complete.")


@definitions
def defs():
    return Definitions(
        assets=[hubspot_contacts],
    )
