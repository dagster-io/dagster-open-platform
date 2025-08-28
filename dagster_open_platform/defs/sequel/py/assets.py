# assets/sequel_events.py

import re
from datetime import datetime

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset, get_dagster_logger
from dagster_open_platform.defs.sequel.py.resources import SequelResource
from dagster_open_platform.defs.snowflake.py.resources import SnowflakeResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from snowflake.connector.pandas_tools import write_pandas

log = get_dagster_logger()

SEQUEL_BASE_URL = "https://api.introvoke.com/api/v1"
ANALYTICS_BASE_URL = "https://analytics.introvoke.com/api/analytics"


def validate_sql_identifier(identifier: str, name: str) -> str:
    """Validate that a SQL identifier only contains safe characters."""
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError(
            f"Invalid {name}: '{identifier}' contains unsafe characters. Only alphanumeric and underscore characters are allowed."
        )
    return identifier


def get_sequel_access_token(client_id: str, client_secret: str, audience: str) -> str:
    """Get OAuth2 access token from Sequel.io API."""
    token_url = "https://api.introvoke.com/api/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": "client_credentials",
    }

    response = requests.post(token_url, json=payload)
    response.raise_for_status()
    data = response.json()
    if "access_token" not in data:
        raise ValueError("Invalid API response: missing access_token")
    return data["access_token"]


def fetch_sequel_events(access_token: str, company_id: str) -> list[dict]:
    """Fetch all events from Sequel.io API."""
    events = []
    url = f"{SEQUEL_BASE_URL}/company/{company_id}/events"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    events = response.json()

    return events


def fetch_sequel_registrants(access_token: str, event_id: str) -> list[dict]:
    """Fetch all registrants for a specific event from Sequel.io API."""
    url = f"{SEQUEL_BASE_URL}/event/{event_id}/registrants"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    registrants = response.json()

    # Add event_id to each registrant record
    for registrant in registrants:
        registrant["event_id"] = event_id

    return registrants


def fetch_sequel_replays(access_token: str, event_id: str) -> list[dict]:
    """Fetch all replays for a specific event from Sequel.io API."""
    url = f"{SEQUEL_BASE_URL}/event/{event_id}/availableReplays"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    replays = response.json()

    # Add event_id to each replay record
    for replay in replays:
        replay["event_id"] = event_id

    return replays


def fetch_sequel_event_user_activity(
    access_token: str, event_id: str, count: int = 100, page: int = 1
) -> dict:
    """Fetch user activity data for a specific event from Sequel.io API."""
    url = f"{ANALYTICS_BASE_URL}/events/{event_id}/user-activity"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    params = {"count": count, "page": page}

    response = requests.post(url, headers=headers, params=params)
    response.raise_for_status()
    user_activity = response.json()
    print("Ran fetch_sequel_event_user_activity")

    log.info(f"User activity: {user_activity}")

    # Add event_id to the response
    user_activity["event_id"] = event_id

    return user_activity


def fetch_all_sequel_event_user_activity(access_token: str, event_id: str) -> list[dict]:
    """Fetch all user activity data for a specific event, handling pagination."""
    all_user_activities = []
    page = 1
    count = 100

    while True:
        try:
            user_activity_response = fetch_sequel_event_user_activity(
                access_token, event_id, count, page
            )

            if not user_activity_response.get("items"):
                break

            # Add event_id to each user activity item
            for item in user_activity_response["items"]:
                item["event_id"] = event_id
                item["total_count"] = user_activity_response.get("totalCount", 0)
                item["page"] = user_activity_response.get("page", page)
                item["page_size"] = user_activity_response.get("size", count)

            all_user_activities.extend(user_activity_response["items"])

            # Check if we've reached the end
            if len(user_activity_response["items"]) < count:
                break

            page += 1

        except Exception as e:
            # Log error and continue with next page if possible
            print(f"Error fetching page {page} for event {event_id}: {e}")
            break

    return all_user_activities


@asset(
    key=["sequel", "full_refresh", "events"],
    group_name="sequel",
    description="Fetch all events from Sequel.io API and load into Snowflake (full refresh).",
)
def sequel_events_full_refresh(
    context: AssetExecutionContext,
    snowflake_sequel: SnowflakeResource,
    sequel: SequelResource,
) -> None:
    """Fetch all events from Sequel.io API and load into Snowflake (full refresh)."""
    database_name = validate_sql_identifier(get_database_for_environment("SEQUEL"), "database_name")
    schema_name = validate_sql_identifier(get_schema_for_environment("FULL_REFRESH"), "schema_name")
    table_name = validate_sql_identifier("EVENTS", "table_name")

    context.log.info(f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}")
    context.log.info("Performing full refresh of events data")

    context.log.info("Getting access token from Sequel.io...")
    access_token = get_sequel_access_token(sequel.client_id, sequel.client_secret, sequel.audience)

    context.log.info("Fetching all events from Sequel.io...")
    events = fetch_sequel_events(access_token, sequel.company_id)
    context.log.info(f"Retrieved {len(events)} events for full refresh.")

    with snowflake_sequel.get_connection() as conn:
        with conn.cursor() as cursor:
            if events:
                # Convert events to pandas DataFrame
                events_df = pd.DataFrame(
                    [
                        {
                            "uid": event.get("uid"),
                            "name": event.get("name"),
                            "description": event.get("description"),
                            "start_date": event.get("startDate"),
                            "end_date": event.get("endDate"),
                            "timezone": event.get("timezone"),
                            "picture": event.get("picture"),
                            "is_event_cancelled": event.get("isEventCancelled"),
                            "is_parent_event": event.get("isParentEvent"),
                            "is_child_session_event": event.get("isChildSessionEvent"),
                            "is_in_person_event": event.get("isInPersonEvent"),
                            "is_simulive_enabled": event.get("isSimuliveEnabled"),
                            "is_event_live": event.get("isEventLive"),
                            "is_replay_enabled": event.get("isReplayEnabled"),
                            "registration_custom_url": event.get("registrationCustomUrl"),
                            "company_id": event.get("company", {}).get("id"),
                            "company_name": event.get("company", {}).get("name"),
                            "live_event_management_is_event_live": event.get(
                                "liveEventManagement", {}
                            ).get("isEventLive"),
                            "live_event_management_replay_enabled": event.get(
                                "liveEventManagement", {}
                            ).get("replayEnabled"),
                            "live_event_management_event_livestream_end_time": event.get(
                                "liveEventManagement", {}
                            ).get("eventLivestreamEndTime"),
                            "event_embed_status_status": event.get("eventEmbedStatus", {}).get(
                                "status"
                            ),
                            "event_embed_status_camera_disabled": event.get("eventEmbedStatus", {})
                            .get("issues", {})
                            .get("cameraDisabled"),
                            "event_embed_status_microphone_disabled": event.get(
                                "eventEmbedStatus", {}
                            )
                            .get("issues", {})
                            .get("microphoneDisabled"),
                            "event_embed_status_width_too_small": event.get("eventEmbedStatus", {})
                            .get("issues", {})
                            .get("widthTooSmall"),
                            "event_embed_status_height_too_small": event.get("eventEmbedStatus", {})
                            .get("issues", {})
                            .get("heightTooSmall"),
                            "_loaded_at": datetime.now().isoformat(),
                        }
                        for event in events
                    ]
                )

                # Write the new data directly to the target table
                context.log.info(
                    f"Loading {len(events)} events into {database_name}.{schema_name}.{table_name}"
                )
                write_pandas(
                    conn=conn,
                    df=events_df,
                    table_name=table_name,
                    database=database_name,
                    schema=schema_name,
                    overwrite=True,
                    auto_create_table=True,  # Let write_pandas create the table
                    quote_identifiers=False,
                )

            else:
                # If no events found, still truncate the table to ensure it's empty
                context.log.info(
                    f"No events found. Truncating table {database_name}.{schema_name}.{table_name}"
                )
                cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

            context.log.info("Full refresh completed successfully")

    context.log.info("Snowflake load complete.")


@asset(
    key=["sequel", "full_refresh", "registrants"],
    group_name="sequel",
    description="Fetch all registrants from Sequel.io API and load into Snowflake (full refresh).",
)
def sequel_registrants_full_refresh(
    context: AssetExecutionContext,
    snowflake_sequel: SnowflakeResource,
    sequel: SequelResource,
) -> None:
    """Fetch all registrants from Sequel.io API and load into Snowflake (full refresh)."""
    database_name = validate_sql_identifier(get_database_for_environment("SEQUEL"), "database_name")
    schema_name = validate_sql_identifier(get_schema_for_environment("FULL_REFRESH"), "schema_name")
    table_name = validate_sql_identifier("REGISTRANTS", "table_name")

    context.log.info(f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}")
    context.log.info("Performing full refresh of registrants data")

    context.log.info("Getting access token from Sequel.io...")
    access_token = get_sequel_access_token(sequel.client_id, sequel.client_secret, sequel.audience)

    context.log.info("Fetching all events to get event IDs...")
    # For full refresh, we need to get all events first, then check registrants for each event
    events = fetch_sequel_events(access_token, sequel.company_id)
    context.log.info(f"Found {len(events)} events to process.")

    all_registrants = []
    for event in events:
        event_id = event.get("uid")
        if not event_id:
            context.log.warning(f"Skipping event without uid: {event.get('name')}")
            continue

        context.log.info(f"Fetching registrants for event: {event.get('name')} (ID: {event_id})")

        try:
            # For full refresh, get all registrants for this event
            registrants = fetch_sequel_registrants(access_token, event_id)
            all_registrants.extend(registrants)
            context.log.info(f"Retrieved {len(registrants)} registrants for event {event_id}")
        except Exception as e:
            context.log.error(f"Error fetching registrants for event {event_id}: {e}")

    context.log.info(f"Total registrants retrieved: {len(all_registrants)}")

    with snowflake_sequel.get_connection() as conn:
        with conn.cursor() as cursor:
            if all_registrants:
                # Convert registrants to pandas DataFrame
                registrants_df = pd.DataFrame(
                    [
                        {
                            "event_id": registrant.get("event_id"),
                            "name": registrant.get("name"),
                            "email": registrant.get("email"),
                            "join_url": registrant.get("join_url"),
                            "metadata": registrant.get("metadata"),
                            "_loaded_at": datetime.now().isoformat(),
                        }
                        for registrant in all_registrants
                    ]
                )

                # Write the new data directly to the target table
                context.log.info(
                    f"Loading {len(all_registrants)} registrants into {database_name}.{schema_name}.{table_name}"
                )
                write_pandas(
                    conn=conn,
                    df=registrants_df,
                    table_name=table_name,
                    database=database_name,
                    schema=schema_name,
                    overwrite=True,
                    auto_create_table=True,  # Let write_pandas create the table
                    quote_identifiers=False,
                )

            else:
                # If no registrants found, still truncate the table to ensure it's empty
                context.log.info(
                    f"No registrants found. Truncating table {database_name}.{schema_name}.{table_name}"
                )
                cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

            context.log.info("Full refresh completed successfully")

    context.log.info("Snowflake load complete.")


@asset(
    key=["sequel", "full_refresh", "user_activity"],
    group_name="sequel",
    description="Fetch all event user activity logs from Sequel.io API and load into Snowflake (full refresh).",
)
def sequel_user_activity_full_refresh(
    context: AssetExecutionContext,
    snowflake_sequel: SnowflakeResource,
    sequel: SequelResource,
) -> None:
    """Fetch all event user activity logs from Sequel.io API and load into Snowflake (full refresh)."""
    database_name = validate_sql_identifier(get_database_for_environment("SEQUEL"), "database_name")
    schema_name = validate_sql_identifier(get_schema_for_environment("FULL_REFRESH"), "schema_name")
    table_name = validate_sql_identifier("USER_ACTIVITY", "table_name")

    context.log.info(f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}")
    context.log.info("Performing full refresh of user activity data")

    context.log.info("Getting access token from Sequel.io...")
    access_token = get_sequel_access_token(sequel.client_id, sequel.client_secret, sequel.audience)

    context.log.info("Fetching all events to get event IDs...")
    # For full refresh, we need to get all events first, then check user activity for each event
    events = fetch_sequel_events(access_token, sequel.company_id)
    context.log.info(f"Found {len(events)} events to process.")

    all_user_activity = []
    for event in events:
        event_id = event.get("uid")
        if not event_id:
            context.log.warning(f"Skipping event without uid: {event.get('name')}")
            continue

        context.log.info(f"Fetching user activity for event: {event.get('name')} (ID: {event_id})")

        try:
            # For full refresh, get all user activity for this event
            user_activity = fetch_all_sequel_event_user_activity(access_token, event_id)
            all_user_activity.extend(user_activity)
            context.log.info(
                f"Retrieved {len(user_activity)} user activity records for event {event_id}"
            )
        except Exception as e:
            context.log.error(f"Error fetching user activity for event {event_id}: {e}")

    context.log.info(f"Total user activity records retrieved: {len(all_user_activity)}")

    with snowflake_sequel.get_connection() as conn:
        with conn.cursor() as cursor:
            if all_user_activity:
                # Convert user activity to pandas DataFrame
                user_activity_df = pd.DataFrame(
                    [
                        {
                            "event_id": log.get("eventId"),
                            "email": log.get("email"),
                            "name": log.get("name"),
                            "attended": log.get("attended"),
                            "live_time": log.get("liveTime"),
                            "lead_score": log.get("leadScore"),
                            "engagement_score": log.get("engagementScore"),
                            "raw_lead_score": log.get("rawLeadScore"),
                            "lead_score_details": log.get("leadScoreDetails"),
                            "live_time_spent_percentage": log.get("liveTimeSpentPercentage"),
                            "on_demand_time_spent_percentage": log.get(
                                "onDemandTimeSpentPercentage"
                            ),
                            "average_live_view_time": log.get("averageLiveViewTime"),
                            "average_on_demand_view_time": log.get("averageOnDemandViewTime"),
                            "on_demand_time": log.get("onDemandTime"),
                            "circles_number": log.get("circlesNumber"),
                            "comments": log.get("comments"),
                            "company_id": log.get("companyId"),
                            "created_on": log.get("createdOn"),
                            "updated_on": log.get("updatedOn"),
                            "polls_number": log.get("pollsNumber"),
                            "questions_number": log.get("questionsNumber"),
                            "questions": log.get("questions"),
                            "registration_date": log.get("registrationDate"),
                            "uid": log.get("uid"),
                            "user_id": log.get("userId"),
                            "viewed_replay": log.get("viewedReplay"),
                            "lead_status": log.get("leadStatus"),
                            "messages_reactions_count": log.get("messagesReactionsCount"),
                            "_loaded_at": datetime.now().isoformat(),
                        }
                        for log in all_user_activity
                    ]
                )

                # Write the new data directly to the target table
                context.log.info(
                    f"Loading {len(all_user_activity)} user activity records into {database_name}.{schema_name}.{table_name}"
                )
                write_pandas(
                    conn=conn,
                    df=user_activity_df,
                    table_name=table_name,
                    database=database_name,
                    schema=schema_name,
                    overwrite=True,
                    auto_create_table=True,  # Let write_pandas create the table
                    quote_identifiers=False,
                )

            else:
                # If no user activity found, still truncate the table to ensure it's empty
                context.log.info(
                    f"No user activity found. Truncating table {database_name}.{schema_name}.{table_name}"
                )
                cursor.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

            context.log.info("Full refresh completed successfully")

    context.log.info("Snowflake load complete.")
