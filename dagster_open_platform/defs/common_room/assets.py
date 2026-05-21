"""Common Room → Snowflake pipeline.

Architecture:
  Common Room webhook
    → Lambda Function URL  (managed by common_room_ingest_lambda Dagster asset)
    → S3  s3://BUCKET/webhooks/YYYY/MM/DD/<ts>-<id>.json
    → Snowflake stage  aws.<schema>.stage_common_room_webhooks  (refreshed hourly)
    → common_room_signals  COPY INTO aws.<schema>.COMMON_ROOM_SIGNALS  (runs at :15)

Snowflake tracks loaded files natively (COPY INTO without FORCE), so each
webhook JSON file is ingested exactly once regardless of how many times the
asset runs.

Deploying the Lambda
--------------------
Run the `common_room_ingest_lambda` asset manually whenever the handler code
changes. The function_url metadata shows the URL to configure in Common Room.
Required env vars in Dagster Cloud:
  COMMON_ROOM_LAMBDA_ROLE_ARN   — IAM execution role (from Terraform output)
  COMMON_ROOM_BUCKET            — S3 bucket name
  COMMON_ROOM_WEBHOOK_SECRET    — shared secret validated by the Lambda
"""

import io
import os
import pathlib
import textwrap
import zipfile

import boto3
import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_schema_for_environment
from dagster_snowflake import SnowflakeResource

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_AWS_DB = "aws"
_DEST_TABLE = "COMMON_ROOM_SIGNALS"

# Lambda deployment constants
_FUNCTION_NAME = "elementl_common_room_ingest"
_RUNTIME = "python3.12"
_REGION = "us-west-2"
_TIMEOUT = 30

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _aws_schema() -> str:
    """The schema name inside the AWS database (mirrors schema_from_env() in YAML)."""
    return get_schema_for_environment("ELEMENTL")


def _dest_table() -> str:
    return f"{_AWS_DB}.{_aws_schema()}.{_DEST_TABLE}"


def _create_table_ddl(table: str) -> str:
    return textwrap.dedent(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_record_id         VARCHAR     NOT NULL,
            workflow_name            VARCHAR,
            definition_key           VARCHAR,
            observed_at              TIMESTAMP_NTZ,
            replication_date         DATE,
            sfdc_account_id          VARCHAR,
            member_name              VARCHAR,
            member_title             VARCHAR,
            previous_title           VARCHAR,
            current_org_name         VARCHAR,
            current_org_domain       VARCHAR,
            previous_org_name        VARCHAR,
            previous_org_domain      VARCHAR,
            organization_change_date TIMESTAMP_NTZ,
            linkedin_url             VARCHAR,
            common_room_contact_link VARCHAR,
            common_room_activity_url VARCHAR,
            external_activity_url    VARCHAR,
            activity_type            VARCHAR,
            raw_payload              VARIANT,
            ingested_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT pk_cr_signals PRIMARY KEY (source_record_id)
        )
    """).strip()


# Map workflow name → definition key (mirrors ingest-recognizers.ts)
_WORKFLOW_TO_DEF_KEY: dict[str, str] = {
    "personnel_change": "common_room.personnel_change",
    "case_study_published": "common_room.case_study_published",
    "product_announcement": "common_room.product_announcement",
    "fundraise": "common_room.fundraise",
    "acquisition": "common_room.acquisition",
    "job_posting_dagster": "common_room.job_posting_dagster",
}


# ---------------------------------------------------------------------------
# Asset: deploy / update the ingest Lambda
# ---------------------------------------------------------------------------


@dg.asset(
    key=["common_room", "ingest_lambda"],
    group_name="common_room",
    description=(
        "Deploys or updates the Common Room webhook ingest Lambda in AWS. "
        "Creates the function on first run; updates code + config on subsequent runs. "
        "The function_url metadata shows the URL to configure in Common Room's webhook settings. "
        "Run this asset manually whenever lambda_functions/common_room_ingest.py changes."
    ),
    kinds={"aws"},
    owners=["team:gtm"],
    # Infrastructure asset — trigger manually, not on a schedule.
)
def common_room_ingest_lambda(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Create or update the Common Room ingest Lambda.

    On first run:
      1. Creates the Lambda function with the handler code zip.
      2. Attaches a public Function URL (no auth — secret validated in handler).
      3. Grants lambda:InvokeFunctionUrl to * so Common Room can call it.

    On subsequent runs:
      1. Updates the function code zip.
      2. Updates the environment variables.

    Required env vars:
      COMMON_ROOM_LAMBDA_ROLE_ARN   — ARN of the IAM execution role (from Terraform output)
      COMMON_ROOM_BUCKET            — destination S3 bucket name
      COMMON_ROOM_WEBHOOK_SECRET    — shared secret validated by the Lambda handler
    """
    handler_path = (
        pathlib.Path(__file__).parent.parent.parent / "lambda_functions" / "common_room_ingest.py"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(handler_path, arcname="lambda_function.py")
    zip_bytes = buf.getvalue()

    role_arn = os.environ["COMMON_ROOM_LAMBDA_ROLE_ARN"]
    bucket = os.environ["COMMON_ROOM_BUCKET"]
    webhook_secret = os.environ["COMMON_ROOM_WEBHOOK_SECRET"]

    env_vars = {
        "COMMON_ROOM_BUCKET": bucket,
        "COMMON_ROOM_WEBHOOK_SECRET": webhook_secret,
    }

    client = boto3.client("lambda", region_name=_REGION)

    try:
        client.get_function(FunctionName=_FUNCTION_NAME)
        exists = True
    except client.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        context.log.info(f"Updating existing Lambda: {_FUNCTION_NAME}")
        client.update_function_code(
            FunctionName=_FUNCTION_NAME,
            ZipFile=zip_bytes,
        )
        # Wait for code update before touching config.
        client.get_waiter("function_updated").wait(FunctionName=_FUNCTION_NAME)
        client.update_function_configuration(
            FunctionName=_FUNCTION_NAME,
            Environment={"Variables": env_vars},
        )
        # Ensure Function URL and public-invoke permission exist — they may be
        # absent if a previous first-run failed between create_function and
        # create_function_url_config.
        try:
            client.get_function_url_config(FunctionName=_FUNCTION_NAME)
        except client.exceptions.ResourceNotFoundException:
            context.log.info("Function URL missing — creating it now")
            client.create_function_url_config(
                FunctionName=_FUNCTION_NAME,
                AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["POST"]},
            )
            client.add_permission(
                FunctionName=_FUNCTION_NAME,
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        action = "updated"
    else:
        context.log.info(f"Creating new Lambda: {_FUNCTION_NAME}")
        client.create_function(
            FunctionName=_FUNCTION_NAME,
            Runtime=_RUNTIME,
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Timeout=_TIMEOUT,
            Environment={"Variables": env_vars},
        )
        client.get_waiter("function_active").wait(FunctionName=_FUNCTION_NAME)
        client.create_function_url_config(
            FunctionName=_FUNCTION_NAME,
            AuthType="NONE",
            Cors={"AllowOrigins": ["*"], "AllowMethods": ["POST"]},
        )
        client.add_permission(
            FunctionName=_FUNCTION_NAME,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
        action = "created"

    url_config = client.get_function_url_config(FunctionName=_FUNCTION_NAME)
    function_url = url_config["FunctionUrl"]
    function_arn = client.get_function(FunctionName=_FUNCTION_NAME)["Configuration"]["FunctionArn"]

    context.log.info(f"Lambda {action}: {function_url}")

    return dg.MaterializeResult(
        metadata={
            "function_url": dg.MetadataValue.url(function_url),
            "function_arn": dg.MetadataValue.text(function_arn),
            "action": dg.MetadataValue.text(action),
        }
    )


# ---------------------------------------------------------------------------
# Asset: normalise
# ---------------------------------------------------------------------------


@dg.asset(
    key=["common_room", "signals"],
    group_name="common_room",
    description=(
        "Loads Common Room webhook payloads from S3 into "
        "aws.<schema>.COMMON_ROOM_SIGNALS via COPY INTO. "
        "Idempotent — Snowflake tracks loaded files natively."
    ),
    # Run hourly at :15, shortly after the webhook stage refreshes at :00.
    automation_condition=dg.AutomationCondition.on_cron("15 * * * *"),
    kinds={"snowflake"},
    owners=["team:gtm"],
    deps=[
        dg.AssetKey(["aws", _aws_schema(), "stage_common_room_webhooks"]),
    ],
)
def common_room_signals(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Load new Common Room webhook payloads into aws.<schema>.COMMON_ROOM_SIGNALS.

    Uses COPY INTO from the S3 stage instead of an external table. Snowflake
    tracks which files have already been loaded, so re-runs skip files that
    were ingested in a prior run — each JSON file is loaded exactly once.

    The stage contains one JSON file per webhook event written by the Lambda:
      s3://BUCKET/webhooks/YYYY/MM/DD/<ts>-<request-id>.json

    $1 in the transformation query is the parsed JSON root of each file.
    METADATA$FILENAME is used to derive the replication date from the key path.

    Common Room webhook shape:
      {
        "id": "...",
        "source": { "type": "workflow", "name": "personnel_change" },
        "payload": {
          "member": { "fullName": ..., "organization": {...}, ... },
          "salesforceAccountId": "...",
          "timestamp": "...",
          "commonRoomActivityUrl": "..."
        }
      }
    """
    aws_schema = _aws_schema()
    table = _dest_table()
    stage = f"{_AWS_DB}.{aws_schema}.stage_common_room_webhooks"

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # The AWS database requires AWS_WRITER role for DDL and COPY INTO.
        cursor.execute("USE ROLE AWS_WRITER;")

        # Ensure destination table exists.
        cursor.execute(_create_table_ddl(table))

        # ------------------------------------------------------------------
        # COPY INTO with a transformation query.
        # Each file in the stage is tracked; FORCE = FALSE (default) means
        # already-loaded files are skipped automatically.
        # Field paths mirror ingest-recognizers.ts in sorted-pie.
        # METADATA$FILENAME is relative to the stage prefix (s3://BUCKET/webhooks),
        # so paths are YYYY/MM/DD/<ts>-<id>.json — split_part indices start at 1.
        # ------------------------------------------------------------------
        case_branches = "\n                    ".join(
            f"WHEN LOWER(REGEXP_REPLACE(TRIM($1:source:name::VARCHAR), '[\\\\s\\\\-]+', '_')) = '{wf}' THEN '{dk}'"
            for wf, dk in _WORKFLOW_TO_DEF_KEY.items()
        )

        copy_sql = textwrap.dedent(f"""
            COPY INTO {table} (
                source_record_id, workflow_name, definition_key, observed_at,
                replication_date, sfdc_account_id, member_name, member_title,
                previous_title, current_org_name, current_org_domain,
                previous_org_name, previous_org_domain, organization_change_date,
                linkedin_url, common_room_contact_link, common_room_activity_url,
                external_activity_url, activity_type, raw_payload
            )
            FROM (
                SELECT
                    COALESCE(
                        $1:id::VARCHAR,
                        MD5($1::VARCHAR)
                    )                                                        AS source_record_id,
                    LOWER(REGEXP_REPLACE(
                        TRIM(COALESCE($1:source:name::VARCHAR, 'activity')),
                        '[\\\\s\\\\-]+', '_'
                    ))                                                       AS workflow_name,
                    CASE
                        {case_branches}
                        ELSE 'common_room.activity'
                    END                                                      AS definition_key,
                    COALESCE(
                        $1:payload:timestamp::TIMESTAMP_NTZ,
                        $1:payload:occurredAt::TIMESTAMP_NTZ,
                        $1:payload:lastActivityDate::TIMESTAMP_NTZ,
                        TRY_TO_TIMESTAMP_NTZ(
                            split_part(METADATA$FILENAME, '/', 1) || '-' ||
                            split_part(METADATA$FILENAME, '/', 2) || '-' ||
                            split_part(METADATA$FILENAME, '/', 3)
                        )
                    )                                                        AS observed_at,
                    TRY_TO_DATE(
                        split_part(METADATA$FILENAME, '/', 1) || '-' ||
                        split_part(METADATA$FILENAME, '/', 2) || '-' ||
                        split_part(METADATA$FILENAME, '/', 3)
                    )                                                        AS replication_date,
                    COALESCE(
                        $1:payload:salesforceAccountId::VARCHAR,
                        $1:payload:member:organization:salesforceAccountId::VARCHAR,
                        $1:payload:member:company:salesforceAccountId::VARCHAR,
                        $1:payload:company:salesforceAccountId::VARCHAR,
                        $1:payload:organization:salesforceAccountId::VARCHAR
                    )                                                        AS sfdc_account_id,
                    COALESCE(
                        $1:payload:member:fullName::VARCHAR,
                        $1:payload:member:firstName::VARCHAR || ' ' || $1:payload:member:lastName::VARCHAR,
                        $1:payload:fullName::VARCHAR
                    )                                                        AS member_name,
                    $1:payload:member:title::VARCHAR                         AS member_title,
                    $1:payload:member:previousTitle::VARCHAR                 AS previous_title,
                    COALESCE(
                        $1:payload:member:organization:name::VARCHAR,
                        $1:payload:organization:name::VARCHAR,
                        $1:payload:company:name::VARCHAR
                    )                                                        AS current_org_name,
                    COALESCE(
                        $1:payload:member:organization:domain::VARCHAR,
                        $1:payload:organization:domain::VARCHAR,
                        $1:payload:company:domain::VARCHAR
                    )                                                        AS current_org_domain,
                    $1:payload:member:previousOrganization:name::VARCHAR     AS previous_org_name,
                    $1:payload:member:previousOrganization:domain::VARCHAR   AS previous_org_domain,
                    $1:payload:member:organizationChangeDate::TIMESTAMP_NTZ  AS organization_change_date,
                    $1:payload:member:linkedInUrl::VARCHAR                   AS linkedin_url,
                    COALESCE(
                        $1:payload:member:commonRoomContactLink::VARCHAR,
                        $1:payload:commonRoomContactLink::VARCHAR
                    )                                                        AS common_room_contact_link,
                    $1:payload:commonRoomActivityUrl::VARCHAR                AS common_room_activity_url,
                    $1:payload:externalActivityUrl::VARCHAR                  AS external_activity_url,
                    $1:payload:activityType::VARCHAR                         AS activity_type,
                    $1                                                       AS raw_payload
                FROM @{stage}
            )
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = CONTINUE
        """).strip()

        cursor.execute(copy_sql)
        # COPY INTO returns one result row per file processed; column index 3
        # is rows_loaded for that file. Sum across all files for the true total.
        rows_loaded: int = sum(row[3] for row in cursor.fetchall())
        context.log.info(f"COPY INTO: {rows_loaded} new rows loaded")

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(rows_loaded),
        }
    )
