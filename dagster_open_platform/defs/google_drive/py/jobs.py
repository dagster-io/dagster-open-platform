import io
from datetime import datetime

from dagster import Config, ResourceParam, get_dagster_logger, job, op
from dagster_open_platform.defs.google_drive.py.resources import GoogleDriveResource
from dagster_snowflake import SnowflakeResource

log = get_dagster_logger()

FOLDER_ID = "0AFpWAza7qVAdUk9PVA"

FULL_HISTORY_QUERY = "SELECT * FROM DWH_REPORTING.PRODUCT_ACTIVITY_LOGS.ANTHROPIC_API_COSTS"

WEEKLY_QUERY = """
SELECT *
FROM DWH_REPORTING.PRODUCT_ACTIVITY_LOGS.ANTHROPIC_API_COSTS
WHERE cost_date >= DATEADD('week', -1, DATE_TRUNC('week', CURRENT_DATE))
  AND cost_date < DATE_TRUNC('week', CURRENT_DATE)
""".strip()


class ExportConfig(Config):
    full_history: bool = False


@op
def export_anthropic_api_costs(
    config: ExportConfig,
    snowflake: ResourceParam[SnowflakeResource],
    google_drive: ResourceParam[GoogleDriveResource],
) -> None:
    query = FULL_HISTORY_QUERY if config.full_history else WEEKLY_QUERY
    label = "full_history" if config.full_history else "weekly"

    log.info(f"Running {label} export from Snowflake...")
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

    log.info(f"Fetched {len(rows):,} rows")

    buf = io.StringIO()
    buf.write(",".join(columns) + "\n")
    for row in rows:
        buf.write(",".join(_csv_cell(v) for v in row) + "\n")

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"anthropic_api_costs_{label}_{date_str}.csv"

    file_id = google_drive.upload_csv(buf.getvalue(), filename, FOLDER_ID)
    log.info(f"Uploaded '{filename}' to Drive (file ID: {file_id})")


@job(description="Export ANTHROPIC_API_COSTS from Snowflake to Google Drive.")
def anthropic_api_costs_to_drive() -> None:
    export_anthropic_api_costs()


def _csv_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(c in text for c in (",", '"', "\n")):
        text = '"' + text.replace('"', '""') + '"'
    return text
