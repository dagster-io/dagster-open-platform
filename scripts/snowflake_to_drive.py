#!/usr/bin/env python3
"""Export a Snowflake table to CSV and upload it to Google Drive.

Reads DWH_REPORTING.PRODUCT_ACTIVITY_LOGS.ANTHROPIC_API_COSTS (or any table
passed via --table) and uploads a timestamped CSV to a Google Drive folder.

Intended to be run weekly (e.g. via cron or a Dagster schedule).

Prerequisites:
  1. Create a Google Cloud service account and download its JSON key.
  2. Share the target Drive folder with the service account email (Editor).
  3. Enable the Google Drive API in your GCP project.

Environment variables (all required unless --args are used):
    SNOWFLAKE_ACCOUNT             Snowflake account identifier (e.g. ax61354.us-west-2)
    SNOWFLAKE_USER                Snowflake username
    SNOWFLAKE_PRIVATE_KEY_PATH    Path to PEM private key file (preferred for service accounts)
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  Passphrase for encrypted private key (optional)
    SNOWFLAKE_PASSWORD            Snowflake password (fallback if no private key)
    SNOWFLAKE_ROLE                Snowflake role (default: REPORTING)
    SNOWFLAKE_WAREHOUSE           Snowflake warehouse (default: REPORTING)
    GOOGLE_SERVICE_ACCOUNT_JSON   Path to service account JSON key file

Example usage:
    # Service account with private key (recommended for automation)
    export SNOWFLAKE_ACCOUNT="ax61354.us-west-2"
    export SNOWFLAKE_USER="GTM_SERVICE_USER"
    export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"
    export GOOGLE_SERVICE_ACCOUNT_JSON="/path/to/sa-key.json"
    python snowflake_to_drive.py

    # Export a different table
    python snowflake_to_drive.py --table MY_DB.MY_SCHEMA.MY_TABLE
"""

import argparse
import io
import os
import sys
from datetime import datetime

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

DEFAULT_TABLE = "DWH_REPORTING.PRODUCT_ACTIVITY_LOGS.ANTHROPIC_API_COSTS"
DEFAULT_FOLDER_ID = "0AFpWAza7qVAdUk9PVA"
DEFAULT_ROLE = "REPORTING"
DEFAULT_WAREHOUSE = "REPORTING"


class SnowflakeToDriveExporter:
    def __init__(
        self,
        *,
        snowflake_account: str,
        snowflake_user: str,
        snowflake_private_key_path: str | None,
        snowflake_private_key_passphrase: str | None,
        snowflake_password: str | None,
        snowflake_role: str,
        snowflake_warehouse: str,
        service_account_json: str,
        folder_id: str,
    ):
        self.snowflake_account = snowflake_account
        self.snowflake_user = snowflake_user
        self.snowflake_private_key_path = snowflake_private_key_path
        self.snowflake_private_key_passphrase = snowflake_private_key_passphrase
        self.snowflake_password = snowflake_password
        self.snowflake_role = snowflake_role
        self.snowflake_warehouse = snowflake_warehouse
        self.service_account_json = service_account_json
        self.folder_id = folder_id

    def _snowflake_connect(self) -> snowflake.connector.SnowflakeConnection:
        kwargs: dict = {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "role": self.snowflake_role,
            "warehouse": self.snowflake_warehouse,
        }
        if self.snowflake_private_key_path:
            passphrase = (
                self.snowflake_private_key_passphrase.encode()
                if self.snowflake_private_key_passphrase
                else None
            )
            with open(self.snowflake_private_key_path, "rb") as f:
                private_key = load_pem_private_key(
                    f.read(), password=passphrase, backend=default_backend()
                )
            kwargs["private_key"] = private_key.private_bytes(
                encoding=Encoding.DER,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
        elif self.snowflake_password:
            kwargs["password"] = self.snowflake_password
        else:
            kwargs["authenticator"] = "externalbrowser"
        return snowflake.connector.connect(**kwargs)

    def query_to_csv(self, table: str) -> tuple[str, int]:
        """Query the table and return (csv_content, row_count)."""
        parts = table.split(".")
        if len(parts) != 3 or not all(part.replace("_", "").isalnum() for part in parts):
            raise ValueError(
                f"Invalid table name '{table}': must be DB.SCHEMA.TABLE with alphanumeric/underscore parts"
            )
        quoted = ".".join(f'"{part}"' for part in parts)

        print(f"Connecting to Snowflake as {self.snowflake_user} ...")
        conn = self._snowflake_connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {quoted}")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        finally:
            conn.close()

        buf = io.StringIO()
        buf.write(",".join(columns) + "\n")
        for row in rows:
            buf.write(",".join(_csv_cell(v) for v in row) + "\n")

        print(f"Fetched {len(rows):,} rows from {table}")
        return buf.getvalue(), len(rows)

    def upload_to_drive(self, csv_content: str, filename: str) -> str:
        """Upload csv_content to Google Drive, return the file ID."""
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_json,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        media = MediaIoBaseUpload(
            io.BytesIO(csv_content.encode("utf-8")),
            mimetype="text/csv",
            resumable=False,
        )
        metadata = {"name": filename, "parents": [self.folder_id]}
        result = service.files().create(body=metadata, media_body=media, fields="id").execute()
        file_id: str = result["id"]
        print(f"Uploaded '{filename}' to Drive folder {self.folder_id} (file ID: {file_id})")
        return file_id


def _csv_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(c in text for c in (",", '"', "\n")):
        text = '"' + text.replace('"', '""') + '"'
    return text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a Snowflake table to CSV and upload to Google Drive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Fully-qualified Snowflake table name (default: {DEFAULT_TABLE})",
    )
    parser.add_argument(
        "--folder-id",
        default=os.environ.get("GOOGLE_DRIVE_FOLDER_ID", DEFAULT_FOLDER_ID),
        help=f"Google Drive folder ID (default: {DEFAULT_FOLDER_ID})",
    )
    parser.add_argument(
        "--snowflake-account",
        default=os.environ.get("SNOWFLAKE_ACCOUNT"),
        help="Snowflake account identifier (default: $SNOWFLAKE_ACCOUNT)",
    )
    parser.add_argument(
        "--snowflake-user",
        default=os.environ.get("SNOWFLAKE_USER"),
        help="Snowflake username (default: $SNOWFLAKE_USER)",
    )
    parser.add_argument(
        "--private-key-path",
        default=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        help="Path to PEM private key file (default: $SNOWFLAKE_PRIVATE_KEY_PATH)",
    )
    parser.add_argument(
        "--private-key-passphrase",
        default=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        help="Passphrase for encrypted private key (default: $SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)",
    )
    parser.add_argument(
        "--snowflake-password",
        default=os.environ.get("SNOWFLAKE_PASSWORD"),
        help="Snowflake password (fallback if no private key; default: $SNOWFLAKE_PASSWORD)",
    )
    parser.add_argument(
        "--snowflake-role",
        default=os.environ.get("SNOWFLAKE_ROLE", DEFAULT_ROLE),
        help=f"Snowflake role (default: {DEFAULT_ROLE})",
    )
    parser.add_argument(
        "--snowflake-warehouse",
        default=os.environ.get("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
        help=f"Snowflake warehouse (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--service-account-json",
        default=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"),
        help="Path to Google service account JSON key (default: $GOOGLE_SERVICE_ACCOUNT_JSON)",
    )
    parser.add_argument(
        "--filename",
        default=None,
        help="Override the output filename (default: auto-generated with today's date)",
    )

    args = parser.parse_args()

    # Validate required args
    missing = []
    if not args.snowflake_account:
        missing.append("--snowflake-account / $SNOWFLAKE_ACCOUNT")
    if not args.snowflake_user:
        missing.append("--snowflake-user / $SNOWFLAKE_USER")
    if not args.service_account_json:
        missing.append("--service-account-json / $GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        for m in missing:
            print(f"Error: missing required argument: {m}", file=sys.stderr)
        sys.exit(1)

    filename = args.filename or _default_filename(args.table)

    exporter = SnowflakeToDriveExporter(
        snowflake_account=args.snowflake_account,
        snowflake_user=args.snowflake_user,
        snowflake_private_key_path=args.private_key_path,
        snowflake_private_key_passphrase=args.private_key_passphrase,
        snowflake_password=args.snowflake_password,
        snowflake_role=args.snowflake_role,
        snowflake_warehouse=args.snowflake_warehouse,
        service_account_json=args.service_account_json,
        folder_id=args.folder_id,
    )

    csv_content, _row_count = exporter.query_to_csv(args.table)
    exporter.upload_to_drive(csv_content, filename)


def _default_filename(table: str) -> str:
    table_short = table.split(".")[-1].lower()
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"{table_short}_{date_str}.csv"


if __name__ == "__main__":
    main()
