#!/usr/bin/env python3
"""Script to list Athena databases and tables accessible with given AWS credentials.

This script uses AWS credentials to query Athena and list all accessible databases
and their tables. Useful for verifying service account permissions.

Example usage:
    # Using environment variables
    export COMPASS_ATHENA_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
    export COMPASS_ATHENA_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    export COMPASS_ATHENA_WORKGROUP="compass-workgroup"
    export COMPASS_AWS_ACCOUNT_ID="123456789012"
    python list_athena_access.py

    # With command-line arguments (overrides environment variables)
    python list_athena_access.py \
        --access-key-id AKIAIOSFODNN7EXAMPLE \
        --secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
        --workgroup compass-workgroup \
        --account-id 123456789012

    # Filter to specific database prefix
    python list_athena_access.py --database-prefix dagster_plus_operational_data_org_

    # JSON output for scripting
    python list_athena_access.py --output json
"""

import argparse
import json
import os
import sys
from typing import Optional

import boto3


class AthenaAccessChecker:
    """Client for checking Athena database and table access."""

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        workgroup: str,
        account_id: str,
        region: str = "us-west-2",
    ):
        """Initialize the Athena access checker.

        Args:
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            workgroup: Athena workgroup name
            account_id: AWS account ID
            region: AWS region (default: us-west-2)
        """
        self.workgroup = workgroup
        self.account_id = account_id
        self.region = region

        # Initialize Athena client
        self.athena_client = boto3.client(
            "athena",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )

        # Initialize Glue client (used for metadata)
        self.glue_client = boto3.client(
            "glue",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )

        # Initialize STS client (used to get caller identity)
        self.sts_client = boto3.client(
            "sts",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
        )

        # Get and cache caller identity
        self._caller_identity = None

    def get_caller_identity(self) -> dict:
        """Get AWS caller identity information.

        Returns:
            Dictionary with UserId, Account, Arn, and extracted username
        """
        if self._caller_identity is None:
            response = self.sts_client.get_caller_identity()
            self._caller_identity = {
                "user_id": response["UserId"],
                "account": response["Account"],
                "arn": response["Arn"],
                "username": self._extract_username_from_arn(response["Arn"]),
            }
        return self._caller_identity

    @staticmethod
    def _extract_username_from_arn(arn: str) -> str:
        """Extract username from IAM ARN.

        Args:
            arn: IAM ARN (e.g., arn:aws:iam::123456789012:user/username)

        Returns:
            Username or the full ARN if parsing fails
        """
        # ARN format: arn:aws:iam::account:user/username
        # or: arn:aws:sts::account:assumed-role/role-name/session-name
        try:
            if ":user/" in arn:
                return arn.split(":user/")[1]
            if ":assumed-role/" in arn:
                # For assumed roles, return role-name/session-name
                parts = arn.split(":assumed-role/")[1]
                return f"assumed-role/{parts}"
            return arn
        except (IndexError, AttributeError):
            return arn

    def list_databases(self, prefix: Optional[str] = None) -> list[str]:
        """List all accessible databases.

        Args:
            prefix: Optional prefix to filter database names

        Returns:
            List of database names
        """
        databases = []
        paginator = self.glue_client.get_paginator("get_databases")

        try:
            for page in paginator.paginate():
                for db in page["DatabaseList"]:
                    db_name = db["Name"]
                    if prefix is None or db_name.startswith(prefix):
                        databases.append(db_name)
        except Exception as e:
            print(f"Error listing databases: {e}", file=sys.stderr)
            return []

        return sorted(databases)

    def list_tables(self, database: str) -> list[str]:
        """List all tables in a database.

        Args:
            database: Database name

        Returns:
            List of table names
        """
        tables = []
        paginator = self.glue_client.get_paginator("get_tables")

        try:
            for page in paginator.paginate(DatabaseName=database):
                for table in page["TableList"]:
                    tables.append(table["Name"])
        except Exception as e:
            print(f"Error listing tables in {database}: {e}", file=sys.stderr)
            return []

        return sorted(tables)

    def get_table_info(self, database: str, table: str) -> Optional[dict]:
        """Get detailed information about a table.

        Args:
            database: Database name
            table: Table name

        Returns:
            Dictionary with table information or None if error
        """
        try:
            response = self.glue_client.get_table(DatabaseName=database, Name=table)
            table_data = response["Table"]

            return {
                "name": table_data["Name"],
                "location": table_data.get("StorageDescriptor", {}).get("Location", "N/A"),
                "format": table_data.get("StorageDescriptor", {}).get("InputFormat", "N/A"),
                "columns": len(table_data.get("StorageDescriptor", {}).get("Columns", [])),
            }
        except Exception as e:
            print(f"Error getting info for {database}.{table}: {e}", file=sys.stderr)
            return None


def print_text_output(checker: AthenaAccessChecker, database_prefix: Optional[str], verbose: bool):
    """Print database and table information in human-readable format.

    Args:
        checker: AthenaAccessChecker instance
        database_prefix: Optional database name prefix filter
        verbose: Whether to include detailed table information
    """
    # Get caller identity
    try:
        identity = checker.get_caller_identity()
        print(f"AWS User: {identity['username']}")
        print(f"User ARN: {identity['arn']}")
    except Exception as e:
        print(f"Warning: Could not retrieve caller identity: {e}")

    print(f"Checking Athena access in region: {checker.region}")
    print(f"Workgroup: {checker.workgroup}")
    print(f"Account ID: {checker.account_id}")
    if database_prefix:
        print(f"Database prefix filter: {database_prefix}")
    print()

    # List databases
    print("Listing databases...")
    databases = checker.list_databases(prefix=database_prefix)

    if not databases:
        print("No accessible databases found.")
        return

    print(f"Found {len(databases)} accessible database(s):\n")

    # List tables for each database
    total_tables = 0
    for db in databases:
        print(f"📁 {db}")
        tables = checker.list_tables(db)

        if not tables:
            print("  └─ No tables found or access denied")
            continue

        total_tables += len(tables)

        for i, table in enumerate(tables):
            is_last = i == len(tables) - 1
            prefix = "  └─" if is_last else "  ├─"

            if verbose:
                table_info = checker.get_table_info(db, table)
                if table_info:
                    print(f"{prefix} {table}")
                    print(f"  {'   ' if is_last else '  │'}   Location: {table_info['location']}")
                    print(f"  {'   ' if is_last else '  │'}   Columns: {table_info['columns']}")
                else:
                    print(f"{prefix} {table} (info unavailable)")
            else:
                print(f"{prefix} {table}")

        print()

    print(f"Summary: {len(databases)} database(s), {total_tables} table(s)")


def print_json_output(checker: AthenaAccessChecker, database_prefix: Optional[str], verbose: bool):
    """Print database and table information in JSON format.

    Args:
        checker: AthenaAccessChecker instance
        database_prefix: Optional database name prefix filter
        verbose: Whether to include detailed table information
    """
    databases = checker.list_databases(prefix=database_prefix)

    # Get caller identity
    caller_identity = None
    try:
        identity = checker.get_caller_identity()
        caller_identity = {
            "username": identity["username"],
            "arn": identity["arn"],
            "user_id": identity["user_id"],
        }
    except Exception as e:
        caller_identity = {"error": str(e)}

    output = {
        "caller_identity": caller_identity,
        "region": checker.region,
        "workgroup": checker.workgroup,
        "account_id": checker.account_id,
        "database_prefix_filter": database_prefix,
        "databases": [],
    }

    for db in databases:
        tables = checker.list_tables(db)

        db_entry = {
            "name": db,
            "table_count": len(tables),
            "tables": [],
        }

        for table in tables:
            if verbose:
                table_info = checker.get_table_info(db, table)
                if table_info:
                    db_entry["tables"].append(table_info)
                else:
                    db_entry["tables"].append({"name": table, "error": "info unavailable"})
            else:
                db_entry["tables"].append({"name": table})

        output["databases"].append(db_entry)

    output["summary"] = {
        "total_databases": len(databases),
        "total_tables": sum(db["table_count"] for db in output["databases"]),
    }

    print(json.dumps(output, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="List Athena databases and tables accessible with given credentials",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--access-key-id",
        default=os.environ.get("COMPASS_ATHENA_ACCESS_KEY_ID"),
        help="AWS access key ID (default: $COMPASS_ATHENA_ACCESS_KEY_ID)",
    )
    parser.add_argument(
        "--secret-access-key",
        default=os.environ.get("COMPASS_ATHENA_SECRET_ACCESS_KEY"),
        help="AWS secret access key (default: $COMPASS_ATHENA_SECRET_ACCESS_KEY)",
    )
    parser.add_argument(
        "--workgroup",
        default=os.environ.get("COMPASS_ATHENA_WORKGROUP"),
        help="Athena workgroup name (default: $COMPASS_ATHENA_WORKGROUP)",
    )
    parser.add_argument(
        "--account-id",
        default=os.environ.get("COMPASS_AWS_ACCOUNT_ID"),
        help="AWS account ID (default: $COMPASS_AWS_ACCOUNT_ID)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-west-2"),
        help="AWS region (default: us-west-2 or $AWS_REGION)",
    )
    parser.add_argument(
        "--database-prefix",
        help="Filter databases by prefix (e.g., 'dagster_plus_operational_data_org_')",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include detailed table information (location, columns, etc.)",
    )

    args = parser.parse_args()

    # Validate required arguments
    if not args.access_key_id:
        print("Error: --access-key-id or COMPASS_ATHENA_ACCESS_KEY_ID required", file=sys.stderr)
        sys.exit(1)
    if not args.secret_access_key:
        print(
            "Error: --secret-access-key or COMPASS_ATHENA_SECRET_ACCESS_KEY required",
            file=sys.stderr,
        )
        sys.exit(1)
    if not args.workgroup:
        print("Error: --workgroup or COMPASS_ATHENA_WORKGROUP required", file=sys.stderr)
        sys.exit(1)
    if not args.account_id:
        print("Error: --account-id or COMPASS_AWS_ACCOUNT_ID required", file=sys.stderr)
        sys.exit(1)

    # Initialize checker
    checker = AthenaAccessChecker(
        access_key_id=args.access_key_id,
        secret_access_key=args.secret_access_key,
        workgroup=args.workgroup,
        account_id=args.account_id,
        region=args.region,
    )

    # Output results
    if args.output == "json":
        print_json_output(checker, args.database_prefix, args.verbose)
    else:
        print_text_output(checker, args.database_prefix, args.verbose)


if __name__ == "__main__":
    main()
