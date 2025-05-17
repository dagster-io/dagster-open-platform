from dagster import AssetCheckResult, Definitions, asset_check
from dagster.components import definitions
from dagster_snowflake import SnowflakeResource


@asset_check(asset=["fivetran", "salesforce", "account"])
def account_has_valid_org_id(snowflake_sf: SnowflakeResource) -> AssetCheckResult:
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            select organization_id_c as org_id
            from fivetran.salesforce.account
            where organization_id_c not regexp '\\\\d+' and not is_deleted
        """)
        result = cur.fetch_pandas_all()
    return AssetCheckResult(
        passed=bool(len(result.index) == 0),
        metadata={
            "org_ids": result["ORG_ID"].to_list(),
        },
    )


@definitions
def defs():
    return Definitions(
        asset_checks=[account_has_valid_org_id],
    )
