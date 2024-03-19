from dagster import AssetCheckResult, asset_check
from dagster_snowflake import SnowflakeResource


@asset_check(asset=["fivetran", "salesforce", "account"])
def account_has_valid_org_id(snowflake: SnowflakeResource) -> AssetCheckResult:
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            select organization_id_c as org_id
            from fivetran.salesforce.account
            where organization_id_c not regexp '\\\\d+' and not isdeleted           
        """)
        result = cur.fetch_pandas_all()
    return AssetCheckResult(
        passed=bool(len(result.index) == 0),
        metadata={
            "org_ids": result["ORG_ID"].to_list(),
        },
    )
