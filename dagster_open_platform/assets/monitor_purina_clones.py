from dagster import MaterializeResult, asset, get_dagster_logger
from dagster_snowflake import SnowflakeResource

log = get_dagster_logger()


@asset(
    name="inactive_snowflake_clones",
    description="Drops clone purina databases after 14 days of inactivity.",
)
def inactive_snowflake_clones(snowflake: SnowflakeResource) -> MaterializeResult:
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(r"""
            with
            recent_queries as (
                select
                    database_name, 
                    coalesce(
                        max(date(start_time)),
                        current_date - 30
                    ) as last_query_date
                from snowflake.account_usage.query_history
                where date(start_time) > current_date - 30
                group by all
            )
            select
                database_name,
                greatest(
                    date(created),
                    date(last_altered),
                    coalesce(last_query_date, current_date - 30)
                ) as last_activity,
                current_date - last_activity as days_since_last_activity
            from snowflake.information_schema.databases 
                left join recent_queries using(database_name)
            where
                database_name regexp $$PURINA_CLONE_\d+$$
                and days_since_last_activity > 14;        
        """)
        result = cur.fetch_pandas_all()
        dbs_to_drop = result["DATABASE_NAME"].to_list()
        if dbs_to_drop:
            for db in dbs_to_drop:
                log.info(f"Dropping {db}")
                cur.execute(f"drop database {db};")
                log.info(f"{db} dropped.")
        else:
            log.info("No databases to drop.")
    return MaterializeResult(
        metadata={"dropped_databases": dbs_to_drop, "dropped_databases_count": len(dbs_to_drop)},
    )
