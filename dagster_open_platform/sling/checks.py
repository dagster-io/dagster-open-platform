from datetime import timedelta

from dagster import AssetKey, build_last_update_freshness_checks, build_sensor_for_freshness_checks

freshness_checks = build_last_update_freshness_checks(
    assets=[
        AssetKey(["sling", "cloud_product", "event_logs"]),
        AssetKey(["sling", "cloud_product_shard1", "event_logs"]),
    ],
    lower_bound_delta=timedelta(minutes=30),
)

freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=freshness_checks  # type: ignore
)
