from datetime import timedelta

from dagster import AssetKey, build_last_update_freshness_checks, build_sensor_for_freshness_checks

from .assets import cloud_product_main_low_volume, cloud_product_shard1_low_volume

event_log_freshness_checks = build_last_update_freshness_checks(
    assets=[
        AssetKey(["sling", "cloud_product", "event_logs"]),
        AssetKey(["sling", "cloud_product_shard1", "event_logs"]),
    ],
    lower_bound_delta=timedelta(minutes=30),
)

cloud_product_low_volume_freshness_checks = build_last_update_freshness_checks(
    assets=[cloud_product_main_low_volume, cloud_product_shard1_low_volume],
    lower_bound_delta=timedelta(days=1),
)

freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=event_log_freshness_checks + cloud_product_low_volume_freshness_checks  # type: ignore
)
