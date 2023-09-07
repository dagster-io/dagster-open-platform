from dagster import materialize
from purina_open.assets.health_check import health_check_asset


def test_health_check_asset():
    result = materialize([health_check_asset])
    assert result.success
