from dagster import Definitions, asset
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models
from dagster_open_platform.defs.science.models.opportunity_win_probability.model_opportunity_win_probability import (
    predict_and_write_to_snowflake,
)


@asset(
    key=["dwh_science", "model_predictions", "opportunity_win_probability_predictions"],
    deps=[
        get_asset_key_for_model(
            [get_dbt_non_partitioned_models()], "model_opportunity_win_probability"
        )
    ],
    group_name="model_predictions",
    tags={
        "dagster/kind/scikitlearn": "",
        "dagster/kind/xgboost": "",
        "dagster/kind/snowflake": "",
    },
    description="Runs the opportunity win probability model and writes predictions to Snowflake.",
)
def opportunity_win_probability_predictions(_):
    """Runs the opportunity win probability model and writes predictions to Snowflake.
    Depends on the model_opportunity_win_probability dbt model.
    """
    result = predict_and_write_to_snowflake()
    return result


defs = Definitions(
    assets=[opportunity_win_probability_predictions],
)
