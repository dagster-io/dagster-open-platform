from dagster import AssetExecutionContext, asset


@asset
def health_check_asset(context: AssetExecutionContext) -> None:
    """A small asset to verify that Dagster is running."""
    context.log.info("Your materialization is working.")
