"""Thinkific & Hubspot ingestion with `dlt`.

PREREQUISITES

    Ensure environment variables have been set:

        THINKIFIC_API_KEY
        THINKIFIC_SUBDOMAIN
        SOURCES__HUBSPOT__API_KEY
        DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
        DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
        DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
        DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
        DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
        DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

"""

from typing import Optional

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
)
from dagster._annotations import public
from dagster_open_platform.resources.dlt_resource import (
    DltDagsterResource,
    DltDagsterTranslator,
    dlt_assets,
)
from dlt import pipeline
from dlt.extract.resource import DltResource
from dlt_sources.hubspot import hubspot
from dlt_sources.thinkific import thinkific

dlt_resource = DltDagsterResource()


class ThinkificDltDagsterTranslator(DltDagsterTranslator):
    @classmethod
    @public
    def get_auto_materialize_policy(cls, resource: DltResource) -> Optional[AutoMaterializePolicy]:
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 1 * * *")
        )


@dlt_assets(
    dlt_source=thinkific(),
    dlt_pipeline=pipeline(
        pipeline_name="thinkific",
        dataset_name="thinkific",
        destination="snowflake",
    ),
    name="thinkific",
    group_name="education",
    dlt_dagster_translator=ThinkificDltDagsterTranslator(),
)
def thinkific_assets(context: AssetExecutionContext, dlt: DltDagsterResource):
    yield from dlt.run(context=context)


class HubspotDltDagsterTranslator(DltDagsterTranslator):
    @classmethod
    @public
    def get_auto_materialize_policy(cls, resource: DltResource) -> Optional[AutoMaterializePolicy]:
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 0 * * *")
        )


@dlt_assets(
    dlt_source=hubspot(include_history=True),
    dlt_pipeline=pipeline(
        pipeline_name="hubspot",
        dataset_name="hubspot",
        destination="snowflake",
    ),
    name="hubspot",
    group_name="hubspot",
    dlt_dagster_translator=HubspotDltDagsterTranslator(),
)
def hubspot_assets(context: AssetExecutionContext, dlt: DltDagsterResource):
    yield from dlt.run(context=context)
