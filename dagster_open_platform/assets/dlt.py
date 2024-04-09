"""Thinkific & Hubspot ingestion with `dlt`.

The `dlt` pipeline and source secrets can be extracted "auto-magically" from
environment variables, as defined below:

    THINKIFIC_API_KEY
    THINKIFIC_SUBDOMAIN
    SOURCES__HUBSPOT__API_KEY
    DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE
    DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD
    DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME
    DESTINATION__SNOWFLAKE__CREDENTIALS__HOST
    DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE
    DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE

see: https://dlthub.com/docs/tutorial/grouping-resources#handle-secrets

Or passed to the pipeline or source itself using the function parameters.

"""

from typing import Optional

import yaml
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    file_relative_path,
)
from dagster._annotations import public
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    dlt_assets,
)
from dlt import pipeline
from dlt.extract.resource import DltResource
from dlt_sources.github import github_reactions
from dlt_sources.hubspot import hubspot
from dlt_sources.thinkific import thinkific

dlt_resource = DagsterDltResource()


class ThinkificDagsterDltTranslator(DagsterDltTranslator):
    def get_auto_materialize_policy(self, resource: DltResource) -> Optional[AutoMaterializePolicy]:
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
    dlt_dagster_translator=ThinkificDagsterDltTranslator(),
)
def thinkific_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


class HubspotDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_auto_materialize_policy(self, resource: DltResource) -> Optional[AutoMaterializePolicy]:
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
    dlt_dagster_translator=HubspotDagsterDltTranslator(),
)
def hubspot_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


dlt_configuration_path = file_relative_path(__file__, "../../dlt_sources/dlt_configuration.yaml")
dlt_configuration = yaml.safe_load(open(dlt_configuration_path))


# NOTE: currently have `max_items` set to prevent excessive API usage
@dlt_assets(
    dlt_source=github_reactions(
        dlt_configuration["sources"]["github"]["repositories"],
        items_per_page=100,
        max_items=250,
    ).with_resources("issues"),
    dlt_pipeline=pipeline(
        pipeline_name="github_issues",
        dataset_name="github",
        destination="snowflake",
    ),
    name="github",
    group_name="github",
)
def github_reactions_dagster_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
