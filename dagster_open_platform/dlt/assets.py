from typing import Optional

import yaml
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    SourceAsset,
    file_relative_path,
)
from dagster._annotations import public
from dagster_embedded_elt.dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dlt_sources.buildkite import pipelines
from dlt_sources.github import github_reactions
from dlt_sources.hubspot import hubspot
from dlt_sources.thinkific import thinkific

from dlt import pipeline
from dlt.extract.resource import DltResource


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
        progress="log",
    ),
    name="thinkific",
    group_name="thinkific",
    dlt_dagster_translator=ThinkificDagsterDltTranslator(),
)
def thinkific_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


thinkific_source_assets = [
    SourceAsset(key, group_name="thinkific") for key in thinkific_assets.dependency_keys
]


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
        progress="log",
    ),
    name="hubspot",
    group_name="hubspot",
    dlt_dagster_translator=HubspotDagsterDltTranslator(),
)
def hubspot_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


hubspot_source_assets = [
    SourceAsset(key, group_name="hubspot") for key in hubspot_assets.dependency_keys
]

dlt_configuration_path = file_relative_path(__file__, "../../dlt_sources/dlt_configuration.yaml")
dlt_configuration = yaml.safe_load(open(dlt_configuration_path))


class GithubDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_auto_materialize_policy(self, resource: DltResource) -> Optional[AutoMaterializePolicy]:
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 0 * * *")
        )


@dlt_assets(
    dlt_source=github_reactions(
        dlt_configuration["sources"]["github"]["repositories"],
        items_per_page=100,
        max_items=500,
    ).with_resources("issues"),
    dlt_pipeline=pipeline(
        pipeline_name="github_issues",
        dataset_name="github",
        destination="snowflake",
        progress="log",
    ),
    name="github",
    group_name="github",
    dlt_dagster_translator=GithubDagsterDltTranslator(),
)
def github_reactions_dagster_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


github_source_assets = [
    SourceAsset(key, group_name="github") for key in github_reactions_dagster_assets.dependency_keys
]


class BuildkiteDltTranslator(DagsterDltTranslator):
    @public
    def get_auto_materialize_policy(self, resource: DltResource) -> Optional[AutoMaterializePolicy]:
        return AutoMaterializePolicy.eager().with_rules(
            AutoMaterializeRule.materialize_on_cron("0 0 * * *")
        )


@dlt_assets(
    dlt_source=pipelines(
        org_slug="dagster",
        pipeline_slugs=["internal", "dagster"],
    ),
    dlt_pipeline=pipeline(
        pipeline_name="buildkite_pipelines_internal",
        dataset_name="buildkite",
        destination="snowflake",
        progress="log",
    ),
    name="buildkite",
    group_name="buildkite",
    dlt_dagster_translator=BuildkiteDltTranslator(),
)
def buildkite_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


buildkite_source_assets = [
    SourceAsset(key, group_name="buildkite") for key in buildkite_assets.dependency_keys
]
