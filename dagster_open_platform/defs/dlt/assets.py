import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    SourceAsset,
    file_relative_path,
)
from dagster._annotations import public
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_open_platform.defs.dlt.sources.buildkite import buildkite_source_v2, pipelines
from dagster_open_platform.defs.dlt.sources.github import github_reactions
from dagster_open_platform.defs.dlt.sources.hubspot import hubspot
from dagster_open_platform.defs.dlt.sources.thinkific import thinkific
from dlt import pipeline
from dlt.extract.resource import DltResource


class ThinkificDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_automation_condition(self, resource):
        return (
            AutomationCondition.cron_tick_passed("0 0 * * *") & ~AutomationCondition.in_progress()
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
    dagster_dlt_translator=ThinkificDagsterDltTranslator(),
)
def thinkific_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


thinkific_source_assets = [
    SourceAsset(key, group_name="thinkific") for key in thinkific_assets.dependency_keys
]


class HubspotDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_automation_condition(self, resource):
        return (
            AutomationCondition.cron_tick_passed("0 0 * * *") & ~AutomationCondition.in_progress()
        )

    @public
    def get_asset_key(self, resource: DltResource) -> AssetKey:
        return AssetKey(["dlt", resource.source_name, resource.name])


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
    dagster_dlt_translator=HubspotDagsterDltTranslator(),
)
def hubspot_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


hubspot_source_assets = [
    SourceAsset(key, group_name="hubspot") for key in hubspot_assets.dependency_keys
]

dlt_configuration_path = file_relative_path(__file__, "./sources/configuration.yaml")
dlt_configuration = yaml.safe_load(open(dlt_configuration_path))


class GithubDagsterDltTranslator(DagsterDltTranslator):
    @public
    def get_automation_condition(self, resource):
        return (
            AutomationCondition.cron_tick_passed("0 0 * * *") & ~AutomationCondition.in_progress()
        )


@dlt_assets(
    dlt_source=github_reactions(
        dlt_configuration["sources"]["github"]["repositories"],
        items_per_page=100,
        max_items=500,
    ).with_resources("issues", "stargazers"),
    dlt_pipeline=pipeline(
        pipeline_name="github_issues",
        dataset_name="github",
        destination="snowflake",
        progress="log",
    ),
    name="github",
    group_name="github",
    dagster_dlt_translator=GithubDagsterDltTranslator(),
)
def github_reactions_dagster_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


github_source_assets = [
    SourceAsset(key, group_name="github") for key in github_reactions_dagster_assets.dependency_keys
]


class BuildkiteDltTranslator(DagsterDltTranslator):
    @public
    def get_automation_condition(self, resource):
        return (
            AutomationCondition.cron_tick_passed("0 0 * * *") & ~AutomationCondition.in_progress()
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
    dagster_dlt_translator=BuildkiteDltTranslator(),
)
def buildkite_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=buildkite_source_v2(
        org_slug="dagster",
    ),
    dlt_pipeline=pipeline(
        pipeline_name="buildkite_pipelines",
        dataset_name="buildkite_v2",
        destination="snowflake",
        progress="log",
    ),
    name="buildkite_v2",
    group_name="buildkite_v2",
    dagster_dlt_translator=BuildkiteDltTranslator(),
)
def buildkite_assets_v2(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


buildkite_source_assets = [
    SourceAsset(key, group_name="buildkite") for key in buildkite_assets.dependency_keys
] + [SourceAsset(key, group_name="buildkite_v2") for key in buildkite_assets_v2.dependency_keys]
