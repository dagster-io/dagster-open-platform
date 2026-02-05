import json
from collections.abc import Callable, Mapping
from functools import cached_property
from typing import Annotated, Any, Optional, TypeAlias, Union

import dagster as dg
import dagster.components as dg_components
from dagster.components import Component, ComponentLoadContext, Model, Resolvable
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils import TranslatorResolvingInfo
from dagster_fivetran import (
    DagsterFivetranTranslator,
    FivetranConnectorTableProps,
    FivetranWorkspace,
    build_fivetran_assets_definitions,
)


def resolve_translation(context: dg_components.ResolutionContext, model):
    info = TranslatorResolvingInfo(
        asset_attributes=model,
        resolution_context=context,
        model_key="translation",
    )
    return lambda base_asset_spec, props: info.get_asset_spec(
        base_asset_spec,
        {
            "props": props,
            "spec": base_asset_spec,
        },
    )


TranslationFn: TypeAlias = Callable[[dg.AssetSpec, FivetranConnectorTableProps], dg.AssetSpec]
ResolvedTranslationFn: TypeAlias = Annotated[
    TranslationFn,
    dg_components.Resolver(
        resolve_translation,
        model_field_type=Union[str, dg_components.AssetAttributesModel],  # type: ignore
    ),
]


class ProxyDagsterFivetranTranslator(DagsterFivetranTranslator):
    def __init__(self, fn: TranslationFn):
        self.fn = fn

    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        spec = self.fn(base_asset_spec, props)

        return spec


class FivetranWorkspaceModel(Model):
    api_key: str
    api_secret: str
    account_id: str


class FivetranComponent(Component, Model, Resolvable):
    """Loads Fivetran connectors as Dagster assets."""

    workspace: Annotated[
        FivetranWorkspace,
        dg_components.Resolver(
            lambda context, model: FivetranWorkspace(
                **resolve_fields(model, FivetranWorkspaceModel, context)  # type: ignore
            )
        ),
    ]
    translation: Optional[ResolvedTranslationFn] = None
    connection_setup_tests_schedule: Optional[str] = None

    @cached_property
    def translator(self) -> DagsterFivetranTranslator:
        if self.translation:
            return ProxyDagsterFivetranTranslator(self.translation)
        return DagsterFivetranTranslator()

    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {
            "hourly_if_not_in_progress": dg.AutomationCondition.cron_tick_passed("0 * * * *")
            & ~dg.AutomationCondition.in_progress(),
            "group_from_db_and_schema": lambda props: (
                f"fivetran_{'_'.join(props.table.split('.')[:-1])}"
            ),
        }

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        fivetran_assets = build_fivetran_assets_definitions(
            workspace=self.workspace,
            dagster_fivetran_translator=self.translator,
        )

        schedules = []
        if self.connection_setup_tests_schedule:
            schedules.append(_connection_setup_tests_schedule(self.connection_setup_tests_schedule))

        return dg.Definitions(
            assets=fivetran_assets,
            resources={"fivetran": self.workspace},
            schedules=schedules,
        )


def _connection_setup_tests_schedule(
    schedule_cron_expression: str,
) -> dg.ScheduleDefinition:
    @dg.op
    def run_connection_setup_tests(context: dg.OpExecutionContext, fivetran: FivetranWorkspace):
        _execute_connection_setup_tests(context, fivetran)

    @dg.job
    def fivetran_connection_setup_tests():
        run_connection_setup_tests()

    return dg.ScheduleDefinition(
        job=fivetran_connection_setup_tests,
        cron_schedule=schedule_cron_expression,
    )


def _execute_connection_setup_tests(
    context: dg.OpExecutionContext,
    workspace: FivetranWorkspace,
):
    data = workspace.get_or_fetch_workspace_data()
    client = workspace.get_client()
    body = json.dumps(
        {"trust_certificates": True, "trust_fingerprints": True},
    )
    for connector_id, connector in data.connectors_by_id.items():
        response = client._make_request(  # noqa: SLF001
            method="POST",
            endpoint=f"connections/{connector_id}/test",
            data=body,
        )
        response.raise_for_status()
        test_result = response.json()
        if setup_tests := test_result.get("data", {}).get("setup_tests"):
            for test in setup_tests:
                test_name = test.get("title", "Unknown test")
                test_status = test.get("status", "Unknown")

                if test_status in ("FAILED", "JOB_FAILED"):
                    details = test["details"] if test.get("details") else ""
                    raise dg.Failure(
                        f"Connection {connector.name} ({connector_id}) setup test '{test_name}' {test_status}: {test['message']} {details}"
                    )

                context.log.info(
                    f"Connection {connector.name} ({connector_id}) setup test '{test_name}': {test_status}"
                )
