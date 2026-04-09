import json
from collections.abc import Callable, Mapping
from functools import cached_property
from typing import Annotated, Any, TypeAlias

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
        model_field_type=str | dg_components.AssetAttributesModel,  # type: ignore
    ),
]


_DEFAULT_CRON = "0 0 * * *"


_FIVETRAN_RESCHEDULED_SYNC_STATE = "rescheduled"


class SkipOnRescheduleFivetranWorkspace(FivetranWorkspace):
    """FivetranWorkspace that skips asset materialization when a Fivetran sync is rescheduled.

    Checks the connector's sync_state before triggering a sync. If Fivetran has marked
    the connector as 'rescheduled' (e.g. due to quota limits), the run completes
    successfully without materializing the assets, avoiding an indefinite polling hang.
    """

    def _sync_and_poll(self, context: dg.AssetExecutionContext):
        connector_id = None
        for spec in context.assets_def.specs:
            md = spec.metadata.get("dagster-fivetran/connector_id")
            if md is not None:
                connector_id = md.value if hasattr(md, "value") else str(md)  # type: ignore[union-attr]
                break

        if connector_id:
            connector_details = self.get_client().get_connector_details(connector_id)
            sync_state = connector_details.get("status", {}).get("sync_state", "")
            if sync_state == _FIVETRAN_RESCHEDULED_SYNC_STATE:
                context.log.info(
                    f"Connector '{connector_id}' sync_state is '{_FIVETRAN_RESCHEDULED_SYNC_STATE}'. "
                    "Skipping sync and emitting materialization events to allow downstream assets to proceed."
                )
                for asset_key in context.selected_asset_keys:
                    yield dg.MaterializeResult(
                        asset_key=asset_key,
                        metadata={
                            "skipped_reason": dg.MetadataValue.text(
                                f"Fivetran connector '{connector_id}' was in '{_FIVETRAN_RESCHEDULED_SYNC_STATE}' state; no sync performed."
                            )
                        },
                    )
                return

        yield from super()._sync_and_poll(context)


class ProxyDagsterFivetranTranslator(DagsterFivetranTranslator):
    def __init__(self, fn: TranslationFn, connector_schedules: dict[str, str]):
        self.fn = fn
        self.connector_schedules = connector_schedules

    def get_asset_spec(self, props: FivetranConnectorTableProps) -> dg.AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        spec = self.fn(base_asset_spec, props)

        cron = self.connector_schedules.get(props.connector_id, _DEFAULT_CRON)
        return spec.replace_attributes(
            automation_condition=dg.AutomationCondition.cron_tick_passed(cron)
            & ~dg.AutomationCondition.in_progress(),
        )


class FivetranWorkspaceModel(Model):
    api_key: str
    api_secret: str
    account_id: str


class FivetranComponent(Component, Model, Resolvable):
    """Loads Fivetran connectors as Dagster assets."""

    workspace: Annotated[
        FivetranWorkspace,
        dg_components.Resolver(
            lambda context, model: SkipOnRescheduleFivetranWorkspace(
                **resolve_fields(model, FivetranWorkspaceModel, context)  # type: ignore
            )
        ),
    ]
    translation: ResolvedTranslationFn | None = None
    connection_setup_tests_schedule: str | None = None
    excluded_connector_ids: list[str] | None = None
    connector_schedules: dict[str, str] | None = None

    @cached_property
    def translator(self) -> DagsterFivetranTranslator:
        schedules = self.connector_schedules or {}
        fn = (
            self.translation if self.translation else lambda base_asset_spec, props: base_asset_spec
        )
        return ProxyDagsterFivetranTranslator(fn, schedules)

    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {
            "group_from_db_and_schema": lambda props: (
                f"fivetran_{'_'.join(props.table.split('.')[:-1])}"
            ),
        }

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        excluded = set(self.excluded_connector_ids or [])
        fivetran_assets = build_fivetran_assets_definitions(
            workspace=self.workspace,
            dagster_fivetran_translator=self.translator,
            connector_selector_fn=(
                (lambda connector: connector.id not in excluded) if excluded else None
            ),
        )

        schedules = []
        if self.connection_setup_tests_schedule:
            schedules.append(_connection_setup_tests_schedule(self.connection_setup_tests_schedule))

        return dg.Definitions(
            assets=fivetran_assets,
            resources={"fivetran": self.workspace},
            schedules=schedules,
            sensors=[
                dg.AutomationConditionSensorDefinition(
                    name="fivetran_automation_sensor",
                    target=dg.AssetSelection.key_prefixes(["fivetran"]),
                )
            ],
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
