from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Optional, Union

import dagster as dg
import dagster.components as dg_components
import jinja2
from dagster._core.definitions.asset_spec import InternalFreshnessPolicy
from dagster.components.core.context import ComponentLoadContext
from dagster.components.resolved.core_models import ResolvedAssetAttributes
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_shared.record import record
from dagster_snowflake import SnowflakeResource
from pydantic import Field

log = dg.get_dagster_logger()


def _schema_from_env():
    return "elementl" if get_environment() == "PROD" else "dev"


@record
class SnowflakeEntity(dg_components.Resolvable):
    entity_name: str = Field(
        ...,
        description="The name of the entity to create or refresh. Must be a valid Snowflake entity name.",
    )
    template_variables: dict[str, str] = Field(
        ...,
        description="A dictionary of template variables to be used in the create, show, and refresh statements.",
    )
    asset_attributes: Optional[ResolvedAssetAttributes] = None


class StatementFile(dg_components.Model):
    file: str


ResolvedStatement = Annotated[
    Union[str, StatementFile],
    dg_components.Resolver(lambda ctx, stmt: stmt, model_field_type=Union[str, StatementFile]),  # type: ignore
]


@record
class SnowflakeCreateOrRefreshComponent(dg_components.Component, dg_components.Resolvable):
    """This component creates an asset which creates or refreshes entities in Snowflake (e.g. stages, tables etc).

    The component is configured with a template file which contains the SQL query to create or refresh the entity, alongside
    template strings to show or refresh the entity.
    """

    create_statement: ResolvedStatement = Field(
        ..., description="A SQL statement to create the entity"
    )
    show_statement: ResolvedStatement = Field(
        ..., description="A SQL SHOW statement to check if the entity exists"
    )
    refresh_statement: ResolvedStatement = Field(
        ..., description="A SQL statement to refresh the entity"
    )

    role: str = Field(..., description="The Snowflake role to use to create or refresh the entity")

    entities: list[SnowflakeEntity] = Field(
        ...,
        description="A list of Snowflake entities to create or refresh when the asset is materialized",
    )
    asset_attributes: Optional[ResolvedAssetAttributes] = Field(
        None, description="Asset attributes to apply to the created assets"
    )

    @classmethod
    def get_additional_scope(cls) -> dict[str, Any]:
        return {"schema_from_env": _schema_from_env}

    def _op_name(self, context: ComponentLoadContext) -> str:
        return "_".join(entity.entity_name.split(".")[-1] for entity in self.entities)

    def _populate_statement(
        self,
        context: ComponentLoadContext,
        statement: Union[str, StatementFile],
        template_variables: dict[str, str],
    ) -> str:
        statement_text = (
            statement
            if isinstance(statement, str)
            else (Path(context.path) / statement.file).read_text()
        )
        return jinja2.Template(statement_text).render(**template_variables)

    def spec_from_stage(self, stage: SnowflakeEntity) -> dg.AssetSpec:
        """Creates an AssetSpec representing a given SnowflakeEntity."""
        return dg.AssetSpec(
            **(self.asset_attributes or {}),
            **(stage.asset_attributes or {}),
            key=dg.AssetKey(stage.entity_name.split(".")),
            internal_freshness_policy=InternalFreshnessPolicy.time_window(
                fail_window=timedelta(hours=23)
            ),
        )

    @cached_property
    def stage_by_key(self) -> dict[dg.AssetKey, SnowflakeEntity]:
        """Cached mapping of AssetKey to SnowflakeEntity."""
        stage_by_key: dict[dg.AssetKey, SnowflakeEntity] = {}
        for stage in self.entities:
            stage_by_key[dg.AssetKey(stage.entity_name.split("."))] = stage

        return stage_by_key

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        load_context = context

        specs = [self.spec_from_stage(stage) for stage in self.entities]

        @dg.multi_asset(
            specs=specs,
            name=self._op_name(context),
            can_subset=True,
        )
        def _create_or_refresh_entities(
            context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
        ):
            yield from self.execute(context, load_context, snowflake_sf)

        return dg.Definitions(
            assets=[_create_or_refresh_entities],
        )

    def execute(
        self,
        context: dg.AssetExecutionContext,
        load_context: ComponentLoadContext,
        snowflake_sf: SnowflakeResource,
    ) -> Any:
        """Execute the create or refresh operation for the given asset keys."""
        with snowflake_sf.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"USE ROLE {self.role};")
            for key in context.selected_asset_keys:
                stage = self.stage_by_key[key]
                schema = ".".join(stage.entity_name.split(".")[:-1])

                cur.execute(f"USE SCHEMA {schema.upper()};")

                show_query = self._populate_statement(
                    load_context, self.show_statement, stage.template_variables
                )
                cur.execute(show_query)
                stages = cur.fetchall()

                if not stages:
                    create_query = self._populate_statement(
                        load_context, self.create_statement, stage.template_variables
                    )
                    cur.execute(create_query)
                    log.info(f"Created {'.'.join(key.path)}")
                    yield dg.MaterializeResult(asset_key=key)
                    continue

                refresh_query = self._populate_statement(
                    load_context, self.refresh_statement, stage.template_variables
                )
                cur.execute(refresh_query)

                log.info(f"Refreshed {'.'.join(key.path)}")
                yield dg.MaterializeResult(asset_key=key)
