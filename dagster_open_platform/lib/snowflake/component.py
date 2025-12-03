import os
from pathlib import Path
from typing import Annotated, Any, Optional, Union

import dagster as dg
import dagster.components as dg_components
import pydantic
from dagster.components.core.context import ComponentLoadContext
from dagster.components.resolved.core_models import ResolvedAssetAttributes
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource
from pydantic import Field

log = dg.get_dagster_logger()

# Paths to SQL templates (relative to the YAML location)
OBJECT_CREATE_SQL = os.path.join(os.path.dirname(__file__), "sql", "create_object.sql")


def _database_from_env():
    if get_environment() == "LOCAL":
        return "sandbox"
    else:
        return "aws"


def _schema_from_env():
    if get_environment() == "PROD":
        return "elementl"
    else:
        return "dev"


def _role_from_env():
    if get_environment() == "LOCAL":
        return "SANDBOX_WRITER"
    else:
        return "AWS_WRITER"


class StatementFile(dg_components.Model):
    file: str


ResolvedStatement = Annotated[
    Union[str, StatementFile],
    dg_components.Resolver(lambda ctx, stmt: stmt, model_field_type=Union[str, StatementFile]),  # type: ignore
]


class SnowflakeObjectOptions(pydantic.BaseModel, dg_components.Resolvable):
    location: Annotated[Optional[str], Field(default=None, description="Location of the object")]
    url: Annotated[Optional[str], Field(default=None, description="URL of the object")]
    storage_integration: Annotated[
        Optional[str], Field(default=None, description="Storage integration of the object")
    ]
    directory_enabled: Annotated[
        Optional[bool], Field(default=None, description="Directory enabled of the object")
    ]
    file_format: Annotated[dict[str, Any], Field(..., description="File format of the object")]
    directory: Annotated[
        Optional[dict[str, Any]], Field(default=None, description="Directory of the object")
    ]
    auto_refresh: Annotated[
        Optional[bool], Field(default=None, description="Auto refresh of the object")
    ]
    comment: Annotated[Optional[str], Field(default=None, description="Comment of the object")]
    pattern: Annotated[Optional[str], Field(default=None, description="Pattern of the object")]
    refresh_on_create: Annotated[
        Optional[bool], Field(default=None, description="Refresh on create of the object")
    ]

    # add dictionary .items() function, return all attributes
    def items(self):
        return self.model_dump().items()


class SnowflakeCreateOrRefreshComponent(
    pydantic.BaseModel, dg_components.Component, dg_components.Resolvable
):
    name: Annotated[str, Field(..., description="Name of the stage")]
    database_name: Annotated[str, Field(..., description="Database of the stage")]
    schema_name: Annotated[str, Field(..., description="Schema of the stage")]
    type: Annotated[str, Field(..., description="Type of the object")]
    role: Annotated[str, Field(..., description="Role of the object")]
    columns: Annotated[
        Optional[list[dict[str, Any]]], Field(default=None, description="Columns of the object")
    ]
    partition_by: Annotated[
        Optional[str], Field(default=None, description="Partition by of the object")
    ]
    asset_attributes: Annotated[
        Optional[ResolvedAssetAttributes],
        Field(default=None, description="Asset attributes to apply to the created assets"),
    ]
    options: Annotated[
        Optional[SnowflakeObjectOptions],
        Field(
            default=None,
            description="Configuration options for the Snowflake object being created or refreshed",
        ),
    ]

    @property
    def snowflake_object_name(self) -> str:
        return {
            "stage": "STAGE",
            "external table": "EXTERNAL TABLE",
        }[self.type]

    @property
    def _show_query(self) -> str:
        return f"SHOW {self.snowflake_object_name}S LIKE '{self.name}';"

    @property
    def _refresh_query(self) -> str:
        return f"ALTER {self.snowflake_object_name} {self.name} REFRESH;"

    @classmethod
    def get_additional_scope(cls) -> dict[str, Any]:
        return {
            "schema_from_env": _schema_from_env,
            "database_from_env": _database_from_env,
            "role_from_env": _role_from_env,
        }

    def _op_name(self, context: ComponentLoadContext) -> str:
        return self.name

    def spec_from_stage(self) -> dg.AssetSpec:
        """Creates an AssetSpec representing a given SnowflakeEntity."""
        return dg.AssetSpec(
            **(self.asset_attributes or {}),
            key=dg.AssetKey([self.database_name, self.schema_name, self.name]),
        )

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        load_context = context

        @dg.asset(
            **(self.asset_attributes or {}),
            key=dg.AssetKey([self.database_name, self.schema_name, self.name]),
        )
        def _create_or_refresh_object(
            context: dg.AssetExecutionContext, snowflake: SnowflakeResource
        ):
            self.execute(context, load_context, snowflake)

        # Explicitly cast to AssetsDefinition for type checking
        asset_def: dg.AssetsDefinition = _create_or_refresh_object  # type: ignore
        return dg.Definitions(assets=[asset_def])

    def execute(
        self,
        context: dg.AssetExecutionContext,
        load_context: ComponentLoadContext,
        snowflake: SnowflakeResource,
    ) -> None:
        with snowflake.get_connection() as conn:
            cur = conn.cursor()

            log.info(f"Using role {self.role}")
            cur.execute(f"USE ROLE {self.role};")
            log.info(f"Using database {self.database_name}")
            cur.execute(f"USE DATABASE {self.database_name.upper()};")
            log.info(f"Using schema {self.database_name.upper()}.{self.schema_name.upper()}")
            cur.execute(f"USE SCHEMA {self.database_name.upper()}.{self.schema_name.upper()};")

            cur.execute(self._show_query)
            exists = cur.fetchall()
            template_variables = {
                "name": self.name,
                "type": self.type,
                "schema_name": self.schema_name,
                "database_name": self.database_name,
                "role": self.role,
                "columns": self.columns,
                "partition_by": self.partition_by,
                "options": self.options,
            }
            if not exists:
                create_query = self._populate_statement(
                    load_context,
                    statement_file=OBJECT_CREATE_SQL,
                    template_variables=template_variables,
                )
                log.info("Executing create query")
                log.info(create_query)
                cur.execute(create_query)
                log.info(f"Created {self.name}")
            refresh_query = self._populate_statement(
                load_context,
                statement_text=self._refresh_query,
                template_variables=template_variables,
            )
            log.info("Executing refresh query")
            log.info(refresh_query)
            cur.execute(refresh_query)
            log.info(f"Refreshed {self.name}")

    def _populate_statement(
        self,
        context: ComponentLoadContext,
        template_variables: dict[str, str],
        statement_text: str | None = None,
        statement_file: Optional[str] = None,
    ) -> str:
        query_text = (
            statement_text
            if statement_text is not None
            else Path(statement_file).read_text()
            if statement_file is not None
            else ""
        )
        import jinja2

        sql_text = jinja2.Template(query_text).render(**template_variables)
        log.info(sql_text)
        return sql_text
