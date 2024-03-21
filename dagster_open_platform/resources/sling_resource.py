import contextlib
import enum
import os
import re
import tempfile
from subprocess import PIPE, STDOUT, Popen
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Union

import sling
import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    ConfigurableResource,
    asset,
    get_dagster_logger,
)
from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.test_utils import environ
from pydantic import create_model

logger = get_dagster_logger()

# NOTE: Temporarily keeping Ben's Sling resource implementation until embedded-elt's API settles in a bit more.


class SlingPostgresConfig(Config):
    host: str
    user: str
    password: str
    ssh_tunnel: Optional[str]
    database: str
    ssh_private_key: Optional[str]


class SlingSnowflakeConfig(Config):
    host: str
    user: str
    password: str
    database: str
    role: str
    warehouse: str


class SlingMode(enum.Enum):
    FULL_REFRESH = "full-refresh"
    TRUNCATE = "truncate"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"


class CustomSlingResource(ConfigurableResource):
    postgres_config: SlingPostgresConfig
    snowflake_config: SlingSnowflakeConfig

    def _get_env_config(self) -> Dict[str, Any]:
        return {
            "connections": {
                "postgres": {
                    "type": "postgres",
                    "user": self.postgres_config.user,
                    "password": self.postgres_config.password,
                    "host": self.postgres_config.host,
                    "sslmode": "require",
                    "database": self.postgres_config.database,
                    **(
                        {
                            "ssh_tunnel": self.postgres_config.ssh_tunnel,
                            "ssh_private_key": self.postgres_config.ssh_private_key,
                        }
                        if self.postgres_config.ssh_tunnel
                        else {}
                    ),
                },
                "snowflake": {
                    "type": "snowflake",
                    "user": self.snowflake_config.user,
                    "password": self.snowflake_config.password,
                    "host": self.snowflake_config.host,
                    "database": self.snowflake_config.database,
                    "role": self.snowflake_config.role,
                    "warehouse": self.snowflake_config.warehouse,
                },
            }
        }

    @contextlib.contextmanager
    def _setup_config(self) -> Generator[None, None, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(os.path.join(temp_dir, "env.yaml"), "w") as f:
                f.write(yaml.dump(self._get_env_config()))
            with environ({"SLING_HOME_DIR": temp_dir}):
                yield

    def exec_sling_cmd(
        self, cmd, stdin=None, stdout=PIPE, stderr=STDOUT
    ) -> Generator[str, None, None]:
        # copied from
        # https://github.com/slingdata-io/sling-python/blob/main/sling/__init__.py#L251
        # fixes issue w/ trying to gather stderr when stderr=STDOUT
        with Popen(cmd, shell=True, stdin=stdin, stdout=stdout, stderr=stderr) as proc:
            assert proc.stdout

            for line in proc.stdout:
                fmt_line = str(line.strip(), "utf-8")
                yield fmt_line

            proc.wait()
            if proc.returncode != 0:
                raise Exception("Sling command failed with error code %s", proc.returncode)

    def _sync(
        self,
        source_conn: str,
        target_conn: str,
        source_table: str,
        dest_table: str,
        mode: SlingMode = SlingMode.FULL_REFRESH,
        primary_key: Optional[List[str]] = None,
        update_key: Optional[str] = None,
        allow_alter_table: bool = True,
    ) -> Generator[str, None, None]:
        """Runs a Sling sync from the given source table to the given destination table. Generates
        output lines from the Sling CLI.
        """
        with self._setup_config():
            config = {
                "source": {
                    "conn": source_conn,
                    "stream": source_table,
                    **(
                        {
                            "primary_key": primary_key,
                        }
                        if primary_key
                        else {}
                    ),
                    **(
                        {
                            "update_key": update_key,
                        }
                        if update_key
                        else {}
                    ),
                    "options": {
                        "empty_as_null": False,
                    },
                },
                "target": {
                    "conn": target_conn,
                    "object": dest_table,
                    **(
                        {
                            "options": {
                                "add_new_columns": False,
                                "adjust_column_type": False,
                            }
                        }
                        if not allow_alter_table
                        else {}
                    ),
                },
                "mode": mode.value,
            }

            sling_cli = sling.Sling(**config)

            logger.info("Starting Sling sync with mode: %s", mode)
            cmd = sling_cli._prep_cmd()  # noqa: SLF001

            yield from self.exec_sling_cmd(cmd)

    def sync_postgres_to_snowflake(
        self,
        source_table: str,
        dest_table: str,
        mode: SlingMode = SlingMode.FULL_REFRESH,
        primary_key: Optional[List[str]] = None,
        update_key: Optional[str] = None,
    ) -> Generator[str, None, None]:
        """Runs a Sling sync from the given Postgres table to the given Snowflake table. Generates
        output lines from the Sling CLI.
        """
        yield from self._sync(
            source_conn="postgres",
            target_conn="snowflake",
            source_table=source_table,
            dest_table=dest_table,
            mode=mode,
            primary_key=primary_key,
            update_key=update_key,
        )

    def sync_snowflake_to_postgres(
        self,
        source_table: str,
        dest_table: str,
        mode: SlingMode = SlingMode.FULL_REFRESH,
        primary_key: Optional[List[str]] = None,
        update_key: Optional[str] = None,
        allow_alter_table: bool = True,
    ) -> Generator[str, None, None]:
        """Runs a Sling sync from the given Snowflake table to the given Postgres table. Generates
        output lines from the Sling CLI.
        """
        yield from self._sync(
            source_conn="snowflake",
            target_conn="postgres",
            source_table=source_table,
            dest_table=dest_table,
            mode=mode,
            primary_key=primary_key,
            update_key=update_key,
            allow_alter_table=allow_alter_table,
        )


def build_sync_postgres_to_snowflake_asset(
    key: AssetKey,
    sling_resource_key: str,
    source_table: str,
    dest_table: str,
    mode=SlingMode.FULL_REFRESH,
    primary_key: Optional[Union[str, List[str]]] = None,
    update_key: Optional[str] = None,
    deps: Optional[Iterable[CoercibleToAssetKey]] = None,
) -> AssetsDefinition:
    """Factory which builds an asset definition that syncs the given source table to the given
    destination table.
    """
    if primary_key is not None and not isinstance(primary_key, list):
        primary_key = [primary_key]

    @asset(
        required_resource_keys={sling_resource_key},
        key=key,
        deps=deps,
        group_name="postgres_mirror",
    )
    def sync(context: AssetExecutionContext) -> None:
        sling: CustomSlingResource = getattr(context.resources, sling_resource_key)
        for stdout_line in sling.sync_postgres_to_snowflake(
            source_table=source_table,
            dest_table=dest_table,
            primary_key=primary_key,
            update_key=update_key,
            mode=mode,
        ):
            print(stdout_line)
            cleaned_line = re.sub(r"\[[0-9;]+[a-zA-Z]", " ", stdout_line)
            trimmed_line = cleaned_line[cleaned_line.find("INF") + 6 :].strip()
            match = re.search(r"(\d+) rows", trimmed_line)
            if match:
                context.add_output_metadata({"row_count": int(match.group(1))})
            context.log.info(trimmed_line)

    return sync


def build_sync_snowflake_to_postgres_asset(
    key: AssetKey,
    sling_resource_key: str,
    source_table: str,
    dest_table: str,
    mode=SlingMode.FULL_REFRESH,
    primary_key: Optional[Union[str, List[str]]] = None,
    update_key: Optional[str] = None,
    deps: Optional[Iterable[CoercibleToAssetKey]] = None,
    preflight_check: Optional[Callable[[AssetExecutionContext], None]] = None,
    preflight_resource_keys: Optional[Set[str]] = None,
    group_name: Optional[str] = None,
    allow_alter_table: bool = True,
) -> AssetsDefinition:
    """Factory which builds an asset definition that syncs the given source table to the given
    destination table.
    """
    if primary_key is not None and not isinstance(primary_key, list):
        primary_key = [primary_key]

    ConfigClass = create_model(
        "SyncConfig",
        __base__=Config,
        source=(str, source_table),
        dest=(str, dest_table),
        update_key=(Optional[str], update_key),
        primary_key=(Optional[List[str]], primary_key),
        mode=(SlingMode, mode.value.upper()),
    )

    @asset(
        required_resource_keys={sling_resource_key} | (preflight_resource_keys or set()),
        key=key,
        deps=deps,
        compute_kind="sling",
        group_name=group_name,
    )
    def sync(context: AssetExecutionContext, config: ConfigClass) -> None:  # type: ignore
        if preflight_check:
            preflight_check(context)

        context.log.info(
            f"Syncing {config.source} to {config.dest} using primary key {config.primary_key} and"
            f" update key {config.update_key} using mode {config.mode}"
        )

        sling: CustomSlingResource = getattr(context.resources, sling_resource_key)
        for stdout_line in sling.sync_snowflake_to_postgres(
            source_table=config.source,
            dest_table=config.dest,
            primary_key=config.primary_key,
            update_key=config.update_key,
            mode=config.mode,
            allow_alter_table=allow_alter_table,
        ):
            print(stdout_line)
            cleaned_line = re.sub(r"\[[0-9;]+[a-zA-Z]", " ", stdout_line)
            trimmed_line = cleaned_line[cleaned_line.find("INF") + 6 :].strip()
            match = re.search(r"(\d+) rows", trimmed_line)
            if match:
                context.add_output_metadata({"row_count": int(match.group(1))})
            context.log.info(trimmed_line)

    return sync
