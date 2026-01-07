from contextlib import contextmanager

import dagster as dg
import psycopg2
from dagster.components import definitions


class PostgresResource(dg.ConfigurableResource):
    """Resource for connecting to Postgres database."""

    host: str
    user: str
    database: str
    password: str
    port: int = 5432
    sslmode: str = "require"

    @contextmanager
    def get_connection(self):
        """Get a Postgres connection."""
        conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            database=self.database,
            password=self.password,
            port=self.port,
            sslmode=self.sslmode,
        )
        try:
            yield conn
        finally:
            conn.close()


postgres = PostgresResource(
    host=dg.EnvVar("CLOUD_PROD_REPLICA_POSTGRES_TAILSCALE_HOST"),
    user=dg.EnvVar("CLOUD_PROD_POSTGRES_USER"),
    database="dagster",
    password=dg.EnvVar("CLOUD_PROD_POSTGRES_PASSWORD"),
    sslmode="require",
)


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={"postgres": postgres},
    )
