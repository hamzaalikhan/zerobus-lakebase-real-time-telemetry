import os
import logging
import psycopg
from psycopg_pool import ConnectionPool
from server.config import get_workspace_client

logger = logging.getLogger(__name__)

DB_SCHEMA = os.environ.get("DB_SCHEMA", "ecommerce")

w = get_workspace_client()

# Connection details come entirely from the environment. When the app is bound
# to its Lakebase project as a resource (see resources/datacart_storefront.app.yml),
# the Apps platform injects PGHOST/PGPORT/PGUSER/PGDATABASE/PGSSLMODE at runtime
# and auto-creates the service principal's Postgres login role. ENDPOINT_NAME is
# supplied via the app's config env (the binding does not inject it) and is used
# to mint short-lived OAuth DB tokens — Lakebase credentials expire hourly, so
# there is no static password to inject.
ENDPOINT_NAME = os.environ["ENDPOINT_NAME"]
LAKEBASE_PROJECT = ENDPOINT_NAME.split("/")[1]  # projects/<id>/branches/.../endpoints/...

username = os.environ["PGUSER"]
host = os.environ["PGHOST"]
port = os.environ.get("PGPORT", "5432")
database = os.environ.get("PGDATABASE", "databricks_postgres")
sslmode = os.environ.get("PGSSLMODE", "require")


class OAuthConnection(psycopg.Connection):
    """psycopg connection that mints a fresh OAuth DB token as its password.

    Lakebase DB credentials are short-lived (they expire ~hourly), so the token
    is generated per new connection rather than injected once at deploy time.
    """

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        logger.info(f"Generating DB credential for endpoint: {ENDPOINT_NAME}")
        try:
            credential = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
            logger.info(f"Credential generated, expires: {credential.expire_time}")
            kwargs["password"] = credential.token
        except Exception as e:
            logger.error(f"Failed to generate DB credential: {e}")
            raise
        try:
            conn = super().connect(conninfo, **kwargs)
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise


pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} port={port} sslmode={sslmode} connect_timeout=15",
    connection_class=OAuthConnection,
    min_size=1,
    max_size=10,
    max_lifetime=2700,
    open=False,
)


def get_branch_connection(branch_id: str) -> psycopg.Connection:
    """Get a direct connection to a specific branch endpoint (used by bonus labs)."""
    branch_full = f"projects/{LAKEBASE_PROJECT}/branches/{branch_id}"
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    if not endpoints:
        raise Exception(f"No endpoint found for branch '{branch_id}'")
    branch_host = endpoints[0].status.hosts.host
    endpoint_name = endpoints[0].name
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return psycopg.connect(
        dbname=database,
        user=username,
        password=cred.token,
        host=branch_host,
        port=int(port),
        sslmode=sslmode,
        autocommit=True,
        connect_timeout=15,
    )
