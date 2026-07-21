import os
import logging
import psycopg
from psycopg_pool import ConnectionPool
from server.config import get_workspace_client

logger = logging.getLogger(__name__)

DB_SCHEMA = os.environ.get("DB_SCHEMA", "ecommerce")

w = get_workspace_client()


def _discover_workshop_project() -> tuple[str, str]:
    """Find the workshop's Lakebase project + production endpoint via the SDK.

    Strategy:
      1. If LAKEBASE_PROJECT and ENDPOINT_NAME env vars are set, use them.
      2. Otherwise, look up *this* app's creator (the deployer), find their numeric
         user ID, and resolve the project name `lakebase-workshop-<creator-id>`.
         This is deterministic per deployer — even if multiple workshop projects
         exist in the same workspace, each app finds *its own* project.
      3. Fallback: list accessible projects and pick the lone `lakebase-workshop-*`
         entry (works only if the SP has access to exactly one workshop project).

    Returns (project_id, endpoint_name).
    """
    project_id = os.environ.get("LAKEBASE_PROJECT", "").strip()
    endpoint_name = os.environ.get("ENDPOINT_NAME", "").strip()

    # Drop any unsubstituted DAB placeholders that uploaded as literal strings.
    if "${" in project_id:
        project_id = ""
    if "${" in endpoint_name:
        endpoint_name = ""

    if project_id and endpoint_name:
        logger.info(f"Using project from env: {project_id}")
        return project_id, _resolve_endpoint(project_id, endpoint_name)

    # --- Strategy 2: derive from the app's creator -----------------------
    project_id = _project_id_from_app_creator()
    if project_id:
        endpoint_name = _resolve_endpoint(project_id, "")
        logger.info(f"Resolved project from app creator: {project_id}")
        return project_id, endpoint_name

    # --- Strategy 3: fallback to first workshop project ------------------
    projects = list(w.postgres.list_projects())
    workshop_projects = [
        p for p in projects
        if (p.name or "").startswith("projects/lakebase-workshop-")
    ]
    if not workshop_projects:
        raise RuntimeError(
            "No Lakebase project named 'lakebase-workshop-*' is accessible to this app's "
            "service principal. Run Lab 2.1 to grant the SP project-level access."
        )
    if len(workshop_projects) > 1:
        names = ", ".join(p.name for p in workshop_projects)
        logger.warning(
            f"Multiple workshop projects accessible — falling back to first: {names}"
        )
    project = workshop_projects[0]
    project_id = project.name.split("/", 1)[1]
    endpoint_name = _resolve_endpoint(project_id, "")
    logger.info(f"Discovered (fallback) project={project_id} endpoint={endpoint_name}")
    return project_id, endpoint_name


def _project_id_from_app_creator() -> str:
    """Use the app's `creator` field to derive the deployer's project_id.

    The Apps platform sets `DATABRICKS_APP_NAME` on every running app. We look up
    the app's metadata, read `creator` (the deployer's email/userName), then look
    up the deployer's numeric user ID via SCIM. The bundle names projects
    `lakebase-workshop-<deployer_user_id>`, so this gives us the exact project.
    """
    app_name = os.environ.get("DATABRICKS_APP_NAME")
    if not app_name:
        return ""

    try:
        app_info = w.apps.get(name=app_name)
        creator_email = app_info.creator
        if not creator_email:
            return ""
        users = list(w.users.list(filter=f'userName eq "{creator_email}"'))
        if not users:
            logger.warning(f"No SCIM user found for app creator {creator_email}")
            return ""
        return f"lakebase-workshop-{users[0].id}"
    except Exception as e:
        logger.warning(f"Could not derive project from app creator: {e}")
        return ""


def _resolve_endpoint(project_id: str, endpoint_name: str) -> str:
    """If endpoint_name is empty, list endpoints on the project's production branch."""
    if endpoint_name:
        return endpoint_name
    endpoints = list(w.postgres.list_endpoints(
        parent=f"projects/{project_id}/branches/production"
    ))
    if not endpoints:
        raise RuntimeError(
            f"Project {project_id} has no production endpoint yet — provisioning may "
            f"still be in progress."
        )
    return endpoints[0].name


LAKEBASE_PROJECT, ENDPOINT_NAME = _discover_workshop_project()


class OAuthConnection(psycopg.Connection):
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


username = os.environ.get("PGUSER", "") or w.current_user.me().user_name
host = os.environ.get("PGHOST", "")
port = os.environ.get("PGPORT", "5432")
database = os.environ.get("PGDATABASE", "databricks_postgres")
sslmode = os.environ.get("PGSSLMODE", "require")

if not host:
    host = w.postgres.get_endpoint(name=ENDPOINT_NAME).status.hosts.host
    logger.info(f"Resolved host: {host}")

pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} port={port} sslmode={sslmode} connect_timeout=15",
    connection_class=OAuthConnection,
    min_size=1,
    max_size=10,
    max_lifetime=2700,
    open=False,
)


def get_branch_connection(branch_id: str) -> psycopg.Connection:
    """Get a direct connection to a specific branch endpoint."""
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
