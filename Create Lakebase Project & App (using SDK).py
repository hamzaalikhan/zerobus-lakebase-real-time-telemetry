# Databricks notebook source
# MAGIC %md
# MAGIC # Optional: Create the Lakebase Project and Storefront App via SDK (No DABs)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to use this notebook
# MAGIC
# MAGIC The default workshop path uses Declarative Automation Bundles (DABs) to provision the Lakebase
# MAGIC Autoscaling project, the storefront app, and the binding between them in one shot. **That's
# MAGIC the recommended path.**
# MAGIC
# MAGIC Use this notebook **only if** you can't or don't want to deploy with DABs — for example:
# MAGIC
# MAGIC - Your workspace doesn't have the Workspace Files / serverless compute features required by the workspace-UI deploy
# MAGIC - You don't have the Databricks CLI installed and don't want to install it
# MAGIC - You want to step through the SDK calls yourself to learn the underlying API
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Creates the **Lakebase Autoscaling project** (`lakebase-workshop-<your-user-id>`)
# MAGIC 2. Verifies the default `production` branch and its compute endpoint
# MAGIC 3. Creates the **DataCart Storefront app** (`storefront-<your-user-id>`)
# MAGIC 4. **Binds the Lakebase project to the app** as a database resource — the platform then
# MAGIC    auto-injects `PGHOST`, `PGUSER`, `PGPORT`, and `PGDATABASE` env vars on the next deploy
# MAGIC
# MAGIC The **only** remaining manual step is pointing the app at the source code and clicking
# MAGIC **Deploy** in the workspace UI — see the section at the bottom.
# MAGIC
# MAGIC > **Naming compatibility.** The project and app names match the DABs version exactly, so
# MAGIC > Lab 1.1 (project discovery) and Lab 2.1 (app discovery + permission grants) work with no
# MAGIC > further configuration.
# MAGIC
# MAGIC > **Docs**: [Lakebase Autoscaling Projects](https://docs.databricks.com/aws/en/oltp/projects/) | [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/) | [App resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/configuration#--resources)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC The project ID is auto-derived from your numeric Databricks user ID — same convention as
# MAGIC the bundle. The display name combines your first and last name for readability in the UI.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()

project_name = f"lakebase-workshop-{me.id}"
display_name = f"Lakebase Workshop — {me.name.given_name} {me.name.family_name}"

# Compute settings — match the DABs config in resources/lakebase_instance.yml
PG_VERSION = 17
MIN_CU = 0.5
MAX_CU = 2.0
SUSPEND_TIMEOUT_SECONDS = 300

print(f"User:                {me.user_name}")
print(f"Project ID:          {project_name}")
print(f"Display name:        {display_name}")
print(f"Postgres version:    {PG_VERSION}")
print(f"Compute:             {MIN_CU} – {MAX_CU} CU, scale-to-zero after {SUSPEND_TIMEOUT_SECONDS}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Project
# MAGIC
# MAGIC `create_project` is a long-running operation — `.wait()` blocks until the project is in
# MAGIC `AVAILABLE` state. Idempotent: re-running this cell against an existing project returns
# MAGIC without recreating it.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings, Duration
)

# Re-run safety: skip create if project already exists.
existing = next(
    (p for p in w.postgres.list_projects() if p.name == f"projects/{project_name}"),
    None,
)

if existing:
    print(f"ℹ️  Project '{project_name}' already exists — skipping create.")
    project_obj = existing
else:
    print(f"🔄 Creating project '{project_name}'...")
    project_obj = w.postgres.create_project(
        project=Project(spec=ProjectSpec(
            display_name=display_name,
            pg_version=PG_VERSION,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=MIN_CU,
                autoscaling_limit_max_cu=MAX_CU,
                suspend_timeout_duration=Duration(seconds=SUSPEND_TIMEOUT_SECONDS),
            ),
        )),
        project_id=project_name,
    ).wait()
    print(f"✅ Project '{project_name}' created!")

workspace_host = w.config.host.rstrip("/")
print(f"\n🔗 Lakebase UI: {workspace_host}/lakebase/projects/{project_obj.uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify the Production Branch & Endpoint

# COMMAND ----------

import time

# The default 'production' branch is created automatically.
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next(b for b in branches if b.status and b.status.default)
print(f"✅ Production branch: {prod_branch.name}")

# Wait for the primary compute endpoint to be ready (typically <60s).
endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
for i in range(30):
    if endpoints:
        break
    time.sleep(10)
    endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
    print(f"   waiting for endpoint... ({(i+1)*10}s)")

if not endpoints:
    raise RuntimeError("Compute endpoint not available after 5 minutes.")

ep = endpoints[0]
print(f"\n✅ Endpoint ready:")
print(f"   Name: {ep.name}")
print(f"   Host: {ep.status.hosts.host}")
print(f"   Database: databricks_postgres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create the Storefront App and Bind the Lakebase Project
# MAGIC
# MAGIC We'll create the Databricks App and attach the Lakebase project as a **database resource**
# MAGIC in a single SDK call. Binding the project as a resource is what gets the platform to inject
# MAGIC `PGHOST`, `PGUSER`, `PGPORT`, and `PGDATABASE` into the app's environment on the next deploy.
# MAGIC
# MAGIC The app name pattern (`storefront-<your-user-id>`) matches the DAB version, so Lab 2.1
# MAGIC discovers it the same way it would after a `bundle deploy`.

# COMMAND ----------

from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import (
    App,
    AppResource,
    AppResourcePostgres,
    AppResourcePostgresPostgresPermission,
)

app_name = f"storefront-{me.id}"
app_description = f"DataCart Storefront — {me.name.given_name} {me.name.family_name}"

# Discover the default database on the production branch. Lakebase auto-creates one
# (typically named `databricks-postgres`) but the exact name is set by the service, so
# we look it up rather than hardcoding. Poll briefly in case it hasn't materialized yet.
print("⏳ Discovering default database on the production branch...")
default_db_full_name = None
for i in range(24):                                              # up to ~2 minutes
    databases = list(w.postgres.list_databases(parent=prod_branch.name))
    if databases:
        default_db_full_name = databases[0].name
        print(f"✅ Database ready: {default_db_full_name}")
        break
    time.sleep(5)
    print(f"   waiting for default database... ({(i+1)*5}s)")

if default_db_full_name is None:
    raise RuntimeError(
        f"No databases found in {prod_branch.name} after 2 minutes. "
        f"Check the project status in the Lakebase UI."
    )

lakebase_resource = AppResource(
    name="lakebase",
    description="Workshop Lakebase Autoscaling project (production branch)",
    postgres=AppResourcePostgres(
        branch=prod_branch.name,                                       # projects/<id>/branches/production
        database=default_db_full_name,                                 # projects/<id>/branches/<branch>/databases/databricks-postgres
        permission=AppResourcePostgresPostgresPermission.CAN_CONNECT_AND_CREATE,
    ),
)

app_spec = App(
    name=app_name,
    description=app_description,
    resources=[lakebase_resource],
)

# Re-run safety: update if app already exists, otherwise create.
try:
    w.apps.get(name=app_name)
    print(f"ℹ️  App '{app_name}' already exists — updating resources binding...")
    app_obj = w.apps.update(name=app_name, app=app_spec)
    print(f"✅ App '{app_name}' updated with Lakebase binding.")
except NotFound:
    print(f"🔄 Creating app '{app_name}'...")
    app_obj = w.apps.create(app=app_spec).result()
    print(f"✅ App '{app_name}' created with Lakebase binding.")

print()
print(f"📋 App details:")
print(f"   Name:           {app_obj.name}")
print(f"   URL:            {app_obj.url}")
print(f"   SP Client ID:   {app_obj.service_principal_client_id}")
print(f"   SP Name:        {app_obj.service_principal_name}")
print()
print(f"🔗 App in workspace UI: {workspace_host}/apps/{app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## You're done with the SDK setup
# MAGIC
# MAGIC The Lakebase project, the storefront app, and the binding between them are all in place.
# MAGIC The labs (Lab 1.1 onward) will discover them automatically.
# MAGIC
# MAGIC There's **one** manual step left — pointing the app at the source code and clicking Deploy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final step: Deploy the source code (UI)
# MAGIC
# MAGIC 1. In the workspace, go to **Compute → Apps → `storefront-<your-user-id>`** (your app from
# MAGIC    Step 4 — the description shows your name to help you spot it).
# MAGIC 2. Click the **Deploy** button.
# MAGIC 3. Set the source path to wherever you have the `datacart-storefront/` source files in
# MAGIC    your workspace, e.g.
# MAGIC    `/Workspace/Users/<your-email>/lakebase-in-a-box-workshop-data-centric/datacart-storefront`.
# MAGIC 4. Click **Deploy** again. The app starts and shows **"Loading…"** until you grant the SP
# MAGIC    database access in Lab 2.1 — that's expected.
# MAGIC
# MAGIC From here, continue the workshop with **Lab 1.1**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC If you ever want to tear down what this notebook created (without `databricks bundle destroy`):

# COMMAND ----------

# Uncomment to delete the app and the Lakebase project.
# WARNING: deletes all branches, computes, databases, data, and the app's source deployment.
# w.apps.delete(name=app_name)
# print(f"🗑️  Deleted app {app_name}")
# w.postgres.delete_project(name=f"projects/{project_name}").wait()
# print(f"🗑️  Deleted project {project_name}")

