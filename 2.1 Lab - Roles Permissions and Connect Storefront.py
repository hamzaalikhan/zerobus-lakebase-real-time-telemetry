# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2.1: Roles, Permissions, and Connecting the Storefront to Lakebase
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This lab covers how **permissions** work in Lakebase across the two main layers — **workspace**
# MAGIC and **database** — and then grants the DataCart Storefront app the database access it needs to
# MAGIC start serving real data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Understand the two permission layers** — workspace and database — and how they interact
# MAGIC 2. **Manage project permissions** to grant access to Databricks identities, groups, and service principals
# MAGIC 3. **Manage Postgres roles** by creating OAuth and password-based roles for database access
# MAGIC 4. **Grant schema-level permissions** using standard PostgreSQL `GRANT` commands
# MAGIC 5. **Verify and audit** role permissions using PostgreSQL catalog queries
# MAGIC 6. **Connect** the already-deployed DataCart Storefront app to Lakebase via its service principal
# MAGIC
# MAGIC ## Where We Are
# MAGIC
# MAGIC 1. **Lab 1.1** discovered your Lakebase Autoscaling project and seeded the `ecommerce` schema
# MAGIC    with 5 tables: `customers`, `products`, `inventory`, `orders`, and `order_items`.
# MAGIC
# MAGIC 2. **The DataCart Storefront app** is already deployed and running as a Databricks App, but
# MAGIC    currently shows **"Loading..."** because its service principal has no Postgres access yet —
# MAGIC    that's what this lab fixes.
# MAGIC
# MAGIC > **Docs**: [Manage Postgres roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles) | [Grant user access tutorial](https://docs.databricks.com/aws/en/oltp/projects/grant-user-access-tutorial) | [Manage permissions](https://docs.databricks.com/aws/en/oltp/projects/manage-permissions)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Understanding Permission Layers
# MAGIC
# MAGIC Lakebase has **two independent permission layers**. Think of it like a house:
# MAGIC
# MAGIC ![security_layers.jpg](./Includes/images/core_concepts/security_layers.jpg)
# MAGIC
# MAGIC - **Workspace Layer (The House Key)**: Controls who can enter the house, renovate the kitchen, or pay the electric bill. In Databricks, this is who can manage the project — restart compute, create branches, delete the project.
# MAGIC
# MAGIC - **Database Layer (The Safe Combination)**: Just because you are in the house doesn't mean you can open the safe. In Databricks, this is standard PostgreSQL security — who can query tables, insert/update/delete data, manage schemas.
# MAGIC
# MAGIC <table style="border-collapse: collapse; width: 100%; border: 2px solid #5c5c5c;">
# MAGIC   <thead>
# MAGIC     <tr style="background-color: #f2f2f2;">
# MAGIC       <th style="border: 1px solid #5c5c5c; text-align: left; padding: 12px; font-weight: bold;">Layer</th>
# MAGIC       <th style="border: 1px solid #5c5c5c; text-align: left; padding: 12px; font-weight: bold;">Authentication</th>
# MAGIC       <th style="border: 1px solid #5c5c5c; text-align: left; padding: 12px; font-weight: bold;">Interfaces</th>
# MAGIC       <th style="border: 1px solid #5c5c5c; text-align: left; padding: 12px; font-weight: bold;">What it controls</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;"><strong>Workspace</strong></td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">Workspace OAuth tokens</td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">REST API, Databricks CLI, Databricks SDKs (Python, Java, Go), Terraform</td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">Platform-level actions: creating branches, managing computes, managing project settings</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;"><strong>Database</strong></td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">OAuth database tokens OR Postgres passwords</td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">Postgres clients (psql, pgAdmin), drivers (psycopg, JDBC), Data API</td>
# MAGIC       <td style="border: 1px solid #5c5c5c; text-align: left; padding: 10px;">Who can access data within the database itself</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC These two layers have **no automatic synchronization**. You can grant them independently or together, depending on your organization's requirements.
# MAGIC
# MAGIC **Key principle:** Having workspace access does **not** automatically grant database access, and vice versa. Always configure both layers for your users.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workspace Layer
# MAGIC
# MAGIC This layer controls who can work with Lakebase **platform resources**.
# MAGIC
# MAGIC **By default, all workspace users inherit CAN CREATE permission**, which allows viewing and creating projects. To grant additional access, you must explicitly assign CAN USE or CAN MANAGE:
# MAGIC
# MAGIC | Permission | Description |
# MAGIC |---|---|
# MAGIC | **CAN CREATE** | View and create projects (default for all workspace users) |
# MAGIC | **CAN USE** | View and use project resources (connect, list, view) without creating or managing them |
# MAGIC | **CAN MANAGE** | Full control over project configuration and resources |
# MAGIC
# MAGIC For a complete list of actions each permission level allows, see [Lakebase project ACLs](https://docs.databricks.com/aws/en/security/auth/access-control/#database-project).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Database Layer
# MAGIC
# MAGIC Postgres role permissions control who can access **data within the database** itself.
# MAGIC
# MAGIC When you create a project, Lakebase creates several Postgres roles automatically:
# MAGIC - A Postgres role for the **project owner's Databricks identity** (e.g., `user@databricks.com`), which owns the default `databricks_postgres` database
# MAGIC - A **`databricks_superuser`** administrative role
# MAGIC - Several [system-managed roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles?language=UI#system-roles-created-by-databricks) used by Databricks services for management, monitoring, and data operations
# MAGIC
# MAGIC Lakebase supports two types of Postgres roles for database access:
# MAGIC
# MAGIC <img src="./Includes/images/core_concepts/postgres_roles.png" alt="Postgres Roles" width="900"/>
# MAGIC
# MAGIC - **OAuth roles for Databricks identities**: Created using the `databricks_auth` extension and SQL. Enables Databricks identities (users, service principals, and groups) to connect using OAuth tokens.
# MAGIC
# MAGIC - **Native Postgres password roles**: Created using the Lakebase UI or SQL. Use any valid role name with password authentication.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Creating Postgres Roles
# MAGIC
# MAGIC Before diving into the hands-on lab, let's understand how roles are created at the **database layer**.
# MAGIC
# MAGIC <div style="background-color: #e7f3fe; border-left: 6px solid #2196F3; padding: 12px; margin: 10px 0;">
# MAGIC <strong>Important:</strong> The <code>databricks_create_role()</code> function creates a Postgres role with <strong>LOGIN permission only</strong>. After creating the role, you must explicitly grant database privileges (covered in the Granting Permissions section below).
# MAGIC </div>
# MAGIC
# MAGIC ### Setting Up the `databricks_auth` Extension
# MAGIC
# MAGIC Before creating OAuth roles, each database must have the `databricks_auth` extension installed:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC ```
# MAGIC
# MAGIC ### Creating OAuth Roles
# MAGIC
# MAGIC Use the `databricks_create_role()` function to create Postgres roles for Databricks identities. It accepts two parameters:
# MAGIC - **identity_name**: The Databricks username, service principal ID, or group name
# MAGIC - **identity_type**: One of `'USER'`, `'SERVICE_PRINCIPAL'`, or `'GROUP'`
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - You must have `CREATE` and `CREATE ROLE` permissions on the database
# MAGIC - You must be authenticated as a Databricks identity with a valid OAuth token
# MAGIC - Native Postgres sessions **cannot** create OAuth roles
# MAGIC
# MAGIC **Examples:**
# MAGIC
# MAGIC ```sql
# MAGIC -- For a Databricks user
# MAGIC SELECT databricks_create_role('colleague@your-company.com', 'USER');
# MAGIC
# MAGIC -- For a service principal (use the application/client ID)
# MAGIC SELECT databricks_create_role('8c01cfb1-62c9-4a09-88a8-e195f4b01b08', 'SERVICE_PRINCIPAL');
# MAGIC
# MAGIC -- For a Databricks group (case-sensitive, must match exactly)
# MAGIC SELECT databricks_create_role('Data Engineers', 'GROUP');
# MAGIC ```
# MAGIC
# MAGIC <div style="background-color: #fff3cd; border-left: 6px solid #ffc107; padding: 12px; margin: 10px 0;">
# MAGIC <strong>Group-based authentication tip:</strong> When you create a role for a group, <strong>all members</strong> of that Databricks group can authenticate using the group's role name and their own individual OAuth token. This is the recommended approach for managing permissions at scale.
# MAGIC </div>
# MAGIC
# MAGIC ### Creating Native Postgres Password Roles
# MAGIC
# MAGIC For external applications or non-Databricks users, you can create traditional Postgres roles with password authentication. Passwords must have at least **12 characters** with a mix of lowercase, uppercase, numbers, and symbols (minimum 60-bit entropy):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE ROLE app_readonly_user WITH LOGIN PASSWORD 'Ch@ngeMe_Secur3!2025';
# MAGIC ```

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Granting Database Permissions
# MAGIC
# MAGIC A newly created Postgres role has **LOGIN permission only** and **no database privileges**. You must explicitly grant access using standard PostgreSQL `GRANT` / `REVOKE` commands.
# MAGIC
# MAGIC <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 15px; margin: 10px 0;">
# MAGIC <strong>Permission Hierarchy:</strong><br><br>
# MAGIC <code>Database</code> &rarr; <code>Schema</code> &rarr; <code>Tables / Sequences / Functions</code><br><br>
# MAGIC A user needs <strong>CONNECT</strong> on the database, <strong>USAGE</strong> on the schema, <strong>and</strong> the specific table privilege (SELECT, INSERT, etc.) to actually access data.
# MAGIC </div>
# MAGIC
# MAGIC ### Database-Level Permissions
# MAGIC
# MAGIC | Privilege | Description |
# MAGIC |---|---|
# MAGIC | `CONNECT` | Allows the role to connect to the database |
# MAGIC | `CREATE` | Allows creating new schemas in the database |
# MAGIC | `TEMPORARY` | Allows creating temporary tables |
# MAGIC | `ALL PRIVILEGES` | Grants CONNECT + CREATE + TEMPORARY |
# MAGIC
# MAGIC ### Schema-Level Permissions
# MAGIC
# MAGIC | Privilege | Description |
# MAGIC |---|---|
# MAGIC | `USAGE` | Allows accessing objects within the schema (required for any table access) |
# MAGIC | `CREATE` | Allows creating new tables, views, and other objects in the schema |
# MAGIC
# MAGIC ### Table-Level Permissions
# MAGIC
# MAGIC | Privilege | Description |
# MAGIC |---|---|
# MAGIC | `SELECT` | Read data from the table |
# MAGIC | `INSERT` | Add new rows to the table |
# MAGIC | `UPDATE` | Modify existing rows |
# MAGIC | `DELETE` | Remove rows from the table |
# MAGIC | `TRUNCATE` | Empty the entire table |
# MAGIC | `ALL PRIVILEGES` | Grants all of the above |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On: Connect the DataCart Storefront to Lakebase
# MAGIC
# MAGIC Now let's put these concepts into practice. Every Databricks App runs as a **service principal (SP)** — an automated identity separate from your user account. The SP was created automatically when you created the app, but it needs:
# MAGIC
# MAGIC 1. **A Postgres role** — Created automatically when you add the Lakebase project as an app resource
# MAGIC 2. **Schema-level grants** — The SP's Postgres role exists but has no permissions on the `ecommerce` schema
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐        ┌──────────────────────┐
# MAGIC │  Databricks App      │        │  Lakebase Project     │
# MAGIC │  "storefront-<id>"   │        │  production branch    │
# MAGIC │                      │        │                       │
# MAGIC │  Runs as SP:         │  OAuth │  Postgres role:       │
# MAGIC │  8241cbc7-...        │───────▶│  "8241cbc7-..."       │
# MAGIC │                      │  token │                       │
# MAGIC │  Needs:              │        │  Needs:               │
# MAGIC │  • generate_db_cred  │        │  • USAGE on schema    │
# MAGIC │  • CAN_MANAGE on     │        │  • ALL on tables      │
# MAGIC │    project           │        │  • ALL on sequences   │
# MAGIC └──────────────────────┘        └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Why can't the SP access the schema automatically?**
# MAGIC > In PostgreSQL, having a role doesn't grant access to any schemas or tables.
# MAGIC > The project owner must explicitly grant permissions — this is standard Postgres
# MAGIC > security and a good practice for production databases.
# MAGIC
# MAGIC > **Why not just use your user account?**
# MAGIC > Databricks Apps run as service principals for security isolation. The app
# MAGIC > should never use a human user's credentials — SPs have their own OAuth tokens
# MAGIC > that rotate automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify the Service Principal
# MAGIC
# MAGIC Every Databricks App has a service principal (SP) that acts as its identity.
# MAGIC The SP is created automatically when you create the app. Let's find it. **Make sure to change the APP_NAME variable to the name of the app you have deployed.**

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Your Lakebase project — name auto-derived from your numeric user ID
project_name = f"lakebase-workshop-{w.current_user.me().id}"
db_user = w.current_user.me().user_name

# The storefront app — name also auto-derived from your numeric user ID
APP_NAME = f"storefront-{w.current_user.me().id}"

app_info = w.apps.get(APP_NAME)
SP_CLIENT_ID = app_info.service_principal_client_id
SP_NAME = app_info.service_principal_name
APP_URL = app_info.url

print(f"📋 App Details:")
print(f"   App Name:      {APP_NAME}")
print(f"   App URL:       {APP_URL}")
print(f"   SP Client ID:  {SP_CLIENT_ID}")
print(f"   SP Name:       {SP_NAME}")
print(f"")
print(f"📋 Lakebase Project:")
print(f"   Project:       {project_name}")
print(f"   Schema:        ecommerce")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Connect to the Production Branch
# MAGIC
# MAGIC We connect as **you** (the project owner) to grant roles to the SP.
# MAGIC Only the project owner or a user with sufficient privileges can grant access.

# COMMAND ----------

import psycopg2

DB_SCHEMA = "ecommerce"

# Get production endpoint
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next(b for b in branches if b.status and b.status.default)
prod_branch_name = prod_branch.name

endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))
prod_endpoint = endpoints[0]
prod_host = prod_endpoint.status.hosts.host
prod_endpoint_name = prod_endpoint.name

# Generate your OAuth credential (project owner)
cred = w.postgres.generate_database_credential(endpoint=prod_endpoint_name)

conn = psycopg2.connect(
    host=prod_host,
    port=5432,
    dbname="databricks_postgres",
    user=db_user,
    password=cred.token,
    sslmode="require"
)
conn.autocommit = True

print(f"✅ Connected to production branch as {db_user}")
print(f"   Host: {prod_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Grant the SP Project-Level Access + Create Its Postgres Role
# MAGIC
# MAGIC Two grants are needed before the storefront can connect:
# MAGIC
# MAGIC | Grant | Layer | What it enables |
# MAGIC |---|---|---|
# MAGIC | `CAN_USE` on the Lakebase project | Databricks platform | The SP can `list_projects()` / `list_endpoints()` and generate OAuth credentials |
# MAGIC | A Postgres role tied to the SP's identity | Postgres | The SP has a database-level identity to authenticate as |
# MAGIC
# MAGIC On Lakebase Provisioned, binding a database to an app via the Apps platform auto-creates
# MAGIC both. Lakebase Autoscaling doesn't have that native binding yet, so we do both with two
# MAGIC explicit calls — one Databricks Permissions API call, one SQL call.
# MAGIC
# MAGIC > **Docs:** [Grant user access tutorial](https://docs.databricks.com/aws/en/oltp/projects/grant-user-access-tutorial)

# COMMAND ----------

# Grant the app's SP CAN_USE on the Lakebase project (Databricks-side permission).
# This lets the SP list projects/endpoints and request OAuth tokens.
# NOTE: the Permissions API for "database-projects" identifies the project by its
# project_id (the human-readable name), NOT the UUID.
try:
    w.api_client.do(
        "PATCH",
        f"/api/2.0/permissions/database-projects/{project_name}",
        body={
            "access_control_list": [
                {
                    "service_principal_name": SP_CLIENT_ID,
                    "permission_level": "CAN_USE",
                }
            ]
        },
    )
    print(f"✅ Granted CAN_USE on project to SP {SP_CLIENT_ID}")
except Exception as e:
    print(f"⚠️  Could not set project permission via SDK: {e}")
    print(f"   You can grant it manually: open the Lakebase project's Settings → Project Permissions → add the SP with CAN_USE.")

# COMMAND ----------

with conn.cursor() as cur:
    # Make sure the databricks_auth extension is installed (idempotent).
    cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth;")

    # Check if the role already exists (re-run safety).
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (SP_CLIENT_ID,))
    if cur.fetchone():
        print(f"ℹ️  SP role '{SP_CLIENT_ID}' already exists — skipping create.")
    else:
        # Create the Postgres role tied to the app's service principal identity.
        cur.execute(
            "SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')",
            (SP_CLIENT_ID,)
        )
        print(f"✅ Created SP Postgres role '{SP_CLIENT_ID}'")

    # Verify
    cur.execute("""
        SELECT r.rolname, r.rolcanlogin
        FROM pg_roles r
        WHERE r.rolname = %s
    """, (SP_CLIENT_ID,))
    role_info = cur.fetchone()

if role_info:
    print(f"\n📋 Verified SP role:")
    print(f"   Role name: {role_info[0]}")
    print(f"   Can login: {role_info[1]}")
else:
    raise RuntimeError(
        f"SP role '{SP_CLIENT_ID}' could not be created. "
        f"Check that you have CAN_MANAGE on the Lakebase project "
        f"and that the databricks_auth extension is available."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Explore Existing Roles
# MAGIC
# MAGIC Before granting permissions, let's see all the roles currently in the project. This uses the same `pg_roles` catalog query we discussed in the lecture section above.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute("""
        SELECT
            rolname AS role_name,
            rolcanlogin AS can_login,
            rolcreatedb AS can_create_db,
            rolcreaterole AS can_create_role
        FROM pg_roles
        WHERE rolname NOT LIKE 'pg_%'
        ORDER BY rolname;
    """)
    roles = cur.fetchall()

print("📋 All roles in the project (excluding pg_ system roles):\n")
print(f"   {'Role Name':<45} {'Login':<8} {'CreateDB':<10} {'CreateRole'}")
print(f"   {'-'*45} {'-'*8} {'-'*10} {'-'*10}")
for r in roles:
    print(f"   {r[0]:<45} {str(r[1]):<8} {str(r[2]):<10} {r[3]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Grant Schema Permissions
# MAGIC
# MAGIC Now we apply the permission hierarchy we learned about. The SP needs access at the **schema** and **table** level:
# MAGIC
# MAGIC | Grant | Purpose |
# MAGIC |-------|---------|
# MAGIC | `USAGE ON SCHEMA` | Allows the SP to see the schema and its objects |
# MAGIC | `ALL ON ALL TABLES` | Read products/inventory, write orders, etc. |
# MAGIC | `ALL ON ALL SEQUENCES` | Needed for `SERIAL` columns (auto-increment IDs) |
# MAGIC | `ALTER DEFAULT PRIVILEGES` | Ensures future tables (reviews, loyalty_members, promotions) are automatically accessible |
# MAGIC
# MAGIC > The `ALTER DEFAULT PRIVILEGES` grants are important — as the workshop progresses
# MAGIC > and new tables are created (promotions in Lab 3.1, reviews in Lab 6.2), the SP
# MAGIC > will automatically have access without needing to re-run this notebook.

# COMMAND ----------

with conn.cursor() as cur:
    sp_role = f'"{SP_CLIENT_ID}"'

    # Grant schema usage
    cur.execute(f"GRANT USAGE ON SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted USAGE on schema {DB_SCHEMA}")

    # Grant ALL on all existing tables
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted ALL on all tables in {DB_SCHEMA}")

    # Grant ALL on all sequences (needed for SERIAL columns / INSERT)
    cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
    print(f"✅ Granted ALL on all sequences in {DB_SCHEMA}")

    # Set default privileges for future tables
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON TABLES TO {sp_role};")
    print(f"✅ Set default privileges for future tables")

    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON SEQUENCES TO {sp_role};")
    print(f"✅ Set default privileges for future sequences")

print(f"\n🎉 Service principal {SP_CLIENT_ID} now has full access to {DB_SCHEMA}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify the Grants
# MAGIC
# MAGIC Let's confirm the SP has the expected permissions using the `information_schema` and the `has_table_privilege()` function.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT grantee, table_name, privilege_type
        FROM information_schema.table_privileges
        WHERE table_schema = '{DB_SCHEMA}'
          AND grantee = '{SP_CLIENT_ID}'
        ORDER BY table_name, privilege_type
    """)
    grants = cur.fetchall()

if grants:
    print(f"✅ Grants verified for SP on {DB_SCHEMA} tables:\n")
    current_table = ""
    for g in grants:
        if g[1] != current_table:
            current_table = g[1]
            print(f"  {DB_SCHEMA}.{current_table}:")
        print(f"    • {g[2]}")
else:
    print(f"⚠️ No direct table grants found.")
    print(f"   The default privileges should still work for future tables.")
    print(f"   Try accessing the storefront to confirm connectivity.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Table-Level Permissions
# MAGIC
# MAGIC Use the `has_table_privilege()` function for a more detailed check of what the SP can do on each table:

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT
            tablename AS table_name,
            has_table_privilege('{SP_CLIENT_ID}', '{DB_SCHEMA}.' || tablename, 'SELECT') AS can_select,
            has_table_privilege('{SP_CLIENT_ID}', '{DB_SCHEMA}.' || tablename, 'INSERT') AS can_insert,
            has_table_privilege('{SP_CLIENT_ID}', '{DB_SCHEMA}.' || tablename, 'UPDATE') AS can_update,
            has_table_privilege('{SP_CLIENT_ID}', '{DB_SCHEMA}.' || tablename, 'DELETE') AS can_delete
        FROM pg_tables
        WHERE schemaname = '{DB_SCHEMA}'
        ORDER BY tablename;
    """)
    rows = cur.fetchall()

print(f"📋 Table-level permissions for SP on {DB_SCHEMA}:\n")
print(f"   {'Table':<25} {'SELECT':<10} {'INSERT':<10} {'UPDATE':<10} {'DELETE'}")
print(f"   {'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
for r in rows:
    print(f"   {r[0]:<25} {str(r[1]):<10} {str(r[2]):<10} {str(r[3]):<10} {r[4]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Role Memberships
# MAGIC
# MAGIC Let's also verify role memberships in the project — this shows how roles relate to each other:

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute("""
        SELECT
            r.rolname AS role,
            pg_get_userbyid(m.member) AS member
        FROM pg_auth_members m
        JOIN pg_roles r ON r.oid = m.roleid
        ORDER BY r.rolname;
    """)
    memberships = cur.fetchall()

print("📋 Role memberships:\n")
print(f"   {'Role':<40} {'Member'}")
print(f"   {'-'*40} {'-'*40}")
for m in memberships:
    print(f"   {m[0]:<40} {m[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test the Storefront
# MAGIC
# MAGIC The app should now be able to connect. Open the storefront URL and verify it loads the product catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC Check the app! The service should be connected now!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Grant Roles on Dev Branches (Optional)
# MAGIC
# MAGIC If you've already created branches in Labs 3.1-3.4, the SP needs roles on those too.
# MAGIC This is because each branch has its own independent set of Postgres roles and grants.
# MAGIC
# MAGIC > You can re-run this cell any time after creating new branches.

# COMMAND ----------

# import time

# all_branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

# granted_count = 0
# for branch in all_branches:
#     branch_id = branch.name.split("/branches/")[-1]
#     if branch_id == "production":
#         continue  # Already done above

#     try:
#         branch_endpoints = list(w.postgres.list_endpoints(parent=branch.name))
#         if not branch_endpoints:
#             print(f"   Skipping {branch_id} (no endpoint)")
#             continue

#         branch_host = branch_endpoints[0].status.hosts.host
#         branch_endpoint_name = branch_endpoints[0].name
#         branch_cred = w.postgres.generate_database_credential(endpoint=branch_endpoint_name)

#         branch_conn = psycopg2.connect(
#             host=branch_host, port=5432,
#             dbname="databricks_postgres",
#             user=db_user, password=branch_cred.token,
#             sslmode="require"
#         )
#         branch_conn.autocommit = True

#         with branch_conn.cursor() as cur:
#             sp_role = f'"{SP_CLIENT_ID}"'
#             cur.execute(f"GRANT USAGE ON SCHEMA {DB_SCHEMA} TO {sp_role};")
#             cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
#             cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {DB_SCHEMA} TO {sp_role};")
#             cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON TABLES TO {sp_role};")
#             cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {DB_SCHEMA} GRANT ALL ON SEQUENCES TO {sp_role};")

#         branch_conn.close()
#         print(f"   ✅ Granted SP roles on branch: {branch_id}")
#         granted_count += 1
#     except Exception as e:
#         print(f"   ❌ Failed on {branch_id}: {e}")

# if granted_count == 0:
#     print("ℹ️ No dev branches found (only production). This is expected before Labs 3.1-3.4.")
# else:
#     print(f"\n✅ Granted roles on {granted_count} branch(es)")

# COMMAND ----------

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC In this lab, you learned the two independent permission layers in Lakebase and applied them hands-on:
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC
# MAGIC 1. **Workspace Layer** — Controls platform-level actions (CAN CREATE, CAN USE, CAN MANAGE)
# MAGIC 2. **Database Layer** — Controls data access using standard PostgreSQL roles and privileges
# MAGIC 3. **OAuth roles** — Created via `databricks_create_role()` for Databricks users, service principals, and groups
# MAGIC 4. **Native Postgres roles** — Traditional password-based authentication for external apps
# MAGIC 5. **Permission hierarchy** — Database → Schema → Tables/Sequences, each level must be granted independently
# MAGIC
# MAGIC ### Hands-On Accomplishments
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Identify SP** | Found the app's service principal client ID via the SDK |
# MAGIC | **Verify role** | Confirmed the Postgres role was auto-created by the app resource |
# MAGIC | **Explore roles** | Listed all roles and their attributes using `pg_roles` |
# MAGIC | **Grant permissions** | Gave the SP `USAGE`, `ALL` on tables/sequences in `ecommerce` |
# MAGIC | **Default privileges** | Ensured future tables (reviews, promotions, etc.) are auto-accessible |
# MAGIC | **Verify & audit** | Used `information_schema`, `has_table_privilege()`, and role memberships to confirm grants |
# MAGIC | **Test connectivity** | Verified the storefront can query Lakebase |
# MAGIC
# MAGIC ### What's Next
# MAGIC
# MAGIC The DataCart Storefront is now connected and ready. As you run through the remaining
# MAGIC labs, the storefront will **evolve automatically**:
# MAGIC
# MAGIC - **Lab 3.1** — Sale badges and discount prices appear via Reverse ETL
# MAGIC - **Labs 4.1 / 5.1** — UC foreign catalog and Lakehouse Sync go live (no storefront change; analytics surface lights up)
# MAGIC - **Lab 6.2** — Star ratings, loyalty badges, and "Earn pts" labels appear
# MAGIC - **Lab 6.3** — Priority badges on orders, verified badge in navbar
# MAGIC - **Lab 7.1** — Orders page breaks during the PITR disaster, then recovers
# MAGIC
# MAGIC > The `ALTER DEFAULT PRIVILEGES` grants ensure the SP can access new tables
# MAGIC > created in later labs without needing to re-run this notebook.
