# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.1: Register Lakebase in Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Direction 2 of 3: Live Read-Through from UC into Lakebase
# MAGIC
# MAGIC In Lab 3.1 you pushed curated Lakehouse data **into** Lakebase via a Synced Table. This lab
# MAGIC is the second movement in the data-flow story: **register your Lakebase database as a foreign
# MAGIC catalog in Unity Catalog**, so analytics tools can query the live OLTP data — with no ETL,
# MAGIC no copy, no staleness — and join it against Delta tables in the lakehouse.
# MAGIC
# MAGIC | Direction | Lab | Mechanism |
# MAGIC |---|---|---|
# MAGIC | UC → Lakebase | 3.1 | Synced Tables |
# MAGIC | **Live read-through** | **4.1 (this lab)** | **Lakehouse Federation (foreign catalog)** |
# MAGIC | Lakebase → UC | 5.1 | Lakehouse Sync |
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** the difference between Synced Tables (materialized) and Lakehouse Federation (live)
# MAGIC 2. **Register** a Lakebase database as a Unity Catalog foreign catalog using the Catalog Explorer UI
# MAGIC 3. **Run** a federated join between live OLTP data and Delta analytics data from the SQL Editor
# MAGIC 4. **Reason** about when to use federation vs. Lakehouse Sync (covered in Lab 5.1)
# MAGIC
# MAGIC > **Docs**: [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc) | [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Why Register a Lakebase Database in Unity Catalog?
# MAGIC
# MAGIC Registering Lakebase in UC turns your operational database into a first-class citizen of the
# MAGIC lakehouse — discoverable, governed, and joinable with Delta — without any data movement.
# MAGIC
# MAGIC | Benefit | What you get |
# MAGIC |---|---|
# MAGIC | **Unified governance** | UC permissions, lineage, and audit logs apply to your Lakebase data the same way they do to lakehouse data. One control plane. |
# MAGIC | **Cross-source queries** | Query Delta and Lakebase together from a single SQL interface — combine transactional and analytical data in one statement. |
# MAGIC | **Centralized discovery** | Browse Lakebase schemas, tables, and views in Catalog Explorer alongside everything else in the workspace. |
# MAGIC | **Integrated workflows** | Reach Lakebase data from dashboards, AI/BI, and apps without standing up a separate connection or driver. |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## How Registration Works
# MAGIC
# MAGIC Registration creates a **read-only Unity Catalog catalog that mirrors your Postgres database
# MAGIC structure** — schemas, tables, and views become visible to UC's metadata layer. Queries
# MAGIC still execute against live Lakebase compute; UC handles authorization, discovery, and
# MAGIC governance on top.
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │           Unity Catalog                │
# MAGIC │   (governance, discovery, auditing)    │
# MAGIC │                                        │
# MAGIC │  catalog: lakebase_datacart  (read-only)
# MAGIC │   └── schema: ecommerce               │
# MAGIC │        └── tables: customers, orders…  │
# MAGIC └─────────────┬──────────────────────────┘
# MAGIC               │ predicate pushdown + live read
# MAGIC               ▼
# MAGIC ┌────────────────────────────────────────┐
# MAGIC │      Lakebase Postgres (live OLTP)     │
# MAGIC └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC - **Where it's initiated:** Catalog Explorer (the Lakehouse workspace), not the Lakebase project page.
# MAGIC - **What's created:** a UC catalog backed by an internal connection that uses your Databricks identity (OAuth user-to-machine) to authenticate against Lakebase on every query.
# MAGIC - **Where queries run:** the SQL warehouse plans the query, pushes predicates down to the Lakebase endpoint, and reads live results.
# MAGIC - **What's mirrored:** schema/table metadata. The data itself stays in Postgres — there's no copy.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Permissions and Access Control
# MAGIC
# MAGIC Two permission layers operate independently — UC governs reads through warehouses, Postgres
# MAGIC governs direct database connections.
# MAGIC
# MAGIC | Layer | Controls | How to grant |
# MAGIC |---|---|---|
# MAGIC | **Unity Catalog** | Who can browse / query the foreign catalog from a SQL warehouse, notebook, or dashboard | The user who registers the catalog becomes its owner. Grant others `USE CATALOG` + `SELECT` to let them query. Metastore admins can manage all registered catalogs. |
# MAGIC | **Postgres roles** | Who can connect directly to the Lakebase database via the Postgres wire protocol (`psycopg`, `psql`, the storefront app's SP) | The Postgres roles and `GRANT` statements you set up in Lab 2.1. Independent from UC. |
# MAGIC
# MAGIC > **Key distinction.** UC permissions only protect the federated read path. If you grant a
# MAGIC > user `SELECT` on the foreign catalog but they have no Postgres role grants, queries through
# MAGIC > UC still work because UC connects with the *registrar's* identity, not the requester's. To
# MAGIC > prevent direct OLTP access, manage Postgres roles in Lab 2.1's pattern.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Concept: Lakehouse Federation vs. Synced Tables
# MAGIC
# MAGIC Both let UC consumers see Lakebase data, but they have very different semantics:
# MAGIC
# MAGIC | Aspect | Synced Tables (Lab 3.1) | Foreign Catalog (this lab) |
# MAGIC |---|---|---|
# MAGIC | **Direction of data movement** | UC → Lakebase | Read-through; no movement |
# MAGIC | **Data freshness** | As fresh as the sync mode (snapshot / triggered / continuous) | Always live (queries hit Lakebase compute directly) |
# MAGIC | **Where the query runs** | Postgres compute on the Lakebase project | SQL warehouse pushes predicates down to Lakebase |
# MAGIC | **Best for** | Powering low-latency app reads from analytics-curated data | Ad-hoc analytical queries against live OLTP, joins with Delta |
# MAGIC | **Cost profile** | Sync compute + Postgres storage of the copy | Lakebase R/W compute usage on every query |
# MAGIC | **Write capability** | Read-only on the Lakebase side | Read-only on the UC side |
# MAGIC
# MAGIC **The data-engineer test:** if the question is *"can analysts run dashboards on live order data
# MAGIC without spinning up an ETL pipeline?"*, federation is the right tool. If the question is
# MAGIC *"can the storefront app serve a curated promo list with sub-10ms latency?"*, that's a synced
# MAGIC table.
# MAGIC
# MAGIC > **Heads up:** UC foreign catalogs registered from Lakebase are **read-only** through Unity
# MAGIC > Catalog. Writes still go through the Postgres protocol, the storefront app, or your sync
# MAGIC > pipelines. UC governs reads.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Bundle-deployed Lakebase project (datacart-storefront/databricks.yml)
# Project name is auto-derived per user from ${workspace.current_user.id}
project_name = f"lakebase-workshop-{w.current_user.me().id}"

# Unity Catalog targets — adjust to your workspace
UC_CATALOG = "main"           # the catalog where we'll create the Delta marketing_campaigns table
UC_SCHEMA = "datacart_demo"   # schema within UC_CATALOG
FOREIGN_CATALOG = "lakebase_datacart"   # the foreign catalog name we'll register
CONNECTION_NAME = "lakebase_datacart_conn"

print(f"User:             {w.current_user.me().user_name}")
print(f"Lakebase project: {project_name}")
print(f"Foreign catalog:  {FOREIGN_CATALOG}")
print(f"UC schema:        {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register the Foreign Catalog (Catalog Explorer UI)
# MAGIC
# MAGIC We'll use the Databricks UI to register the Lakebase project as a Unity Catalog foreign
# MAGIC catalog. This is the cleanest path — UC creates the underlying connection and catalog in
# MAGIC one go and uses your own Databricks identity for authentication (OAuth user-to-machine).
# MAGIC
# MAGIC > **Docs:** [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc)
# MAGIC
# MAGIC ### Steps in the Databricks UI
# MAGIC
# MAGIC 1. Use the **apps switcher** in the top-left of the workspace to switch to the **Lakehouse** workspace if you aren't already there.
# MAGIC 2. Open **Catalog Explorer** from the sidebar.
# MAGIC 3. Click the **+** icon next to the catalog list and select **Create a catalog**.
# MAGIC 4. In the dialog:
# MAGIC    - **Catalog name**: `lakebase_datacart`
# MAGIC    - **Type**: select **Lakebase Postgres**
# MAGIC    - **Compute**: select **Autoscaling**
# MAGIC    - **Project**: pick your workshop project (`lakebase-workshop-<FirstName>-<LastName>`)
# MAGIC    - **Branch**: `production`
# MAGIC    - **Postgres database**: `databricks_postgres`
# MAGIC 5. Click **Create**.
# MAGIC
# MAGIC The new catalog appears in Catalog Explorer alongside your other UC catalogs.
# MAGIC
# MAGIC > **Read-only by design.** The foreign catalog is read-only through Unity Catalog — writes
# MAGIC > still go through Postgres protocol or the storefront app. UC governs reads (tags, lineage,
# MAGIC > grants, audit logs) on the live OLTP data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Register the foreign catalog programmatically
# MAGIC
# MAGIC The UI flow above is the easiest way to do it once, but you'll want a code-based path for
# MAGIC CI/CD or repeatable demos. The cell below uses SQL DDL (executed via `spark.sql`) to do the
# MAGIC same thing the Catalog Explorer dialog did — create a UC connection backed by your Lakebase
# MAGIC project, then create the foreign catalog from it.
# MAGIC
# MAGIC > **Pick one path or the other** — don't run both, or you'll get "already exists" errors
# MAGIC > (the `IF NOT EXISTS` guards below make re-runs safe regardless).
# MAGIC
# MAGIC > **Why two DDL statements?** UC foreign catalogs always sit on top of a *connection*. The
# MAGIC > UI hides this — you fill in a single dialog and UC creates both behind the scenes.

# COMMAND ----------

# Look up the production endpoint's hostname for your bundle-deployed Lakebase project.
# We need this for the CREATE CONNECTION DDL.
prod_branch = next(
    b for b in w.postgres.list_branches(parent=f"projects/{project_name}")
    if b.status and b.status.default
)
pg_endpoint = next(iter(w.postgres.list_endpoints(parent=prod_branch.name)))
pg_host = pg_endpoint.status.hosts.host

print(f"Lakebase host: {pg_host}")
print(f"Connection:    {CONNECTION_NAME}")
print(f"Catalog:       {FOREIGN_CATALOG}")

# COMMAND ----------

# Create the UC connection (backed by Lakebase, OAuth user-to-machine auth).
spark.sql(f"""
    CREATE CONNECTION IF NOT EXISTS {CONNECTION_NAME}
    TYPE postgresql
    OPTIONS (
      host '{pg_host}',
      port '5432',
      database 'databricks_postgres',
      auth_type 'OAUTH_USER_TO_MACHINE'
    )
    COMMENT 'Lakebase Autoscaling project: {project_name} (Lab 4.1)'
""")
print(f"✅ Connection '{CONNECTION_NAME}' ready")

# Create the foreign catalog backed by that connection.
spark.sql(f"""
    CREATE FOREIGN CATALOG IF NOT EXISTS {FOREIGN_CATALOG}
    USING CONNECTION {CONNECTION_NAME}
    OPTIONS (database 'databricks_postgres')
    COMMENT 'Live foreign catalog into the {project_name} Lakebase project'
""")
print(f"✅ Foreign catalog '{FOREIGN_CATALOG}' ready")
print(f"\nQuery it from any SQL warehouse: SELECT * FROM {FOREIGN_CATALOG}.ecommerce.orders LIMIT 5;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Smoke-Test — Live Query Against Lakebase
# MAGIC
# MAGIC Now switch to the **SQL Editor** (sidebar → **SQL Editor**) and connect to a serverless SQL
# MAGIC warehouse. Paste the query below. You should see the same `orders` rows you'd see if you
# MAGIC logged into the Postgres instance directly — it's a real-time read with no copy or
# MAGIC replication lag.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_id, customer_id, status, total
# MAGIC FROM lakebase_datacart.ecommerce.orders
# MAGIC ORDER BY order_id
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: The Value Moment — Federated Join with Delta Marketing Data
# MAGIC
# MAGIC This is the scenario data engineers care about. We have:
# MAGIC - **Live OLTP orders** in Lakebase (federated as `lakebase_datacart.ecommerce.orders`)
# MAGIC - **Curated marketing campaigns** in Delta (we'll create this in a moment)
# MAGIC
# MAGIC Without federation you'd have to ETL one into the other before joining. With federation, the
# MAGIC SQL warehouse pushes the predicate down to Lakebase, pulls back only the matching rows, and
# MAGIC joins them against Delta in-place.
# MAGIC
# MAGIC ### Pick a target catalog for the demo Delta table
# MAGIC
# MAGIC The federated query joins live OLTP data with a small Delta table you'll stage in Unity
# MAGIC Catalog. Pick a UC catalog you have `CREATE SCHEMA` privileges on (`main` is the most common
# MAGIC default; in workshops you may have a per-user catalog). Set it once below — every SQL block
# MAGIC in this step uses it.

# COMMAND ----------

# MAGIC %md
# MAGIC **➡ Set your target catalog here, then re-run this cell.** All later queries reference `MY_CATALOG`.

# COMMAND ----------

# Edit this value to match the UC catalog you want to use for the demo Delta table.
MY_CATALOG = "main"

# Derived names — usually you don't need to change these.
DEMO_SCHEMA  = f"{MY_CATALOG}.datacart_demo"
CAMPAIGNS_TABLE = f"{DEMO_SCHEMA}.marketing_campaigns"

print(f"Target catalog: {MY_CATALOG}")
print(f"Demo schema:    {DEMO_SCHEMA}")
print(f"Campaigns:      {CAMPAIGNS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC In the **SQL Editor**, run the following two queries in order. Replace `<MY_CATALOG>` with
# MAGIC the catalog name you set above before pasting.
# MAGIC
# MAGIC ### 4a. Stage a Delta table of marketing campaigns
# MAGIC
# MAGIC ```sql
# MAGIC -- Stage a tiny Delta table representing marketing-team-curated campaigns.
# MAGIC CREATE SCHEMA IF NOT EXISTS <MY_CATALOG>.datacart_demo;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE <MY_CATALOG>.datacart_demo.marketing_campaigns (
# MAGIC   campaign        STRING,
# MAGIC   utm_source      STRING,
# MAGIC   start_date      DATE,
# MAGIC   end_date        DATE,
# MAGIC   target_segment  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO <MY_CATALOG>.datacart_demo.marketing_campaigns VALUES
# MAGIC   ('Spring Kickoff',     'spring_email',  DATE '2026-03-01', DATE '2026-04-01', 'returning'),
# MAGIC   ('Loyalty Push',       'loyalty_push',  DATE '2026-04-01', DATE '2026-05-15', 'loyalty'),
# MAGIC   ('Cross-Channel Demo', 'paid_social',   DATE '2026-04-15', DATE '2026-06-01', 'new');
# MAGIC ```
# MAGIC
# MAGIC ### 4b. Run the federated join
# MAGIC
# MAGIC > **Note:** The orders table created in Lab 1.1 doesn't have a `utm_source` column by default.
# MAGIC > For this demo we join on `customer_id` modulo the campaign array length so every order maps
# MAGIC > to a campaign. In a real setting, an `orders.utm_source` column populated by the storefront
# MAGIC > would replace this. The point is the *shape* of the query — federated join in one statement.
# MAGIC
# MAGIC ```sql
# MAGIC WITH live_orders AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     total,
# MAGIC     status
# MAGIC   FROM lakebase_datacart.ecommerce.orders
# MAGIC ),
# MAGIC campaigns_indexed AS (
# MAGIC   SELECT campaign, utm_source, ROW_NUMBER() OVER (ORDER BY campaign) - 1 AS idx,
# MAGIC          (SELECT COUNT(*) FROM <MY_CATALOG>.datacart_demo.marketing_campaigns) AS n
# MAGIC   FROM <MY_CATALOG>.datacart_demo.marketing_campaigns
# MAGIC )
# MAGIC SELECT
# MAGIC   c.campaign,
# MAGIC   c.utm_source,
# MAGIC   COUNT(o.order_id)            AS attributed_orders,
# MAGIC   ROUND(SUM(o.total), 2)       AS attributed_revenue
# MAGIC FROM live_orders o
# MAGIC JOIN campaigns_indexed c
# MAGIC   ON (o.customer_id % c.n) = c.idx
# MAGIC GROUP BY c.campaign, c.utm_source
# MAGIC ORDER BY attributed_revenue DESC;
# MAGIC ```
# MAGIC
# MAGIC > **Tip:** in the SQL Editor you can run `USE CATALOG <your-catalog>;` once at the top, then
# MAGIC > drop the catalog prefix from every reference (e.g. just `datacart_demo.marketing_campaigns`).

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened:**
# MAGIC - The SQL warehouse parsed the join.
# MAGIC - It pushed a `SELECT order_id, customer_id, total, status FROM ecommerce.orders` down to Lakebase.
# MAGIC - Lakebase returned the live OLTP rows.
# MAGIC - The warehouse joined them against the Delta `marketing_campaigns` table in-place.
# MAGIC
# MAGIC No ETL. No staleness. No second copy of the data. The lakehouse and the OLTP database
# MAGIC are queryable as one logical surface.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations
# MAGIC
# MAGIC Worth keeping in mind for production planning:
# MAGIC
# MAGIC | Limitation | What it means | Workaround |
# MAGIC |---|---|---|
# MAGIC | **Read-only through UC** | You can't `INSERT` / `UPDATE` / `DELETE` against the foreign catalog from a SQL warehouse | Writes go through Postgres protocol, the storefront app, or a Synced Tables / Lakehouse Sync pipeline (Labs 3.1 / 5.1) |
# MAGIC | **One Postgres database per catalog** | Each foreign catalog represents a single Lakebase database | Register additional databases as separate catalogs |
# MAGIC | **Metadata is cached** | UC caches schema metadata to reduce Postgres traffic — newly created Lakebase tables may not appear immediately | Refresh the catalog from Catalog Explorer or wait for the next cache refresh |
# MAGIC | **Branch-scoped registration** | A registered catalog points at a specific Lakebase branch | Register each branch you want to expose as its own UC catalog |
# MAGIC
# MAGIC None of these block the workshop scenarios — they're things to plan for when designing a
# MAGIC production federation strategy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - You created a UC connection that authenticates with **your own Databricks identity** —
# MAGIC   no shared secrets, no static passwords.
# MAGIC - You registered Lakebase as a UC foreign catalog so any SQL warehouse / serverless SQL /
# MAGIC   notebook can query OLTP data with the same identity model as Delta.
# MAGIC - You ran a federated join: live OLTP × Delta marketing data, in one statement, with no ETL.
# MAGIC - You now have a working mental model for **when to federate vs. when to sync** — the next
# MAGIC   lab (5.1 Lakehouse Sync) is where you'll see the analytics-throughput end of that decision.
