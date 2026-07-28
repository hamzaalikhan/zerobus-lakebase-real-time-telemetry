# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.3: Sync Demand Back to Lakebase
# MAGIC
# MAGIC This is the **present** step — the final movement of the real-time analytics loop:
# MAGIC
# MAGIC | Lab | Step | What it does |
# MAGIC |---|---|---|
# MAGIC | 4.1 | collect | Landed the raw clickstream in bronze |
# MAGIC | 4.2 | aggregate | Built the gold `product_demand` table with a Lakeflow pipeline |
# MAGIC | **4.3 (this lab)** | **present** | Sync `product_demand` back to Lakebase so the storefront's **Supplier View** shows it |
# MAGIC
# MAGIC This reuses exactly the **synced-table** mechanism from Lab 2.1 — just in the same direction
# MAGIC (UC → Lakebase). We push the small gold table into the production Lakebase branch, and the
# MAGIC storefront's Supplier Demand View reads it. The app does no aggregation; all the demand math ran
# MAGIC in the governed lakehouse pipeline, and only the small result is served back.
# MAGIC
# MAGIC ```
# MAGIC product_demand (gold, UC) ──synced table──▶ Lakebase ecommerce.product_demand_synced_prod
# MAGIC                                                          │
# MAGIC                                                          ▼
# MAGIC                                            Supplier Demand View (/supplier)
# MAGIC ```
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Create** a synced table from the gold `product_demand` table into Lakebase
# MAGIC 2. **Grant** the storefront's service principal read access to the synced table
# MAGIC 3. **See** the Supplier Demand View light up — with zero application code changes
# MAGIC
# MAGIC > **Prerequisite:** Lab 4.2 (which created `product_demand` in your catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog name")
dbutils.widgets.text("schema", "", "2. Schema name")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()

project_name = f"zerobus-lakebase-{w.current_user.me().id}"
db_schema = "ecommerce"
db_user = w.current_user.me().user_name

print(f"Gold table:       {UC_CATALOG}.{UC_SCHEMA}.product_demand")
print(f"Lakebase project: {project_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Synced Table (UI)
# MAGIC
# MAGIC Push the gold `product_demand` table into the production Lakebase branch so the storefront's
# MAGIC **Supplier Demand View** can read it.
# MAGIC
# MAGIC **In the Databricks UI:**
# MAGIC
# MAGIC 1. **Catalog** → your catalog → `ecommerce` → `product_demand`
# MAGIC 2. **Create** → **Synced table**
# MAGIC 3. In the dialog:
# MAGIC    - **Table name**: `product_demand_synced_prod`
# MAGIC    - **Database type**: **Lakebase Serverless (Autoscaling)**
# MAGIC    - **Project**: your workshop project (`zerobus-lakebase-...`)
# MAGIC    - **Branch**: **production**
# MAGIC    - **Sync mode**: **Snapshot** (simplest; the gold table is small)
# MAGIC    - **Primary key**: `product_id`
# MAGIC 4. **Create**, and wait for the sync to complete.
# MAGIC
# MAGIC > This is the same synced-table flow as Lab 2.1 (promotions). The mechanism is identical — only
# MAGIC > the source table differs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Grant the Storefront SP Access to the Synced Table
# MAGIC
# MAGIC Just like Lab 2.1: synced tables are created by the Lakebase sync pipeline (a different role), so
# MAGIC re-grant table access to the app's SP so the Supplier View can read `product_demand_synced_prod`.

# COMMAND ----------

import psycopg2

APP_NAME = f"storefront-{w.current_user.me().id}"
SP_CLIENT_ID = w.apps.get(APP_NAME).service_principal_client_id

# Connect to the production branch as the project owner.
prod_branch = next(
    b for b in w.postgres.list_branches(parent=f"projects/{project_name}")
    if b.status and b.status.default
)
endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
prod_host = endpoints[0].status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
conn = psycopg2.connect(host=prod_host, port=5432, dbname="databricks_postgres",
                        user=db_user, password=cred.token, sslmode="require")
conn.autocommit = True

with conn.cursor() as cur:
    sp = f'"{SP_CLIENT_ID}"'
    cur.execute(f"GRANT USAGE ON SCHEMA {db_schema} TO {sp};")
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {db_schema} TO {sp};")
print(f"✅ Granted the storefront SP read access on {db_schema} (includes product_demand_synced_prod)")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: The Supplier Demand View Goes Live
# MAGIC
# MAGIC Open the storefront's **Supplier Demand View** at **`/supplier`** (append it to your app URL).
# MAGIC Within ~30 seconds of the sync completing you'll see the aggregated demand table — views, clicks,
# MAGIC add-to-cart, cart rate, units sold, and a restock flag per product.
# MAGIC
# MAGIC ```
# MAGIC clickstream (4.1) ──▶ medallion pipeline (4.2) ──▶ product_demand (gold)
# MAGIC                                                          │ synced table (4.3)
# MAGIC                                                          ▼
# MAGIC                                            Supplier Demand View (/supplier)
# MAGIC ```
# MAGIC
# MAGIC > The storefront's Supplier View auto-detects a table named `product_demand*` in the `ecommerce`
# MAGIC > schema (see `server/schema_detector.get_demand_table`), so once the sync lands, the view lights
# MAGIC > up with **no app redeploy**.
# MAGIC
# MAGIC **Key insight:** The app never aggregated anything. The lakehouse did the demand math (Photon,
# MAGIC governed, joined with orders and inventory), and only the small aggregated result was synced back
# MAGIC for serving. That's the full **collect → aggregate → present** loop on one platform.
# MAGIC
# MAGIC ## Done
# MAGIC
# MAGIC You closed the loop: raw clickstream → medallion aggregation → synced back to Lakebase → served
# MAGIC in the storefront.
# MAGIC
# MAGIC **Next:** Lab 5.1 — Zerobus, the serverless push API that would stream this clickstream in live
# MAGIC (instead of seeding it), and what happens when data doesn't match the table's schema.
