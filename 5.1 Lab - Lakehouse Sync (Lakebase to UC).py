# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.1: Lakehouse Sync — Lakebase to Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Outbound — Streaming OLTP into Delta for Analytics
# MAGIC
# MAGIC This is the third movement in the data-flow story. Together, the three labs map every direction
# MAGIC of data movement between Lakebase and the lakehouse:
# MAGIC
# MAGIC | Direction | Lab | Mechanism | Best for |
# MAGIC |---|---|---|---|
# MAGIC | UC → Lakebase | 3.1 | Synced Tables | Serving Lakehouse data to apps |
# MAGIC | Live read-through | 4.1 | UC foreign catalog (federation) | Ad-hoc joins, governed reads |
# MAGIC | **Lakebase → UC** | **5.1 (this lab)** | **Lakehouse Sync** | **High-throughput analytics on OLTP data** |
# MAGIC
# MAGIC In this lab you'll set up Lakehouse Sync so the live `orders`, `customers`, and `order_items`
# MAGIC tables in Lakebase are continuously mirrored as Delta tables in Unity Catalog. Once that's
# MAGIC running, BI dashboards, ML pipelines, and ad-hoc analytical queries can hit Delta — getting
# MAGIC full lakehouse performance — without putting any load on the OLTP database that powers the
# MAGIC storefront.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what Lakehouse Sync is and how it complements Synced Tables (Lab 3.1) and
# MAGIC    federation (Lab 4.1)
# MAGIC 2. **Create** a Lakehouse Sync configuration that mirrors Lakebase tables to UC Delta
# MAGIC 3. **Trigger** the initial snapshot and verify Delta tables appear in UC
# MAGIC 4. **Demonstrate** end-to-end propagation by inserting a row in Lakebase and observing it in Delta
# MAGIC 5. **Run** an analytics query on the Delta-side data — "OLTP analytics without OLTP load"
# MAGIC
# MAGIC **Available as a beta feature on AWS but will be on AZURE soon!**
# MAGIC
# MAGIC > **Docs**: [Lakehouse Sync](https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Concept: Why Sync OLTP into Delta?
# MAGIC
# MAGIC Federation (Lab 4.1) is great for **live, low-volume** queries. But if a BI dashboard scans
# MAGIC every order from the last 30 days and you have a million orders, federation will:
# MAGIC
# MAGIC - Saturate the Lakebase compute (which is also serving the storefront)
# MAGIC - Run slowly because OLTP storage is row-oriented, not columnar
# MAGIC - Cost the same as serving live application traffic
# MAGIC
# MAGIC **Lakehouse Sync** addresses this by keeping a continuously-updated Delta replica of your
# MAGIC OLTP tables in Unity Catalog. Analytics queries hit Delta — columnar storage, photon
# MAGIC acceleration, and zero contention with the storefront.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────┐                  ┌─────────────────────────────────┐
# MAGIC │   Lakebase (production) │                  │      Unity Catalog (Delta)       │
# MAGIC │  ─────────────────────  │  Lakehouse Sync  │  ──────────────────────────────  │
# MAGIC │   ecommerce.orders      │ ──────────────▶ │  <your-catalog>.datacart_uc.orders │
# MAGIC │   ecommerce.customers   │       CDC        │  <your-catalog>.datacart_uc.customers│
# MAGIC │   ecommerce.order_items │                  │  <your-catalog>.datacart_uc.order_items│
# MAGIC │                         │                  │                                  │
# MAGIC │  Storefront (writes)    │                  │  BI / dashboards / ML (reads)    │
# MAGIC └─────────────────────────┘                  └─────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC The OLTP side keeps the storefront happy. The Delta side absorbs heavy analytical scans.
# MAGIC Both stay in sync via Lakebase's managed CDC.

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

# Bundle-deployed Lakebase project
project_name = f"lakebase-workshop-{w.current_user.me().id}"

# Where the synced Delta tables will land
# UC_CATALOG = "<<add your catalog>>"
UC_CATALOG = "serverless_stable_339b90_catalog"
UC_SCHEMA = "datacart_uc"
TABLES_TO_SYNC = ["orders", "customers", "order_items"]

print(f"User:             {w.current_user.me().user_name}")
print(f"Lakebase project: {project_name}")
print(f"Sync target:      {UC_CATALOG}.{UC_SCHEMA}")
print(f"Tables:           {', '.join(TABLES_TO_SYNC)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare the Target UC Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
print(f"✅ Schema {UC_CATALOG}.{UC_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set `REPLICA IDENTITY FULL` on Source Tables
# MAGIC
# MAGIC Lakehouse Sync uses Postgres logical replication to capture row-level changes. For
# MAGIC `UPDATE`s and `DELETE`s to be replicated correctly, each source table needs its
# MAGIC **replica identity** set to `FULL` — that tells Postgres to log the entire old row in the
# MAGIC WAL (write-ahead log), not just the primary key.
# MAGIC
# MAGIC Without this, Lakehouse Sync **silently skips the table**:
# MAGIC > Tables without REPLICA IDENTITY FULL will be skipped. Run ALTER TABLE ... REPLICA IDENTITY FULL to include them.
# MAGIC
# MAGIC We run the `ALTER TABLE` once per table before configuring the sync. It's idempotent — safe
# MAGIC to re-run.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

import psycopg2

# Connect to the Lakebase production branch as the project owner.
prod_branch_obj = next(
    b for b in w.postgres.list_branches(parent=f"projects/{project_name}")
    if b.status and b.status.default
)
prod_endpoint = next(iter(w.postgres.list_endpoints(parent=prod_branch_obj.name)))
pg_host = prod_endpoint.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=prod_endpoint.name)

owner_conn = psycopg2.connect(
    host=pg_host,
    port=5432,
    database="databricks_postgres",
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode="require",
)
owner_conn.autocommit = True

with owner_conn.cursor() as cur:
    for table in TABLES_TO_SYNC:
        cur.execute(f"ALTER TABLE ecommerce.{table} REPLICA IDENTITY FULL;")
        print(f"✅ ecommerce.{table}: REPLICA IDENTITY FULL")

    # Verify
    cur.execute("""
        SELECT n.nspname AS schema, c.relname AS table,
               CASE c.relreplident
                 WHEN 'd' THEN 'default (primary key)'
                 WHEN 'n' THEN 'nothing'
                 WHEN 'f' THEN 'full'
                 WHEN 'i' THEN 'index'
               END AS replica_identity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ecommerce' AND c.relname = ANY(%s)
        ORDER BY c.relname
    """, (TABLES_TO_SYNC,))
    print("\n📋 Replica identity per table:")
    for row in cur.fetchall():
        print(f"   {row[0]}.{row[1]:<15} → {row[2]}")

owner_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create the Lakehouse Sync Configuration
# MAGIC
# MAGIC Lakehouse Sync is configured at the project level. The cleanest way to set it up is via the
# MAGIC Databricks UI — that's what we'll walk through here. (You can also do this via the SDK /
# MAGIC REST API; see the docs link above.)
# MAGIC
# MAGIC ### UI walkthrough
# MAGIC
# MAGIC 1. Open **Catalog Explorer** in the sidebar.
# MAGIC 2. Navigate to your Lakebase project: **Lakebase Postgres** → `lakebase-workshop-<FirstName>-<LastName>`
# MAGIC 3. Click the **production** branch
# MAGIC 4. In the branch overview page, click **Lakehouse Sync** button
# MAGIC 5. Click the start sync button on the right side of the screen
# MAGIC 6. Fill out the dialog box that pops up:
# MAGIC    - Pick the right **Sync mode:** *Continuous* (or *Triggered* if you'd rather control when sync runs)
# MAGIC 7. Click **Create sync**. The pipeline provisions in ~1 minute and immediately runs an
# MAGIC    initial snapshot.
# MAGIC
# MAGIC > **Permissions:** the project owner (you) automatically has the rights to create the sync.
# MAGIC > In team setups, the project owner grants `CAN_USE` on the project to whoever is operating
# MAGIC > the sync pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify the Initial Snapshot Landed in Delta
# MAGIC
# MAGIC Navigate to the target location and view the tables now synced from Lakebase into Lakehouse!

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Happens When Lakebase Schemas Change?
# MAGIC
# MAGIC In Labs 6.2 (Schema Migration) and 6.3 (Branch Reset) you'll add new columns to `customers`
# MAGIC and `orders` on the Lakebase side. Lakehouse Sync handles schema evolution:
# MAGIC
# MAGIC - **New columns** appear in the Delta tables on the next sync cycle.
# MAGIC - **Dropped columns** stop being written; existing Delta history is preserved.
# MAGIC - **Renamed columns** are treated as drop + add; rename through migration tooling explicitly
# MAGIC   to avoid that.
# MAGIC
# MAGIC In Lab 6.2, after applying the migration, come back and re-query the synced `customers` table
# MAGIC under `<your-catalog>.datacart_uc` — the new `loyalty_points` column will appear in Delta with
# MAGIC no extra work on your side.
# MAGIC
# MAGIC In Lab 7.1 (PITR), if you DROP `orders` on Lakebase, the sync pipeline pauses and reports an
# MAGIC error. After PITR recovery, restart the pipeline if it gave up. The Delta side stays
# MAGIC consistent with the post-recovery Lakebase state.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Federation vs. Lakehouse Sync — Decision Notes
# MAGIC
# MAGIC | Use federation when… | Use Lakehouse Sync (this lab) when… |
# MAGIC |---|---|
# MAGIC | The query is ad-hoc or low-frequency | The query runs many times per minute on the same data |
# MAGIC | The data must be live to the millisecond | Hourly or near-real-time freshness is acceptable |
# MAGIC | You need governance (UC tags, lineage) on read | You need to run heavy aggregations without OLTP load |
# MAGIC | The join touches small slices of OLTP | The query scans large fractions of OLTP tables |
# MAGIC | You want zero pipeline overhead | You can pay for a sync pipeline to amortize cost |
# MAGIC
# MAGIC In production, most data-centric teams use **both**: federation for live spot-checks /
# MAGIC governed read APIs, and Lakehouse Sync for high-throughput analytical workloads. The next
# MAGIC lab covers the latter.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Summary
# MAGIC
# MAGIC - You set up a Lakehouse Sync that mirrors three Lakebase tables to Delta in UC.
# MAGIC - You verified an end-to-end write path: storefront-style insert in Lakebase → Delta replica.
# MAGIC - You ran a customer-LTV aggregation against Delta — exactly the workload you don't want
# MAGIC   running directly on the OLTP database.
# MAGIC - You now have a complete picture of all three Lakebase ↔ Lakehouse data movements:
# MAGIC   inbound (Synced Tables, Lab 3.1), live (Federation, Lab 4.1), outbound (this lab).
# MAGIC
# MAGIC With those three primitives in your toolkit, the rest of the workshop (branching, schema
# MAGIC migration, PITR) is about safely *evolving* the OLTP side while these data flows continue
# MAGIC to operate. That's where we go next.

# COMMAND ----------

conn.close()
