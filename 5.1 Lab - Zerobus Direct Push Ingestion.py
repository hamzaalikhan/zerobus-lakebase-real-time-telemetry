# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.1: Zerobus — Live Clickstream from the App
# MAGIC
# MAGIC In Labs 4.1–4.3 we *seeded* a clickstream so the medallion pipeline had data. In the real world
# MAGIC that stream arrives **live** from the application. **Zerobus** is how it gets there: a serverless
# MAGIC **push ingestion API** that writes records straight into a Unity-Catalog Delta table — no Kafka,
# MAGIC no Kinesis, no message bus to run.
# MAGIC
# MAGIC The DataCart storefront already has a Zerobus producer built in (`server/zerobus_producer.py`) —
# MAGIC it's just **off by default**. In this lab you'll create the target table, grant the app access,
# MAGIC turn the producer on with a redeploy, click around the storefront, and watch **real** shopper
# MAGIC events land in Delta.
# MAGIC
# MAGIC ```
# MAGIC  Shopper clicks in the storefront
# MAGIC         │  server/zerobus_producer.py  →  emit(event_type, product_id)
# MAGIC         ▼
# MAGIC  Zerobus (serverless push)  ──▶  clickstream_live (Delta, Unity Catalog)
# MAGIC ```
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Explain** what Zerobus is and when to reach for it
# MAGIC 2. **Create** the Delta target table and grant the app's service principal write access
# MAGIC 3. **Enable** the storefront's Zerobus producer with a redeploy
# MAGIC 4. **Verify** live shopper events landing in Delta as you use the storefront

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Zerobus?
# MAGIC
# MAGIC **Zerobus** is a serverless **push ingestion API** for Databricks. A producer pushes records over
# MAGIC a lightweight stream and they land **directly in a Unity-Catalog-managed Delta table**.
# MAGIC
# MAGIC | Property | What it means |
# MAGIC |---|---|
# MAGIC | **Serverless push** | The app just calls `stream.ingest_record_offset(...)`. No infra to run. |
# MAGIC | **Writes to Delta directly** | Records land in a governed, versioned, joinable Delta table. |
# MAGIC | **Table must pre-exist** | Zerobus does **not** create tables. You define the schema up front (this lab). |
# MAGIC | **At-least-once delivery** | Duplicates are possible under retries — dedup downstream (e.g. in a silver layer). |
# MAGIC | **Region-gated** | Producer and target table must be in the **same region**; the endpoint URL is region-specific. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC Set the same **catalog** / **schema** you've used throughout, plus your workspace **region** (used
# MAGIC to build the region-specific Zerobus endpoint the app will push to).

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog name")
dbutils.widgets.text("schema", "", "2. Schema name")
dbutils.widgets.text("region", "", "3. Workspace region (e.g. us-west-2)")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()
ZEROBUS_REGION = dbutils.widgets.get("region").strip()

LIVE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.clickstream_live"

workspace_url = w.config.host.rstrip("/")
workspace_id = w.get_workspace_id()
ZEROBUS_ENDPOINT = f"https://{workspace_id}.zerobus.{ZEROBUS_REGION}.cloud.databricks.com"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

print(f"Live table:       {LIVE_TABLE}")
print(f"Zerobus endpoint: {ZEROBUS_ENDPOINT}")
print(f"Workspace URL:    {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Target Table
# MAGIC
# MAGIC Zerobus won't create the table — we define it first, and its schema must match **exactly** what
# MAGIC the producer sends. The storefront's producer (`server/zerobus_producer.py`) emits one JSON record
# MAGIC per shopper action with these three fields:
# MAGIC
# MAGIC | Column | Type | Meaning |
# MAGIC |---|---|---|
# MAGIC | `event_type` | STRING | `view`, `click`, or `add_to_cart` |
# MAGIC | `product_id` | INT | Which product the event is about |
# MAGIC | `event_ts` | STRING | Client-side ISO-8601 timestamp |
# MAGIC
# MAGIC Example payload the app pushes:
# MAGIC
# MAGIC ```json
# MAGIC {"event_type": "add_to_cart", "product_id": 14, "event_ts": "2026-07-29T10:15:42.123456+00:00"}
# MAGIC ```
# MAGIC
# MAGIC > **Why `event_ts` as STRING, not TIMESTAMP?** With JSON ingest, Zerobus passes the value through
# MAGIC > as text — a `TIMESTAMP` column would reject an ISO-8601 string. Landing raw text and casting
# MAGIC > later (in a silver layer) is the standard medallion pattern. **The table schema is a contract:**
# MAGIC > it must line up with the producer's payload, or ingestion fails.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {LIVE_TABLE}")
spark.sql(f"""
    CREATE TABLE {LIVE_TABLE} (
        event_type  STRING,
        product_id  INT,
        event_ts    STRING
    )
    USING DELTA
    COMMENT 'Live storefront clickstream — Zerobus ingest target (Lab 5.1). Append-only, at-least-once.'
""")
print(f"✅ Created target table: {LIVE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Grant the Storefront App Write Access
# MAGIC
# MAGIC The producer authenticates as the storefront's **service principal** (its OAuth creds are
# MAGIC auto-injected by the Apps runtime). For Zerobus ingest, Unity Catalog requires the SP to hold
# MAGIC `USE CATALOG` + `USE SCHEMA` + `MODIFY` + `SELECT` on the target table.
# MAGIC
# MAGIC > `MODIFY` and `SELECT` must be granted explicitly — `ALL PRIVILEGES` alone is **not** sufficient
# MAGIC > for Zerobus ingest.

# COMMAND ----------

APP_NAME = f"storefront-{w.current_user.me().id}"
SP_CLIENT_ID = w.apps.get(APP_NAME).service_principal_client_id

spark.sql(f"GRANT USE CATALOG ON CATALOG {UC_CATALOG} TO `{SP_CLIENT_ID}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {UC_CATALOG}.{UC_SCHEMA} TO `{SP_CLIENT_ID}`")
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {LIVE_TABLE} TO `{SP_CLIENT_ID}`")
print(f"✅ Granted USE CATALOG + USE SCHEMA + MODIFY + SELECT on {LIVE_TABLE}")
print(f"   to the storefront SP: {SP_CLIENT_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Turn On the Producer (Redeploy)
# MAGIC
# MAGIC The producer is gated by three env vars that are **off/unset** by default. Enable it by
# MAGIC redeploying the bundle with these overrides — no code changes needed. Run the print cell below to
# MAGIC get the exact command filled in with your values, then run it in a terminal from the
# MAGIC `datacart-storefront/` folder.

# COMMAND ----------

print("Run this from the datacart-storefront/ folder (replace <your-profile>):\n")
print("databricks bundle deploy --profile <your-profile> \\")
print(f'  --var="zerobus_enabled=true" \\')
print(f'  --var="zerobus_endpoint={ZEROBUS_ENDPOINT}" \\')
print(f'  --var="zerobus_bronze_table={LIVE_TABLE}"')
print("\nThen push the source onto the running app:\n")
print("databricks bundle run datacart_storefront --profile <your-profile>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What those overrides do
# MAGIC
# MAGIC | Bundle variable | Env var the app reads | Effect |
# MAGIC |---|---|---|
# MAGIC | `zerobus_enabled=true` | `ZEROBUS_ENABLED` | Flips the producer on (default off) |
# MAGIC | `zerobus_endpoint=...` | `ZEROBUS_ENDPOINT` | The region-specific Zerobus server endpoint |
# MAGIC | `zerobus_bronze_table=...` | `ZEROBUS_BRONZE_TABLE` | The Delta table you created in Step 2 |
# MAGIC
# MAGIC The app's `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` are injected
# MAGIC automatically by the Apps runtime — the producer uses them to authenticate the Zerobus stream.
# MAGIC
# MAGIC > The producer is **best-effort**: if anything is misconfigured it logs once and the storefront
# MAGIC > keeps serving normally — a telemetry failure never breaks the shop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Some Clicks
# MAGIC
# MAGIC Open the storefront (its URL is printed below) and **browse around** — view products, open a few
# MAGIC detail pages, add items to the cart. Each of those actions emits a `view` / `click` /
# MAGIC `add_to_cart` event to Zerobus.
# MAGIC
# MAGIC > The producer flushes to Delta every ~25 events (and on shutdown), so give it a little browsing
# MAGIC > before you expect rows to appear.

# COMMAND ----------

print(f"Open the storefront and click around:\n   {w.apps.get(APP_NAME).url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Watch the Events Land
# MAGIC
# MAGIC Re-run this cell after browsing — the counts grow as you generate more clicks. Because Zerobus is
# MAGIC **at-least-once**, you may occasionally see a duplicate; that's expected and is what the silver
# MAGIC layer's dedup (Lab 4.2) handles.

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_type, COUNT(*) AS events
    FROM {LIVE_TABLE}
    GROUP BY event_type
    ORDER BY events DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_type, product_id, event_ts
    FROM {LIVE_TABLE}
    ORDER BY event_ts DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC You created a Delta target, granted the storefront's service principal access, enabled the
# MAGIC built-in Zerobus producer with a redeploy, and watched **real shopper clicks** stream straight
# MAGIC into governed Delta — no message bus, no app rewrite.
# MAGIC
# MAGIC This is the live version of what Lab 4.1 seeded: in production, the same clickstream flows through
# MAGIC the medallion pipeline (Lab 4.2) and back to Lakebase for the Supplier View (Lab 4.3). To feed the
# MAGIC medallion from this live table instead of the seed, point the pipeline's source at
# MAGIC `clickstream_live` and re-run it.
# MAGIC
# MAGIC > **Turning it back off:** redeploy with `--var="zerobus_enabled=false"` (or omit the overrides)
# MAGIC > to return the storefront to its default, producer-off state.
