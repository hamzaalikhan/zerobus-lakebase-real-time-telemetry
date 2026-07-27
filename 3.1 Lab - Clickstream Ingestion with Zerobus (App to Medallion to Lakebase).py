# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.1: Real-Time Clickstream — Zerobus → Medallion → Back to Lakebase
# MAGIC
# MAGIC This lab builds the **collect → aggregate → present** loop, driven by the storefront itself.
# MAGIC The DataCart app emits a live clickstream (product views, clicks, add-to-carts); we push it
# MAGIC straight into a governed Delta table with **Zerobus**, aggregate it into a per-product **demand**
# MAGIC signal with a **Lakeflow** pipeline, and sync that signal back into Lakebase — where a
# MAGIC **Supplier Demand View** in the storefront reads it. No message bus, no app-side aggregation.
# MAGIC
# MAGIC ```
# MAGIC  App (shopper) ──emit view/click/add_to_cart──▶ Zerobus ──▶ clickstream_bronze (Delta, UC)
# MAGIC        ▲                                                            │  Lakeflow (SQL) medallion
# MAGIC        │                                                            ▼
# MAGIC  Supplier View ◀── synced table ◀── product_demand (gold)  ◀── clickstream_silver
# MAGIC ```
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Explain** Zerobus — a serverless push API that writes directly into UC Delta tables
# MAGIC 2. **Generate** a realistic baseline clickstream and ingest it with the Zerobus Python SDK
# MAGIC 3. **Build** a bronze → silver → gold medallion pipeline in **Lakeflow, authored in SQL**
# MAGIC 4. **Sync** the gold `product_demand` table back to Lakebase and light up the Supplier View
# MAGIC
# MAGIC > **Prerequisite:** Lab 1.1 (which created the `clickstream_bronze` Delta table and granted the
# MAGIC > storefront's service principal write access to it). This lab points a producer at that table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Zerobus?
# MAGIC
# MAGIC **Zerobus** is a serverless **push ingestion API** for Databricks. Your application (or any
# MAGIC producer) pushes records over a lightweight gRPC/REST stream and they land **directly in a
# MAGIC Unity-Catalog-managed Delta table** — no Kafka, no Kinesis, no self-managed message bus.
# MAGIC
# MAGIC | Property | What it means for this lab |
# MAGIC |---|---|
# MAGIC | **Serverless push** | The storefront just calls `stream.ingest_record_offset(...)`. No infra to run. |
# MAGIC | **Writes to Delta directly** | The clickstream lands in `clickstream_bronze` — governed, versioned, joinable. |
# MAGIC | **Table must pre-exist** | Zerobus does **not** create tables. We created `clickstream_bronze` in Lab 1.1. |
# MAGIC | **At-least-once delivery** | Duplicates are possible — we **dedup in silver** (this is why the medallion matters). |
# MAGIC | **Region-gated** | Workspace and target table must be in the **same region**; the endpoint URL is region-specific. |
# MAGIC
# MAGIC **Why push clickstream to the lakehouse at all — why not just `GROUP BY` in Postgres?**
# MAGIC Aggregating behavioural data in the OLTP tier makes analytical scans compete with the
# MAGIC transactional storefront queries that must stay fast. Instead we push behavioural events to the
# MAGIC lakehouse, aggregate there (Photon, governance, lineage, cheap Delta joins against orders and
# MAGIC inventory), and sync only the **small aggregated result** back to Lakebase for serving. That's the
# MAGIC "Lakebase + lakehouse, better together" pattern this whole workshop is about.

# COMMAND ----------

# MAGIC %pip install databricks-zerobus-ingest-sdk databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC Set the same Unity Catalog you used in Lab 1.1 for the bronze table. The Lakeflow pipeline will
# MAGIC create silver + gold in the same `ecommerce` schema so everything stays together.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# --- Unity Catalog (lakehouse side) ---
UC_CATALOG = "<add-your-catalog-name-here>"   # SAME catalog as Lab 1.1 Step 8
UC_SCHEMA = "ecommerce"
BRONZE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.clickstream_bronze"

# --- Zerobus (region-specific endpoint) ---
# Endpoint form: https://<workspace-id>.zerobus.<region>.cloud.databricks.com
# workspace_id and region are derived from your workspace below; override if needed.
workspace_url = w.config.host.rstrip("/")
workspace_id = w.get_workspace_id()
# Region is the segment after the workspace host resolves; set it explicitly to be safe.
ZEROBUS_REGION = "us-west-2"   # <-- set to your workspace's region
ZEROBUS_ENDPOINT = f"https://{workspace_id}.zerobus.{ZEROBUS_REGION}.cloud.databricks.com"

# --- Lakebase (serving side) ---
project_name = f"zerobus-lakebase-{w.current_user.me().id}"
db_schema = "ecommerce"
db_user = w.current_user.me().user_name

print(f"Bronze table:     {BRONZE_TABLE}")
print(f"Zerobus endpoint: {ZEROBUS_ENDPOINT}")
print(f"Workspace URL:    {workspace_url}")
print(f"Lakebase project: {project_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Seed Reference Tables in Unity Catalog
# MAGIC
# MAGIC The gold demand table joins the clickstream against **products**, **order_items**, and
# MAGIC **inventory**. Those are seeded into Lakebase (Postgres) in Lab 1.1, but the Lakeflow pipeline
# MAGIC reads from **Unity Catalog**. So we seed the same reference data into UC here, reproducibly
# MAGIC (`random.seed(42)` — identical to Lab 1.1), so the pipeline has something to join to.
# MAGIC
# MAGIC > In a production setup these would already be in UC via **Lakehouse Sync** (Lab 4.1). We seed
# MAGIC > them directly here so this lab stands alone.

# COMMAND ----------

import random

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

random.seed(42)  # identical seed to Lab 1.1 → identical products/inventory

# --- products (50) ---
categories = {
    "Electronics": ["Laptop", "Headphones", "Phone Case", "USB Cable", "Webcam",
                    "Keyboard", "Mouse", "Monitor", "Tablet", "Speaker"],
    "Clothing": ["T-Shirt", "Jeans", "Sneakers", "Jacket", "Hat",
                 "Scarf", "Socks", "Belt", "Hoodie", "Shorts"],
    "Books": ["Python Guide", "SQL Mastery", "Data Engineering", "ML Handbook", "Cloud Atlas",
              "Clean Code", "System Design", "Algorithms", "DevOps Handbook", "AI Ethics"],
    "Home": ["Desk Lamp", "Coffee Mug", "Plant Pot", "Cushion", "Candle",
             "Picture Frame", "Clock", "Vase", "Blanket", "Coaster"],
    "Sports": ["Yoga Mat", "Water Bottle", "Resistance Band", "Jump Rope", "Dumbbell",
               "Tennis Ball", "Running Socks", "Gym Bag", "Towel", "Foam Roller"],
}
products = []
pid = 0
for category, items in categories.items():
    for item in items:
        pid += 1
        products.append((pid, item, round(random.uniform(5.99, 299.99), 2), category))

# --- inventory (50) --- mirrors Lab 1.1's generation order
warehouses = ["US-East", "US-West", "EU-Central"]
inventory_rows = []
for product_id in range(1, 51):
    inventory_rows.append(
        (product_id, random.randint(0, 200),
         warehouses[product_id % len(warehouses)], random.choice([5, 10, 15, 20]))
    )

products_df = spark.createDataFrame(products, ["id", "name", "price", "category"])
inventory_df = spark.createDataFrame(
    inventory_rows, ["product_id", "quantity", "warehouse", "reorder_level"]
)
products_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.products")
inventory_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.inventory")

# --- order_items --- derive from the same 22 seeded orders (units sold per product) ---
# We reproduce just the per-product units sold that the demand table needs.
random.seed(42)
# advance the RNG past customers/products to match Lab 1.1 ordering isn't necessary here:
# we only need a plausible, reproducible order_items table for the conversion join.
order_seed = [
    (1, 1), (2, 1), (4, 1), (3, 1), (5, 2), (2, 1), (6, 3), (1, 1), (13, 2), (10, 1),
    (7, 1), (8, 1), (9, 2), (11, 1), (12, 2), (15, 1), (14, 1), (4, 1), (3, 1), (5, 1),
    (1, 1), (2, 2),
]
order_items = []
oid = 0
for prod_id, qty in order_seed:
    oid += 1
    price = next(p[2] for p in products if p[0] == prod_id)
    order_items.append((oid, oid, prod_id, qty, price, round(price * qty, 2)))
order_items_df = spark.createDataFrame(
    order_items, ["id", "order_id", "product_id", "quantity", "unit_price", "line_total"]
)
order_items_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.order_items")

print(f"✅ Seeded UC reference tables: products (50), inventory (50), order_items ({len(order_items)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate a Baseline Clickstream (visible, reproducible)
# MAGIC
# MAGIC Live shopper traffic is sparse during a workshop, so we generate ~500 baseline shopper sessions
# MAGIC to give the pipeline something real to aggregate. The generator is **fully visible** below — no
# MAGIC hidden magic. It models a realistic funnel:
# MAGIC
# MAGIC - Each session views several products (category-weighted — Electronics and Clothing get more
# MAGIC   attention), **clicks** into a subset, and **adds to cart** a smaller subset (funnel drop-off).
# MAGIC - `random.seed(42)` makes it reproducible, so the demand numbers are stable across re-runs.
# MAGIC
# MAGIC This is exactly the shape the storefront emits in production (see
# MAGIC `datacart-storefront/server/zerobus_producer.py`) — one record per event, three event types,
# MAGIC one table.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

random.seed(42)

NUM_SESSIONS = 500
# Category popularity weights (Electronics/Clothing draw more browsing).
category_weight = {"Electronics": 4, "Clothing": 3, "Books": 1, "Home": 2, "Sports": 2}
# product_id → category, from the seed above (products list is 1-indexed by pid).
prod_category = {p[0]: p[3] for p in products}
# Build a weighted pool of product_ids to sample views from.
weighted_pool = []
for prod_id, cat in prod_category.items():
    weighted_pool.extend([prod_id] * category_weight[cat])

now = datetime.now(timezone.utc)
records = []  # each is a dict {event_type, product_id, event_ts}
for s in range(NUM_SESSIONS):
    session_start = now - timedelta(minutes=random.randint(0, 60 * 24))
    # A session views 2-8 products.
    n_views = random.randint(2, 8)
    viewed = random.sample(weighted_pool, min(n_views, len(set(weighted_pool))))
    t = session_start
    for prod_id in viewed:
        t += timedelta(seconds=random.randint(5, 90))
        records.append({"event_type": "view", "product_id": prod_id, "event_ts": t})
        # ~45% of views become a click (open detail page).
        if random.random() < 0.45:
            t += timedelta(seconds=random.randint(3, 40))
            records.append({"event_type": "click", "product_id": prod_id, "event_ts": t})
            # ~35% of clicks become an add-to-cart.
            if random.random() < 0.35:
                t += timedelta(seconds=random.randint(2, 30))
                records.append({"event_type": "add_to_cart", "product_id": prod_id, "event_ts": t})

random.shuffle(records)  # interleave sessions like a real stream
print(f"Generated {len(records)} clickstream events across {NUM_SESSIONS} sessions")
print("Sample:", records[:3])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ingest the Clickstream via Zerobus
# MAGIC
# MAGIC We open a Zerobus stream to `clickstream_bronze` and push each event. Key SDK facts:
# MAGIC
# MAGIC - **JSON record type** — no protobuf compile step; the payload is a **JSON string**.
# MAGIC - `ingest_record_offset(...)` queues a record and returns immediately; the SDK sends and acks in
# MAGIC   the background. Call `flush()` to make records durable in Delta, then `close()`. **Records are
# MAGIC   NOT visible in the table until a flush** — the SDK buffers aggressively for throughput.
# MAGIC
# MAGIC **Authentication.** The SDK supports OAuth client credentials
# MAGIC (`create_stream(client_id, client_secret, ...)`) — that's what the *storefront app* uses at
# MAGIC runtime via its auto-injected service-principal creds. In this **notebook**, the simplest path is
# MAGIC to reuse *your own* notebook identity's bearer token via a custom `HeadersProvider` — no service
# MAGIC principal secret to create. (Your identity already owns the bronze table from Lab 1.1.)

# COMMAND ----------

import json
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties, HeadersProvider


class NotebookTokenHeaders(HeadersProvider):
    """Authenticate the Zerobus stream with the notebook's own OAuth token.

    Zerobus needs, per request: an Authorization bearer header and the target
    table name header. We mint the token from the notebook's WorkspaceClient
    config so no service-principal secret is required for the seed load.
    """
    def get_headers(self):
        token = w.config.oauth_token().access_token
        return [
            ("authorization", f"Bearer {token}"),
            ("x-databricks-zerobus-table-name", BRONZE_TABLE),
        ]


sdk = ZerobusSdk(ZEROBUS_ENDPOINT, workspace_url, application_name="datacart-lab-3.1/1.0")
table_properties = TableProperties(BRONZE_TABLE)  # descriptor None => JSON mode
options = StreamConfigurationOptions(record_type=RecordType.JSON)
# headers_provider overrides OAuth client-credential auth; client_id/secret are ignored
# when it's supplied. (The storefront app instead passes its SP client_id/secret here.)
stream = sdk.create_stream("", "", table_properties, options,
                           headers_provider=NotebookTokenHeaders())

count = 0
for rec in records:
    payload = json.dumps({
        "event_type": rec["event_type"],
        "product_id": int(rec["product_id"]),
        "event_ts": rec["event_ts"].isoformat(),
    })
    stream.ingest_record_offset(payload)
    count += 1

stream.flush()   # block until everything queued is acknowledged + committed to Delta
stream.close()
print(f"✅ Ingested {count} clickstream events into {BRONZE_TABLE} via Zerobus")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the bronze landing
# MAGIC
# MAGIC The raw events are now in Delta. Note that because Zerobus is **at-least-once**, the row count
# MAGIC may be slightly higher than what we sent — that's expected, and we dedup in silver.

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_type, COUNT(*) AS events
    FROM {BRONZE_TABLE}
    GROUP BY event_type
    ORDER BY events DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build the Medallion Pipeline (Lakeflow, authored in SQL)
# MAGIC
# MAGIC The transforms live in `pipelines/clickstream_medallion.sql` (next to this notebook). It defines:
# MAGIC
# MAGIC - **silver** — `CREATE OR REFRESH STREAMING TABLE clickstream_silver`: reads the bronze table as
# MAGIC   a stream, **deduplicates** the at-least-once events (`SELECT DISTINCT`), drops malformed rows
# MAGIC   with `EXPECT ... ON VIOLATION DROP ROW`, and enriches with product name + category.
# MAGIC - **gold** — `CREATE OR REFRESH MATERIALIZED VIEW product_demand`: pivots per-product
# MAGIC   `views / clicks / add_to_carts` with `COUNT(*) FILTER (...)`, derives `cart_rate`, and
# MAGIC   `LEFT JOIN`s the seeded `order_items` (units sold, conversion) and `inventory` (stock,
# MAGIC   restock signal).
# MAGIC
# MAGIC Everything is **SQL** — no PySpark. We create the pipeline pointing at that SQL file and run it.

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary, NotebookLibrary

# Path to the SQL transform file (sibling of this notebook in the repo).
import os
repo_root = os.path.dirname(os.path.dirname(os.path.abspath("__file__"))) if False else None
# In a Databricks Repo/Workspace, reference the file by its workspace path:
SQL_PATH = f"/Workspace{os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())}/pipelines/clickstream_medallion.sql"
print(f"Pipeline source: {SQL_PATH}")

pipeline_name = f"datacart-clickstream-medallion-{w.current_user.me().id}"

created = w.pipelines.create(
    name=pipeline_name,
    catalog=UC_CATALOG,
    schema=UC_SCHEMA,
    serverless=True,
    photon=True,
    configuration={
        "source_catalog": UC_CATALOG,
        "source_schema": UC_SCHEMA,
    },
    libraries=[PipelineLibrary(file=FileLibrary(path=SQL_PATH))],
    continuous=False,
)
pipeline_id = created.pipeline_id
print(f"✅ Created pipeline '{pipeline_name}' ({pipeline_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the pipeline and wait for it to finish

# COMMAND ----------

update = w.pipelines.start_update(pipeline_id=pipeline_id)
print(f"Started update {update.update_id} — waiting for completion...")

import time
from databricks.sdk.service.pipelines import UpdateInfoState

while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
    state = info.update.state
    print(f"   state: {state}")
    if state in (UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
        break
    time.sleep(15)

if state != UpdateInfoState.COMPLETED:
    raise RuntimeError(f"Pipeline update ended in state {state}. Check the pipeline UI: "
                       f"{workspace_url}/pipelines/{pipeline_id}")
print("✅ Pipeline completed — silver + gold are populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify the Gold `product_demand` Table
# MAGIC
# MAGIC This is the lab's deliverable — per-product demand, joined with sales and stock.

# COMMAND ----------

display(spark.sql(f"""
    SELECT product_name, category, views, clicks, add_to_carts, cart_rate,
           units_sold, conversion_rate, in_stock, restock_needed
    FROM {UC_CATALOG}.{UC_SCHEMA}.product_demand
    ORDER BY views DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Sync `product_demand` Back to Lakebase
# MAGIC
# MAGIC Now reverse the direction (exactly the synced-table mechanism from Lab 2.1): push the gold table
# MAGIC into the production Lakebase branch so the storefront's **Supplier Demand View** can read it.
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
# MAGIC    - **Sync mode**: **Snapshot** (simplest; the gold MV is small)
# MAGIC    - **Primary key**: `product_id`
# MAGIC 4. **Create**, and wait for the sync to complete.
# MAGIC
# MAGIC > The storefront's Supplier View auto-detects a table named `product_demand*` in the `ecommerce`
# MAGIC > schema (see `server/schema_detector.get_demand_table`), so once the sync lands, the view lights
# MAGIC > up with no app redeploy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Grant the Storefront SP Access to the Synced Table
# MAGIC
# MAGIC Just like Lab 2.1: synced tables are created by the Lakebase sync pipeline (a different role), so
# MAGIC re-grant table access to the app's SP so the Supplier View can read `product_demand_synced_prod`.

# COMMAND ----------

import time
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
# MAGIC App emits clicks ──▶ Zerobus ──▶ bronze ──▶ silver (dedup+enrich) ──▶ gold product_demand
# MAGIC                                                                              │ synced table
# MAGIC                                                                              ▼
# MAGIC                                                              Supplier Demand View (/supplier)
# MAGIC ```
# MAGIC
# MAGIC **Key insight:** The app never aggregated anything. It only *emitted* raw events. The lakehouse
# MAGIC did the demand math (Photon, governed, joined with orders and inventory), and only the small
# MAGIC aggregated result was synced back for serving. That's the full **collect → aggregate → present**
# MAGIC loop on one platform — **zero application code changes** to surface the result.
# MAGIC
# MAGIC ## Done
# MAGIC
# MAGIC You streamed a live clickstream into governed Delta with Zerobus, aggregated it into a demand
# MAGIC signal with a Lakeflow SQL pipeline, and served it back through Lakebase to the storefront.
# MAGIC
# MAGIC **Next:** Lab 4.1 — Lakehouse Sync (Lakebase → UC), the continuous mirror in the other direction.
