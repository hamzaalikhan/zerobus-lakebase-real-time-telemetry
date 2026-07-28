# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.2: Medallion Pipeline (Lakeflow, in SQL)
# MAGIC
# MAGIC This is the **aggregate** step of the real-time analytics loop:
# MAGIC
# MAGIC | Lab | Step | What it does |
# MAGIC |---|---|---|
# MAGIC | 4.1 | collect | Landed the raw clickstream in **bronze** |
# MAGIC | **4.2 (this lab)** | **aggregate** | A **Lakeflow** medallion pipeline turns bronze into a per-product **demand** signal |
# MAGIC | 4.3 | present | Sync that demand back to Lakebase for the Supplier View |
# MAGIC
# MAGIC We build a bronze → silver → gold pipeline **authored entirely in SQL** (no PySpark). It reads the
# MAGIC bronze clickstream from Lab 4.1 and joins it with the operational tables (`products`, `inventory`,
# MAGIC `order_items`) that **Lakehouse Sync mirrored into UC in Lab 3.1** — so a supplier sees not just
# MAGIC what shoppers browse, but how that intent maps to real sales and stock.
# MAGIC
# MAGIC ```
# MAGIC  clickstream_bronze ─┐
# MAGIC                      ├─▶ clickstream_silver ─┐
# MAGIC  products ───────────┘   (dedup + enrich)    ├─▶ product_demand (gold)
# MAGIC  order_items ─────────────────────────────────┤
# MAGIC  inventory ───────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Understand** silver (clean/dedup/enrich) and gold (business aggregates) in the medallion model
# MAGIC 2. **Create and run** a Lakeflow declarative pipeline from a SQL file
# MAGIC 3. **Read** the lineage graph that shows how the demand table is built from six upstream objects
# MAGIC
# MAGIC > **Prerequisites:** Lab 4.1 (bronze clickstream) and, ideally, Lab 3.1 (Lakehouse Sync, which
# MAGIC > mirrors `products`/`inventory`/`order_items` into UC). If you skipped Lab 3.1, Step 2 below
# MAGIC > seeds those three tables as a fallback so this lab still runs standalone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog name")
dbutils.widgets.text("schema", "", "2. Schema name")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()

workspace_url = w.config.host.rstrip("/")
print(f"Catalog/schema: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Make Sure the Join Tables Exist
# MAGIC
# MAGIC The gold demand table joins the clickstream against **products**, **order_items**, and
# MAGIC **inventory**. In the normal flow those are already in UC because **Lab 3.1 (Lakehouse Sync)**
# MAGIC mirrored them from Lakebase.
# MAGIC
# MAGIC The cell below checks for them. If any are missing (you skipped Lab 3.1), it seeds a small,
# MAGIC reproducible fallback copy so this lab still runs — same `random.seed(42)` as Lab 1.1, so the
# MAGIC numbers line up.

# COMMAND ----------

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(f"{UC_CATALOG}.{UC_SCHEMA}.{name}")

needed = ["products", "inventory", "order_items"]
missing = [t for t in needed if not table_exists(t)]

if not missing:
    print("✅ Join tables already present (mirrored by Lab 3.1 Lakehouse Sync):")
    for t in needed:
        print(f"   {UC_CATALOG}.{UC_SCHEMA}.{t}")
else:
    print(f"⚠️  Missing {missing} — seeding a fallback copy (Lab 3.1 not run).")
    import random
    random.seed(42)

    # products (50) — identical generation order to Lab 1.1
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
    products, pid = [], 0
    for category, items in categories.items():
        for item in items:
            pid += 1
            products.append((pid, item, round(random.uniform(5.99, 299.99), 2), category))

    warehouses = ["US-East", "US-West", "EU-Central"]
    inventory_rows = [
        (p, random.randint(0, 200), warehouses[p % 3], random.choice([5, 10, 15, 20]))
        for p in range(1, 51)
    ]

    # A compact, reproducible order_items table for the conversion join.
    order_seed = [
        (1, 1), (2, 1), (4, 1), (3, 1), (5, 2), (2, 1), (6, 3), (1, 1), (13, 2), (10, 1),
        (7, 1), (8, 1), (9, 2), (11, 1), (12, 2), (15, 1), (14, 1), (4, 1), (3, 1), (5, 1),
        (1, 1), (2, 2),
    ]
    order_items, oid = [], 0
    for prod_id, qty in order_seed:
        oid += 1
        price = next(p[2] for p in products if p[0] == prod_id)
        order_items.append((oid, oid, prod_id, qty, price, round(price * qty, 2)))

    spark.createDataFrame(products, ["id", "name", "price", "category"]) \
        .write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.products")
    spark.createDataFrame(inventory_rows, ["product_id", "quantity", "warehouse", "reorder_level"]) \
        .write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.inventory")
    spark.createDataFrame(order_items, ["id", "order_id", "product_id", "quantity", "unit_price", "line_total"]) \
        .write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.order_items")
    print(f"✅ Seeded fallback products (50), inventory (50), order_items ({len(order_items)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Review the Pipeline SQL
# MAGIC
# MAGIC The transforms live in `pipelines/clickstream_medallion.sql` (next to this notebook). It defines
# MAGIC two objects:
# MAGIC
# MAGIC - **silver** — `CREATE OR REFRESH STREAMING TABLE clickstream_silver`: reads the bronze table as a
# MAGIC   stream, **deduplicates** events (`SELECT DISTINCT`), drops malformed rows with
# MAGIC   `EXPECT ... ON VIOLATION DROP ROW`, and enriches with product name + category.
# MAGIC - **gold** — `CREATE OR REFRESH MATERIALIZED VIEW product_demand`: pivots per-product
# MAGIC   `views / clicks / add_to_carts` with `COUNT(*) FILTER (...)`, derives a `cart_rate`, and joins
# MAGIC   `order_items` (units sold, conversion) and `inventory` (stock, restock signal).
# MAGIC
# MAGIC Everything is SQL. The pipeline reads two config values — `source_catalog` and `source_schema` —
# MAGIC which we pass below so it knows where your tables live.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create the Lakeflow Pipeline

# COMMAND ----------

import os
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

# The SQL file sits in pipelines/ next to this notebook. Resolve its workspace path.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
SQL_PATH = f"/Workspace{os.path.dirname(notebook_path)}/pipelines/clickstream_medallion.sql"
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
print(f"   Open it: {workspace_url}/pipelines/{pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run the Pipeline

# COMMAND ----------

import time
from databricks.sdk.service.pipelines import UpdateInfoState

update = w.pipelines.start_update(pipeline_id=pipeline_id)
print(f"Started update {update.update_id} — waiting for completion...")

while True:
    info = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
    state = info.update.state
    print(f"   state: {state}")
    if state in (UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
        break
    time.sleep(15)

if state != UpdateInfoState.COMPLETED:
    raise RuntimeError(
        f"Pipeline update ended in state {state}. Check the pipeline UI: "
        f"{workspace_url}/pipelines/{pipeline_id}"
    )
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
# MAGIC ## Step 7: Look at the Lineage
# MAGIC
# MAGIC Open the pipeline (link printed in Step 4) and look at the **graph**. You'll see `product_demand`
# MAGIC built from six upstream objects:
# MAGIC
# MAGIC ```
# MAGIC clickstream_bronze ─▶ clickstream_silver ─┐
# MAGIC products ─────────────────────────────────┤
# MAGIC order_items ──────────────────────────────┼─▶ product_demand
# MAGIC inventory ────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC That's the whole point of the medallion: the raw clickstream on its own says *what shoppers look
# MAGIC at*; joined with orders and inventory it becomes *actionable demand* — high intent + low stock =
# MAGIC restock now.
# MAGIC
# MAGIC ## Done
# MAGIC
# MAGIC You built a governed, SQL-only medallion pipeline that aggregates the clickstream into a
# MAGIC per-product demand signal.
# MAGIC
# MAGIC **Next:** Lab 4.3 — sync `product_demand` back to Lakebase so the storefront's Supplier View
# MAGIC shows it.
