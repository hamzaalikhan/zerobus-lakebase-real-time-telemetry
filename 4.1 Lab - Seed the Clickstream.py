# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.1: Seed the Clickstream
# MAGIC
# MAGIC This is the **first of three short labs** that build the real-time analytics loop:
# MAGIC
# MAGIC | Lab | Step | What it does |
# MAGIC |---|---|---|
# MAGIC | **4.1 (this lab)** | **collect** | Land a raw clickstream (product views, clicks, add-to-carts) in a **bronze** Delta table |
# MAGIC | 4.2 | aggregate | A Lakeflow medallion pipeline turns the raw clickstream into a per-product **demand** signal |
# MAGIC | 4.3 | present | Sync that demand back to Lakebase so the storefront's **Supplier View** shows it |
# MAGIC
# MAGIC In this lab we keep it simple: generate a realistic, reproducible clickstream and write it
# MAGIC straight into a bronze Delta table with Spark. (Lab 5.1 later shows how a real app pushes this
# MAGIC same kind of stream in live with **Zerobus** — here we just seed it so the pipeline has data.)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Understand** the bronze layer of the medallion architecture — raw events, landed as-is
# MAGIC 2. **Generate** a realistic shopper funnel (view → click → add-to-cart) reproducibly
# MAGIC 3. **Write** the clickstream into a governed Unity Catalog Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC Set the same **catalog** and **schema** you used in Lab 1.1 (the widgets default to the same
# MAGIC values). The bronze table lands in that schema.

# COMMAND ----------

dbutils.widgets.text("catalog", "datacart", "1. Catalog name")
dbutils.widgets.text("schema", "ecommerce", "2. Schema name")

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()
BRONZE_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.clickstream_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
print(f"Bronze table: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: What Is the Bronze Layer?
# MAGIC
# MAGIC The **medallion architecture** organizes data into three quality tiers:
# MAGIC
# MAGIC | Layer | Role | In this workshop |
# MAGIC |---|---|---|
# MAGIC | 🥉 **Bronze** | Raw events, landed exactly as they arrive | `clickstream_bronze` — this lab |
# MAGIC | 🥈 **Silver** | Cleaned, deduplicated, typed, enriched | `clickstream_silver` — Lab 4.2 |
# MAGIC | 🥇 **Gold** | Business-level aggregates ready to serve | `product_demand` — Lab 4.2 |
# MAGIC
# MAGIC Bronze is deliberately dumb: it captures every event with no filtering, so nothing is lost and
# MAGIC you can always re-process. Our clickstream has one row per shopper action:
# MAGIC
# MAGIC | Column | Type | Meaning |
# MAGIC |---|---|---|
# MAGIC | `session_id` | STRING | Which shopper session the event belongs to |
# MAGIC | `event_type` | STRING | `view`, `click`, or `add_to_cart` |
# MAGIC | `product_id` | INT | Which product the event is about (joins to `products`) |
# MAGIC | `event_ts` | TIMESTAMP | When it happened |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate a Realistic Clickstream
# MAGIC
# MAGIC Live shopper traffic is sparse during a workshop, so we generate ~500 shopper sessions to give
# MAGIC the pipeline something real to aggregate. The generator is **fully visible** below — it models a
# MAGIC realistic funnel where each session:
# MAGIC
# MAGIC - **views** several products (Electronics and Clothing get more attention — category-weighted),
# MAGIC - **clicks** into ~45% of what it views (opens the detail page),
# MAGIC - **adds to cart** ~35% of what it clicks (funnel drop-off).
# MAGIC
# MAGIC `random.seed(42)` makes it reproducible, so the demand numbers are stable across re-runs.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

random.seed(42)

NUM_SESSIONS = 500

# Product 1-50 map to categories in the same order Lab 1.1 seeded them.
category_order = ["Electronics", "Clothing", "Books", "Home", "Sports"]
category_weight = {"Electronics": 4, "Clothing": 3, "Books": 1, "Home": 2, "Sports": 2}
# products 1-10 = Electronics, 11-20 = Clothing, 21-30 = Books, 31-40 = Home, 41-50 = Sports
prod_category = {pid: category_order[(pid - 1) // 10] for pid in range(1, 51)}

# Weighted pool of product_ids to sample views from (popular categories appear more).
weighted_pool = []
for prod_id, cat in prod_category.items():
    weighted_pool.extend([prod_id] * category_weight[cat])

now = datetime.now(timezone.utc)
records = []  # each is a tuple (session_id, event_type, product_id, event_ts)
for s in range(NUM_SESSIONS):
    session_id = f"sess-{s:04d}"
    session_start = now - timedelta(minutes=random.randint(0, 60 * 24))
    n_views = random.randint(2, 8)
    viewed = random.sample(weighted_pool, min(n_views, len(set(weighted_pool))))
    t = session_start
    for prod_id in viewed:
        t += timedelta(seconds=random.randint(5, 90))
        records.append((session_id, "view", prod_id, t))
        if random.random() < 0.45:  # ~45% of views → click
            t += timedelta(seconds=random.randint(3, 40))
            records.append((session_id, "click", prod_id, t))
            if random.random() < 0.35:  # ~35% of clicks → add_to_cart
                t += timedelta(seconds=random.randint(2, 30))
                records.append((session_id, "add_to_cart", prod_id, t))

random.shuffle(records)  # interleave sessions like a real stream
print(f"Generated {len(records)} clickstream events across {NUM_SESSIONS} sessions")
print("Sample:", records[:3])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write the Clickstream to Bronze
# MAGIC
# MAGIC We write the events into a Delta table with an explicit schema. `mode("overwrite")` makes the
# MAGIC lab re-runnable — each run reseeds a clean bronze table.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType([
    StructField("session_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("event_ts", TimestampType(), False),
])

bronze_df = spark.createDataFrame(records, schema=schema)
(bronze_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE))

spark.sql(f"""
    COMMENT ON TABLE {BRONZE_TABLE} IS
    'Raw storefront clickstream (bronze). One row per shopper action: view/click/add_to_cart. Seeded in Lab 4.1; aggregated by the Lab 4.2 medallion pipeline.'
""")
print(f"✅ Wrote {bronze_df.count()} events to {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Inspect the Bronze Table

# COMMAND ----------

display(spark.sql(f"""
    SELECT event_type, COUNT(*) AS events
    FROM {BRONZE_TABLE}
    GROUP BY event_type
    ORDER BY events DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The raw clickstream is now in the bronze Delta table — the funnel is visible in the counts
# MAGIC (views > clicks > add-to-carts). Nothing is cleaned or aggregated yet; that's the next lab's job.
# MAGIC
# MAGIC **Next:** Lab 4.2 — build the medallion pipeline that turns this raw stream into a per-product
# MAGIC demand signal.
