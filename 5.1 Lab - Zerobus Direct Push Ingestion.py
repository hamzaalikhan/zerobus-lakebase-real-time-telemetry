# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.1: Zerobus — Direct Push Ingestion
# MAGIC
# MAGIC In Labs 4.1–4.3 we *seeded* a clickstream so the medallion pipeline had data. In the real world
# MAGIC that stream arrives **live** from the application. **Zerobus** is how it gets there: a serverless
# MAGIC **push ingestion API** that writes records straight into a Unity-Catalog Delta table — no Kafka,
# MAGIC no Kinesis, no message bus to run.
# MAGIC
# MAGIC This lab is a focused, standalone look at Zerobus ingestion. We'll define a **typed** target table,
# MAGIC push conforming records into it, and then deliberately push a record that **doesn't match the
# MAGIC schema** to see how Zerobus protects the table.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Explain** what Zerobus is and when to reach for it
# MAGIC 2. **Create** a typed Delta table and open a Zerobus stream to it
# MAGIC 3. **Ingest** well-formed records and verify they land in Delta
# MAGIC 4. **Observe** what happens when a record does not conform to the table's schema
# MAGIC
# MAGIC > This lab is self-contained — it doesn't depend on the earlier labs' data. It creates its own
# MAGIC > small demo table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Zerobus?
# MAGIC
# MAGIC **Zerobus** is a serverless **push ingestion API** for Databricks. A producer pushes records over
# MAGIC a lightweight stream and they land **directly in a Unity-Catalog-managed Delta table**.
# MAGIC
# MAGIC | Property | What it means |
# MAGIC |---|---|
# MAGIC | **Serverless push** | The producer just calls `stream.ingest_record_offset(...)`. No infra to run. |
# MAGIC | **Writes to Delta directly** | Records land in a governed, versioned, joinable Delta table. |
# MAGIC | **Table must pre-exist** | Zerobus does **not** create tables. You define the schema up front. |
# MAGIC | **At-least-once delivery** | Duplicates are possible under retries — dedup downstream (e.g. in silver). |
# MAGIC | **Region-gated** | Producer and target table must be in the **same region**; the endpoint URL is region-specific. |
# MAGIC
# MAGIC In this workshop's storefront, `server/zerobus_producer.py` uses exactly this API to push live
# MAGIC clicks (it's off by default). Here we drive it directly from the notebook to see it up close.

# COMMAND ----------

# MAGIC %pip install databricks-zerobus-ingest-sdk databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC Zerobus needs a **region-specific endpoint**: `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`.
# MAGIC Set your workspace's region in the widget.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog name")
dbutils.widgets.text("schema", "", "2. Schema name")
dbutils.widgets.text("region", "", "3. Workspace region")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()
ZEROBUS_REGION = dbutils.widgets.get("region").strip()

DEMO_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.zerobus_events_demo"

workspace_url = w.config.host.rstrip("/")
workspace_id = w.get_workspace_id()
ZEROBUS_ENDPOINT = f"https://{workspace_id}.zerobus.{ZEROBUS_REGION}.cloud.databricks.com"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

print(f"Demo table:       {DEMO_TABLE}")
print(f"Zerobus endpoint: {ZEROBUS_ENDPOINT}")
print(f"Workspace URL:    {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Typed Target Table
# MAGIC
# MAGIC Zerobus won't create the table — we define it first, with an explicit **schema**. Giving each
# MAGIC column a real type is what lets Zerobus reject data that doesn't belong.
# MAGIC
# MAGIC | Column | Type | Meaning |
# MAGIC |---|---|---|
# MAGIC | `session_id` | STRING | Shopper session |
# MAGIC | `event_type` | STRING | `view` / `click` / `add_to_cart` |
# MAGIC | `product_id` | INT | Which product |
# MAGIC | `price` | DOUBLE | Product price at event time |
# MAGIC | `event_ts` | STRING | Client-side ISO-8601 timestamp (kept raw as text in bronze) |
# MAGIC
# MAGIC > **Why `event_ts` as STRING?** With JSON ingest, Zerobus passes the value through as text; a
# MAGIC > `TIMESTAMP` column would reject an ISO-8601 string. Landing raw text and casting later (in a
# MAGIC > silver layer) is the standard medallion pattern. The **typed** columns to watch here are
# MAGIC > `product_id` (INT) and `price` (DOUBLE).

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DEMO_TABLE}")
spark.sql(f"""
    CREATE TABLE {DEMO_TABLE} (
        session_id  STRING,
        event_type  STRING,
        product_id  INT,
        price       DOUBLE,
        event_ts    STRING
    )
    USING DELTA
    COMMENT 'Zerobus direct-push demo table (Lab 5.1).'
""")
print(f"✅ Created typed table: {DEMO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Open a Zerobus Stream
# MAGIC
# MAGIC We use the **JSON** record type (no protobuf compile step) and authenticate with the notebook's
# MAGIC own OAuth token via a custom `HeadersProvider` — so there's no service-principal secret to create.
# MAGIC (The storefront app instead passes its SP `client_id`/`client_secret`.)

# COMMAND ----------

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties, HeadersProvider


class NotebookTokenHeaders(HeadersProvider):
    """Authenticate the Zerobus stream with the notebook's own OAuth token."""
    def __init__(self, table_name):
        self._table = table_name

    def get_headers(self):
        token = w.config.oauth_token().access_token
        return [
            ("authorization", f"Bearer {token}"),
            ("x-databricks-zerobus-table-name", self._table),
        ]


def open_stream(table_name):
    sdk = ZerobusSdk(ZEROBUS_ENDPOINT, workspace_url, application_name="datacart-lab-5.1/1.0")
    table_properties = TableProperties(table_name)          # descriptor None => JSON mode
    options = StreamConfigurationOptions(record_type=RecordType.JSON)
    return sdk.create_stream("", "", table_properties, options,
                             headers_provider=NotebookTokenHeaders(table_name))


print("Stream helper ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ingest Conforming Records
# MAGIC
# MAGIC Each record is a **JSON string** whose fields and types match the table. `ingest_record_offset`
# MAGIC queues a record; `flush()` makes everything durable in Delta; `close()` ends the stream.
# MAGIC
# MAGIC > **Records aren't visible in the table until a flush** — the SDK buffers for throughput.

# COMMAND ----------

import json
from datetime import datetime, timezone

good_records = [
    {"session_id": "sess-0001", "event_type": "view",        "product_id": 1,  "price": 1299.99, "event_ts": datetime.now(timezone.utc).isoformat()},
    {"session_id": "sess-0001", "event_type": "click",       "product_id": 1,  "price": 1299.99, "event_ts": datetime.now(timezone.utc).isoformat()},
    {"session_id": "sess-0001", "event_type": "add_to_cart", "product_id": 1,  "price": 1299.99, "event_ts": datetime.now(timezone.utc).isoformat()},
    {"session_id": "sess-0002", "event_type": "view",        "product_id": 14, "price": 69.99,   "event_ts": datetime.now(timezone.utc).isoformat()},
    {"session_id": "sess-0002", "event_type": "view",        "product_id": 23, "price": 24.99,   "event_ts": datetime.now(timezone.utc).isoformat()},
]

stream = open_stream(DEMO_TABLE)
for rec in good_records:
    stream.ingest_record_offset(json.dumps(rec))
stream.flush()
stream.close()
print(f"✅ Ingested {len(good_records)} conforming records into {DEMO_TABLE}")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {DEMO_TABLE} ORDER BY session_id, event_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Try a Non-Conforming Record
# MAGIC
# MAGIC Now push a record that violates the schema: `product_id` is declared `INT`, but we send the
# MAGIC **string** `"not-a-number"`. Zerobus can't coerce that into an integer column, so the flush
# MAGIC **fails** — the bad data never lands, and the good data in the table is untouched.
# MAGIC
# MAGIC This is the value of a typed target: the schema is a contract. Malformed producers get an error
# MAGIC instead of silently corrupting the table.

# COMMAND ----------

bad_record = {
    "session_id": "sess-9999",
    "event_type": "view",
    "product_id": "not-a-number",   # ← should be an INT
    "price": 9.99,
    "event_ts": datetime.now(timezone.utc).isoformat(),
}

stream = open_stream(DEMO_TABLE)
try:
    stream.ingest_record_offset(json.dumps(bad_record))
    stream.flush()   # the type mismatch surfaces here
    print("⚠️ Unexpected: the non-conforming record was accepted.")
except Exception as e:
    print("✅ As expected, Zerobus rejected the non-conforming record.")
    print(f"   Error: {type(e).__name__}: {str(e)[:200]}")
finally:
    try:
        stream.close()
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirm the table is unharmed
# MAGIC
# MAGIC The row count is still exactly the conforming records from Step 4 — the bad record did not land.

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_rows,
           COUNT(*) FILTER (WHERE session_id = 'sess-9999') AS bad_rows
    FROM {DEMO_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Clean Up (Optional)

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {DEMO_TABLE}")
# print(f"🗑️ Dropped {DEMO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC You pushed records straight into a governed Delta table with Zerobus — no message bus — and saw
# MAGIC how a **typed** target table rejects data that doesn't conform, protecting downstream consumers.
# MAGIC
# MAGIC **How this connects to the rest of the workshop:** the clickstream you *seeded* in Lab 4.1 would,
# MAGIC in production, arrive live through Zerobus exactly like this (see `server/zerobus_producer.py`).
# MAGIC From there it flows through the same medallion pipeline (4.2) and back to Lakebase (4.3).
