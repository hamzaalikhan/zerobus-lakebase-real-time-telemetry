# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1.1: Set Up Lakebase and Connect the Storefront
# MAGIC
# MAGIC This is the workshop's setup lab. You'll set your workshop inputs once (catalog, schema),
# MAGIC **create the Unity Catalog** everything downstream writes to, discover your Lakebase project,
# MAGIC connect via OAuth, seed an e-commerce schema, and grant the DataCart Storefront app the database
# MAGIC access it needs to come online. By the end, the storefront serves real data and you're ready for
# MAGIC the rest of the labs.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. **Configure** the workshop inputs with notebook **widgets** (catalog, schema) — set once here,
# MAGIC    reused by every downstream lab
# MAGIC 2. **Create** a Unity Catalog with an explicit **managed storage location** (required for synced
# MAGIC    tables and Lakehouse Sync in later labs)
# MAGIC 3. **Discover** your Lakebase Autoscaling project and **connect** using OAuth token authentication
# MAGIC 4. **Create and populate** a PostgreSQL `ecommerce` schema (5 tables, realistic data)
# MAGIC 5. **Grant** the storefront app's service principal access to that schema
# MAGIC
# MAGIC > **Setup expectation**: Your Lakebase project and the DataCart Storefront app are deployed by
# MAGIC > the bundle before the workshop. If they aren't, see `WORKSHOP_SETUP.md`.
# MAGIC
# MAGIC > **Docs**: [Lakebase Autoscaling Projects](https://docs.databricks.com/aws/en/oltp/projects/) | [Manage Postgres roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles) | [API Reference](https://docs.databricks.com/api/workspace/postgres)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakebase Autoscaling?
# MAGIC
# MAGIC Lakebase Autoscaling is **100% standard PostgreSQL 17** with automatic scaling, instant
# MAGIC branching, and deep Databricks integration. It targets **OLTP** workloads — the transactional
# MAGIC layer behind applications and APIs. Compute scales up on demand and to **zero** when idle, so
# MAGIC you only pay for what you use.
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---|---|
# MAGIC | **Autoscaling compute** | Adjusts compute (CUs) to workload; scales to zero when idle |
# MAGIC | **Instant branching** | Zero-copy dev/test branches from production in seconds |
# MAGIC | **Point-in-time restore** | Restore or branch from any point in the restore window |
# MAGIC | **Unity Catalog integration** | Register Lakebase in UC for federated queries and governance |
# MAGIC
# MAGIC > We use **Lakebase Autoscaling** (the recommended option). Lakebase Provisioned is only for
# MAGIC > regions/features where Autoscaling isn't available.
# MAGIC
# MAGIC > **⚠️ If running in your own workspace:** delete your Lakebase project when done
# MAGIC > (Settings → Delete Project). Each workspace allows a maximum of 1000 projects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture After This Lab
# MAGIC ```
# MAGIC Unity Catalog: <your-catalog>                           ← created in this lab (Step 1)
# MAGIC └── ecommerce (schema)                                  ← the governed lakehouse side later labs use
# MAGIC
# MAGIC Lakebase Project: zerobus-lakebase-<your-user-id>        ← deployed by the bundle
# MAGIC └── production (default branch)
# MAGIC     └── ecommerce (schema)                              ← created + seeded in this lab
# MAGIC         ├── customers    (100 rows)
# MAGIC         ├── products     (50 rows)
# MAGIC         ├── inventory    (50 rows)
# MAGIC         ├── orders       (22 rows)
# MAGIC         └── order_items  (~55 rows)
# MAGIC
# MAGIC DataCart Storefront app  ──(SP granted access in Step 7)──▶  ecommerce schema
# MAGIC ```
# MAGIC
# MAGIC > **Two sides, one platform.** This lab creates both the **Unity Catalog (lakehouse)** side and
# MAGIC > seeds the **Lakebase (OLTP serving)** side the storefront reads from. Later labs move data
# MAGIC > between them in every direction — reverse ETL (Lab 2.1), Lakehouse Sync (Lab 3.1), a
# MAGIC > clickstream medallion (Labs 4.1–4.3), and Zerobus push ingestion (Lab 5.1).

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Workshop & Create Your Catalog
# MAGIC
# MAGIC Everything downstream — this lab and Labs 2.1 through 5.1 — reads from the same **Unity Catalog**
# MAGIC and **schema**. We set them once here using notebook **widgets** (the input boxes that appear at
# MAGIC the top of the notebook), so every lab picks up the same values.
# MAGIC
# MAGIC | Widget | What it is | Default |
# MAGIC |---|---|---|
# MAGIC | `catalog` | The Unity Catalog this workshop creates and uses | `datacart` |
# MAGIC | `schema` | The schema inside that catalog (matches the Lakebase schema name) | `ecommerce` |
# MAGIC | `external_location_url` | A cloud storage path you can write to (`s3://...`, `abfss://...`) — the catalog's **managed location** | *(ask your instructor)* |
# MAGIC
# MAGIC > **Why a managed location?** Later labs create **synced tables** (Lab 2.1, 4.3) and **Lakehouse
# MAGIC > Sync** (Lab 3.1). Those features need the catalog to have an explicit managed storage location
# MAGIC > (an *external location* you have `CREATE` on), not the metastore's default root. We create the
# MAGIC > catalog that way here so the later labs "just work".

# COMMAND ----------

# Widgets — the workshop's single source of truth. Edit the boxes at the top of the
# notebook, or change the defaults here. Every downstream lab reads the same values.
dbutils.widgets.text("catalog", "datacart", "1. Catalog name")
dbutils.widgets.text("schema", "ecommerce", "2. Schema name")
dbutils.widgets.text("external_location_url", "", "3. External location URL (s3://... or abfss://...)")

UC_CATALOG = dbutils.widgets.get("catalog").strip()
UC_SCHEMA = dbutils.widgets.get("schema").strip()
EXTERNAL_LOCATION_URL = dbutils.widgets.get("external_location_url").strip()

if not EXTERNAL_LOCATION_URL:
    raise ValueError(
        "Set the 'external_location_url' widget to a storage path you can write to "
        "(e.g. s3://my-bucket/datacart). Ask your instructor if unsure."
    )

print(f"Catalog:            {UC_CATALOG}")
print(f"Schema:             {UC_CATALOG}.{UC_SCHEMA}")
print(f"Managed location:   {EXTERNAL_LOCATION_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the catalog and schema
# MAGIC
# MAGIC `CREATE CATALOG ... MANAGED LOCATION` points the catalog at your external location. It's
# MAGIC idempotent (`IF NOT EXISTS`), so it's safe to re-run.

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {UC_CATALOG}
    MANAGED LOCATION '{EXTERNAL_LOCATION_URL}'
""")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
print(f"✅ Catalog '{UC_CATALOG}' ready (managed location: {EXTERNAL_LOCATION_URL})")
print(f"✅ Schema '{UC_CATALOG}.{UC_SCHEMA}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Discover Your Lakebase Project
# MAGIC
# MAGIC The `WorkspaceClient` auto-authenticates inside a Databricks notebook. Your project's
# MAGIC `project_id` is derived from your numeric user ID (`zerobus-lakebase-<id>`) — that's the
# MAGIC DNS-compliant name the API uses. In the Lakebase UI the same project shows a friendlier
# MAGIC `display_name` ("Zerobus Lakebase Workshop — Your Name"); both point at the same project.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

project_name = f"zerobus-lakebase-{w.current_user.me().id}"
db_schema = "ecommerce"
db_user = w.current_user.me().user_name

existing = list(w.postgres.list_projects())
project_obj = next((p for p in existing if p.name == f"projects/{project_name}"), None)
if project_obj is None:
    raise RuntimeError(
        f"Project '{project_name}' not found. See WORKSHOP_SETUP.md for setup instructions."
    )

workspace_host = w.config.host.rstrip("/")
print(f"Found project '{project_name}'")
print(f"   UID: {project_obj.uid}")
print(f"   Lakebase UI: {workspace_host}/lakebase/projects/{project_obj.uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get the Production Branch & Endpoint
# MAGIC
# MAGIC Every project has a default `production` branch with a primary read-write compute endpoint.
# MAGIC We need the endpoint's host to connect via `psycopg2`.

# COMMAND ----------

import time

branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next((b for b in branches if b.status and b.status.default), branches[0])

endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
if not endpoints:
    print("Compute endpoint not ready yet. Waiting...")
    for i in range(30):
        time.sleep(10)
        endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
        if endpoints:
            break
        print(f"   Still waiting... ({(i+1)*10}s)")
if not endpoints:
    raise Exception("Compute endpoint not available after 5 minutes. Check the Lakebase UI.")

prod_endpoint = endpoints[0]
prod_endpoint_name = prod_endpoint.name
prod_host = prod_endpoint.status.hosts.host
print(f"Production endpoint ready: {prod_host} (port 5432, db databricks_postgres)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Connect via OAuth
# MAGIC
# MAGIC Lakebase uses **OAuth token authentication** — your Databricks identity generates a
# MAGIC short-lived database token (no passwords to manage). A Postgres role for your identity was
# MAGIC created automatically with the project; it owns `databricks_postgres`. We generate a fresh
# MAGIC token and connect via `psycopg2`.

# COMMAND ----------

import psycopg2

cred = w.postgres.generate_database_credential(endpoint=prod_endpoint_name)
conn = psycopg2.connect(
    host=prod_host, port=5432, dbname="databricks_postgres",
    user=db_user, password=cred.token, sslmode="require",
)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
print(f"Connected to Lakebase as {db_user}")
print(f"   {version[:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed the E-Commerce Schema
# MAGIC
# MAGIC We create 5 tables modeling a realistic e-commerce app, using native PostgreSQL features:
# MAGIC `SERIAL` keys, `REFERENCES` foreign keys, `CHECK`/`UNIQUE` constraints, `ON DELETE CASCADE`.
# MAGIC
# MAGIC ```
# MAGIC customers ─┐        products ──┬──► inventory
# MAGIC            │                   │
# MAGIC            └──► orders ◄────────┘
# MAGIC                   └──► order_items
# MAGIC ```
# MAGIC
# MAGIC > The schema is intentionally rich — later labs evolve it (new columns, tables, backfills) to
# MAGIC > demonstrate reverse ETL and branching.

# COMMAND ----------

SEED_SCHEMA_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {db_schema};
SET search_path TO {db_schema};

DROP TABLE IF EXISTS {db_schema}.order_items CASCADE;
DROP TABLE IF EXISTS {db_schema}.inventory CASCADE;
DROP TABLE IF EXISTS {db_schema}.orders CASCADE;
DROP TABLE IF EXISTS {db_schema}.products CASCADE;
DROP TABLE IF EXISTS {db_schema}.customers CASCADE;

CREATE TABLE {db_schema}.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE {db_schema}.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

CREATE TABLE {db_schema}.inventory (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES {db_schema}.products(id),
    quantity INT NOT NULL DEFAULT 0,
    warehouse VARCHAR(50) NOT NULL DEFAULT 'US-East',
    reorder_level INT NOT NULL DEFAULT 10,
    last_restocked TIMESTAMP DEFAULT NOW(),
    UNIQUE(product_id, warehouse)
);

CREATE TABLE {db_schema}.orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT             NOT NULL REFERENCES {db_schema}.customers(id),
    product_id  INT             NOT NULL REFERENCES {db_schema}.products(id),
    quantity    INT             NOT NULL DEFAULT 1,
    total       NUMERIC(10, 2)  NOT NULL,
    currency    VARCHAR(3)      NOT NULL DEFAULT 'USD',
    order_date  TIMESTAMP       NOT NULL DEFAULT NOW(),
    status      VARCHAR(20)     NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled'))
);

CREATE TABLE {db_schema}.order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES {db_schema}.orders(id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES {db_schema}.products(id),
    quantity INT NOT NULL DEFAULT 1,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(10, 2) NOT NULL
);
"""

with conn.cursor() as cur:
    cur.execute(SEED_SCHEMA_SQL)
print(f"Schema '{db_schema}' created: customers, products, inventory, orders, order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Seed Sample Data
# MAGIC
# MAGIC 100 customers, 50 products across 5 categories, 50 inventory rows, 22 orders, and ~55 order
# MAGIC line items. This data is used across all the labs.

# COMMAND ----------

import random

random.seed(42)  # Reproducible data

with conn.cursor() as cur:

    # --- Customers (100) ---
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace",
                   "Henry", "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia",
                   "Paul", "Quinn", "Ruby", "Sam", "Tara", "Uma", "Victor", "Wendy",
                   "Xander", "Yara", "Zach", "Amber", "Blake", "Cora", "Derek",
                   "Elena", "Felix", "Gina", "Hugo", "Isla", "Jake", "Kira", "Liam",
                   "Maya", "Nate", "Opal", "Pete", "Rosa", "Sean", "Tina", "Uri",
                   "Vera", "Wade", "Xena", "Yuri"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                  "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
                  "Jackson", "Martin"]
    customers = []
    for i in range(100):
        first = first_names[i % len(first_names)]
        last = last_names[i % len(last_names)]
        customers.append((f"{first} {last}", f"{first.lower()}.{last.lower()}.{i}@example.com"))
    cur.executemany(
        f"INSERT INTO {db_schema}.customers (name, email) VALUES (%s, %s)", customers
    )
    print(f"Inserted {len(customers)} customers")

    # --- Products (50) ---
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
                   "Tennis Ball", "Running Socks", "Gym Bag", "Towel", "Foam Roller"]
    }
    products = []
    for category, items in categories.items():
        for item in items:
            products.append((item, round(random.uniform(5.99, 299.99), 2), category))
    cur.executemany(
        f"INSERT INTO {db_schema}.products (name, price, category) VALUES (%s, %s, %s)", products
    )
    print(f"Inserted {len(products)} products")

    # --- Orders (22) ---
    cur.execute(f"""
    INSERT INTO {db_schema}.orders (customer_id, product_id, quantity, total, currency, order_date, status) VALUES
        (1,  1, 1, 1299.99, 'USD', '2024-03-01 10:05:00', 'delivered'),
        (1,  2, 1,   89.99, 'USD', '2024-03-05 14:22:00', 'delivered'),
        (2,  4, 1,  129.99, 'USD', '2024-03-08 09:00:00', 'shipped'),
        (3,  3, 1,  449.99, 'EUR', '2024-03-10 11:30:00', 'confirmed'),
        (4,  5, 2,  119.98, 'EUR', '2024-03-12 16:45:00', 'delivered'),
        (5,  2, 1,   89.99, 'GBP', '2024-03-15 08:10:00', 'shipped'),
        (6,  6, 3,  119.97, 'AED', '2024-03-16 12:00:00', 'pending'),
        (7,  1, 1, 1299.99, 'JPY', '2024-03-18 07:30:00', 'confirmed'),
        (8, 13, 2,  109.98, 'EUR', '2024-03-19 15:15:00', 'delivered'),
        (9, 10, 1,   99.99, 'EUR', '2024-03-20 10:00:00', 'shipped'),
        (10, 7, 1,   24.99, 'INR', '2024-03-21 13:30:00', 'delivered'),
        (11, 8, 1,   49.99, 'BRL', '2024-03-22 09:45:00', 'confirmed'),
        (12, 9, 2,   69.98, 'CNY', '2024-03-23 18:20:00', 'pending'),
        (1, 11, 1,   29.99, 'USD', '2024-03-24 11:05:00', 'shipped'),
        (2, 12, 2,   39.98, 'USD', '2024-03-25 14:00:00', 'delivered'),
        (3, 15, 1,   29.99, 'EUR', '2024-03-26 16:30:00', 'pending'),
        (4, 14, 1,   69.99, 'EUR', '2024-03-27 08:00:00', 'confirmed'),
        (5,  4, 1,  129.99, 'GBP', '2024-03-28 12:45:00', 'shipped'),
        (6,  3, 1,  449.99, 'AED', '2024-03-29 10:10:00', 'confirmed'),
        (7,  5, 1,   59.99, 'JPY', '2024-03-30 07:50:00', 'pending'),
        (8,  1, 1, 1299.99, 'EUR', '2024-03-31 15:00:00', 'confirmed'),
        (9,  2, 2,  179.98, 'EUR', '2024-04-01 09:30:00', 'shipped')
    ON CONFLICT DO NOTHING;
    """)
    cur.execute(f"SELECT count(*) FROM {db_schema}.orders")
    print(f"Inserted {cur.fetchone()[0]} orders")

    # --- Inventory (50 products x 1 warehouse each) ---
    warehouses = ["US-East", "US-West", "EU-Central"]
    inventory_rows = []
    for product_id in range(1, 51):
        inventory_rows.append(
            (product_id, random.randint(0, 200),
             warehouses[product_id % len(warehouses)], random.choice([5, 10, 15, 20]))
        )
    cur.executemany(
        f"INSERT INTO {db_schema}.inventory (product_id, quantity, warehouse, reorder_level) "
        f"VALUES (%s, %s, %s, %s)", inventory_rows
    )
    print(f"Inserted {len(inventory_rows)} inventory records")

    # --- Order Items (1-4 line items per order) ---
    cur.execute(f"SELECT id, product_id, quantity, total FROM {db_schema}.orders ORDER BY id")
    orders = cur.fetchall()
    order_items = []
    for order_id, orig_product_id, orig_qty, orig_total in orders:
        num_items = random.randint(1, 4)
        product_ids = random.sample(range(1, 51), num_items)
        if orig_product_id not in product_ids:
            product_ids[0] = orig_product_id
        remaining_total = float(orig_total)
        for i, pid in enumerate(product_ids):
            qty = random.randint(1, 3)
            if i == len(product_ids) - 1:
                unit_price = round(max(remaining_total / qty, 1.00), 2)
            else:
                unit_price = round(random.uniform(9.99, 199.99), 2)
                remaining_total -= round(unit_price * qty, 2)
            order_items.append((order_id, pid, qty, unit_price, round(unit_price * qty, 2)))
    cur.executemany(
        f"INSERT INTO {db_schema}.order_items (order_id, product_id, quantity, unit_price, line_total) "
        f"VALUES (%s, %s, %s, %s, %s)", order_items
    )
    print(f"Inserted {len(order_items)} order items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify the Seed

# COMMAND ----------

print("=" * 55)
print(f"  {project_name}")
print("=" * 55)
with conn.cursor() as cur:
    print(f"\nTables (schema: {db_schema}):")
    for table in ["customers", "products", "inventory", "orders", "order_items"]:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        print(f"   {db_schema}.{table:16s} {cur.fetchone()[0]:>6} rows")
    print("\nOrder status distribution:")
    cur.execute(f"""
        SELECT status, count(*), ROUND(AVG(total), 2)
        FROM {db_schema}.orders GROUP BY status ORDER BY status
    """)
    for status, cnt, avg in cur.fetchall():
        print(f"   {status:12s} {cnt:4d} orders  (avg ${avg})")
print("\n" + "=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Grant the Storefront App Database Access
# MAGIC
# MAGIC The DataCart Storefront app is already deployed. It runs as a **service principal (SP)** — an
# MAGIC automated identity, separate from your account. When the bundle deployed the app, it **bound
# MAGIC the app to this Lakebase project** (see `datacart-storefront/resources/datacart_storefront.app.yml`).
# MAGIC That binding auto-creates a Postgres **login role** for the SP and grants it `CONNECT`/`CREATE`
# MAGIC on the database — so the app can already connect.
# MAGIC
# MAGIC What the binding does **not** do is grant access to the `ecommerce` schema's tables — in
# MAGIC Postgres, object access must be granted explicitly. That's the last setup step: as the project
# MAGIC owner (which we're already connected as), grant the SP `USAGE` + `ALL` on `ecommerce`.
# MAGIC
# MAGIC ```
# MAGIC Permission hierarchy:   Database  →  Schema  →  Tables / Sequences
# MAGIC                         (binding)    (below)      (below)
# MAGIC ```
# MAGIC
# MAGIC | Grant | Purpose |
# MAGIC |---|---|
# MAGIC | `USAGE ON SCHEMA` | See the schema and its objects |
# MAGIC | `ALL ON ALL TABLES` | Read products/inventory, write orders |
# MAGIC | `ALL ON ALL SEQUENCES` | Needed for `SERIAL` auto-increment IDs |
# MAGIC | `ALTER DEFAULT PRIVILEGES` | Future tables (promotions in Lab 3.1, etc.) are auto-accessible |

# COMMAND ----------

# Look up the storefront app to find its service principal.
APP_NAME = f"storefront-{w.current_user.me().id}"
app_info = w.apps.get(APP_NAME)
SP_CLIENT_ID = app_info.service_principal_client_id
print(f"App:    {APP_NAME}")
print(f"App SP: {SP_CLIENT_ID}")

# Grant the SP schema access — reusing the owner connection from Step 4.
with conn.cursor() as cur:
    sp = f'"{SP_CLIENT_ID}"'
    cur.execute(f"GRANT USAGE ON SCHEMA {db_schema} TO {sp};")
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {db_schema} TO {sp};")
    cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {db_schema} TO {sp};")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {db_schema} GRANT ALL ON TABLES TO {sp};")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {db_schema} GRANT ALL ON SEQUENCES TO {sp};")
print(f"Granted USAGE + ALL on {db_schema}.* to SP {SP_CLIENT_ID}")

# COMMAND ----------

# Verify the grants landed.
with conn.cursor() as cur:
    cur.execute(f"""
        SELECT table_name, privilege_type
        FROM information_schema.table_privileges
        WHERE table_schema = '{db_schema}' AND grantee = '{SP_CLIENT_ID}'
        ORDER BY table_name, privilege_type
    """)
    grants = cur.fetchall()
print(f"{len(grants)} grants on {db_schema} tables for the SP.")
print(f"\nOpen the storefront and confirm it loads the catalog:\n   {app_info.url}")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Grant the App's SP Access to the Catalog
# MAGIC
# MAGIC The later labs sync data *between* Unity Catalog and Lakebase. Grant the storefront's service
# MAGIC principal `USE CATALOG` + `USE SCHEMA` on the catalog you created in Step 1 now, so those labs
# MAGIC don't have to. (Table-level grants are handled per-lab as new tables appear.)

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {UC_CATALOG} TO `{SP_CLIENT_ID}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {UC_CATALOG}.{UC_SCHEMA} TO `{SP_CLIENT_ID}`")
print(f"✅ Granted USE CATALOG + USE SCHEMA on {UC_CATALOG}.{UC_SCHEMA} to SP {SP_CLIENT_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC You configured the workshop widgets, created the `{catalog}` Unity Catalog (with a managed
# MAGIC location so synced tables and Lakehouse Sync work later), seeded the Lakebase `ecommerce`
# MAGIC schema, and granted the storefront app access. The storefront now serves products, stock,
# MAGIC and a working cart.
# MAGIC
# MAGIC As later labs add tables (promotions in Lab 2.1, reviews/loyalty in the bonus labs), the SP
# MAGIC inherits Lakebase access automatically via the `ALTER DEFAULT PRIVILEGES` grants — no need to
# MAGIC re-run this notebook.
# MAGIC
# MAGIC **Next:** Lab 2.1 — Reverse ETL with Synced Tables (sale badges and discounts appear in the app).
