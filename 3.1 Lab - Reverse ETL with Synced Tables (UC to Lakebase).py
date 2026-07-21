# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.1: Reverse ETL with Synced Tables — UC to Lakebase
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Direction 1 of 3: Pushing Reference Data from the Lakehouse into OLTP
# MAGIC
# MAGIC This lab is the first of three modules that map out **how data moves between the Lakehouse and
# MAGIC Lakebase** — the central concern for data engineers and architects:
# MAGIC
# MAGIC | Direction | Lab | Mechanism | Use Case |
# MAGIC |---|---|---|---|
# MAGIC | **UC → Lakebase** (this lab) | **3.1** | **Synced Tables** (managed CDC) | Push curated analytics data into OLTP so apps can serve it with low latency |
# MAGIC | **Live read-through** | **4.1** | **Lakebase registered in UC** (Lakehouse Federation) | Query live OLTP from a SQL warehouse without any ETL |
# MAGIC | **Lakebase → UC** | **5.1** | **Lakehouse Sync** | Continuously stream OLTP into Delta for high-volume analytics |
# MAGIC
# MAGIC In this module you'll move curated analytics data from the Databricks Lakehouse into the Lakebase
# MAGIC Postgres database so the live DataCart Storefront can serve it to shoppers with millisecond
# MAGIC latency. By the end, the storefront's "Spring Sale" badges will appear — driven entirely by a
# MAGIC Delta table managed in Unity Catalog.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what Reverse ETL is and why Lakebase simplifies it
# MAGIC 2. **Understand** Synced Tables and the three sync modes (Snapshot, Triggered, Continuous)
# MAGIC 3. **Create** a Delta table in Unity Catalog for application data
# MAGIC 4. **Set up** a synced table that flows data from the lakehouse to Lakebase
# MAGIC 5. **Update** the Delta table and propagate changes to the live application
# MAGIC
# MAGIC > **Docs**: [Synced Tables](https://docs.databricks.com/aws/en/oltp/projects/sync-tables)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Reverse ETL?
# MAGIC
# MAGIC Traditional ETL moves raw data *into* a lakehouse for analysis. **Reverse ETL is the return journey** — it takes curated, analytics-grade data *out* of the lakehouse and pushes it into an OLTP engine, where applications can query it for low-latency use cases.
# MAGIC
# MAGIC ![reverse_etl_wo_lakebase.png](Includes/images/reverse_etl/reverse_etl_w_o_lakebase.png)
# MAGIC
# MAGIC Most teams face challenges when handling this on their own: managing custom pipelines as data changes, inconsistent governance models, and friction for developers and the business.
# MAGIC
# MAGIC **Lakebase simplifies this process.** When using Lakebase as part of the Data Intelligence Platform, sync to the operational engine is **managed by Databricks** — developers focus on building apps, not managing infrastructure.
# MAGIC
# MAGIC ![reverse_etl_w_lakebase.png](Includes/images/reverse_etl/reverse_etl_w_lakebase.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Synced Tables
# MAGIC
# MAGIC ![synced_tables.png](Includes/images/reverse_etl/synced_tables.png)
# MAGIC
# MAGIC A **synced table** is a managed, read-only copy of a Unity Catalog table inside Lakebase. Creating one links two objects:
# MAGIC
# MAGIC | Object | Location | Purpose |
# MAGIC |--------|----------|---------|
# MAGIC | **Unity Catalog table** | Lakehouse (Delta) | Source of truth; managed by the sync pipeline |
# MAGIC | **Postgres table** | Lakebase database | App-facing replica; automatically kept in sync |
# MAGIC
# MAGIC For example, syncing `analytics.gold.user_profiles` creates a Postgres table you query as:
# MAGIC ```sql
# MAGIC SELECT * FROM "gold"."user_profiles_synced";
# MAGIC ```
# MAGIC
# MAGIC > Synced tables in Postgres are **read-only**. Only create indexes or drop the table (after removing it from Unity Catalog). Direct writes risk breaking sync integrity.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Sync Modes
# MAGIC
# MAGIC Lakebase offers three sync modes, each with its own **latency**, **cost**, and **source requirements** trade-off:
# MAGIC
# MAGIC | Mode | How it works | Latency | Cost | CDF Required? |
# MAGIC |------|-------------|---------|------|---------------|
# MAGIC | **Snapshot** | Replaces the entire Postgres table on every refresh | Minutes (batch) | Low | No |
# MAGIC | **Triggered** | Applies only the changes since the last sync, on a schedule | Minutes (scheduled) | Medium | Yes |
# MAGIC | **Continuous** | Streams changes into Postgres in near real-time | Seconds | High | Yes |
# MAGIC
# MAGIC **Throughput reference:**
# MAGIC
# MAGIC | Mode | Rows/second per Capacity Unit |
# MAGIC |------|-------------------------------|
# MAGIC | Snapshot | ~2,000 |
# MAGIC | Triggered / Continuous | ~150 |
# MAGIC
# MAGIC > Snapshot is ~13x faster per CU because it bulk-loads without tracking individual row changes.
# MAGIC
# MAGIC ### Decision Guide
# MAGIC
# MAGIC ```
# MAGIC Does your app need data fresh within seconds?
# MAGIC   ├── Yes  →  Continuous
# MAGIC   └── No
# MAGIC        │
# MAGIC        Does more than ~10% of the source table change per sync cycle?
# MAGIC          ├── Yes  →  Snapshot  (full reload is more efficient)
# MAGIC          └── No   →  Triggered  (delta sync is cheaper)
# MAGIC ```
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px;">When to use which?</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <ul>
# MAGIC       <li><strong>Snapshot</strong> — Initial data loads, daily batch refreshes, tables without CDF, tables with high churn (>10% rows change between syncs)</li>
# MAGIC       <li><strong>Triggered</strong> — Dashboard data, hourly recommendation refreshes, reporting tables where "fresh within N minutes" is sufficient</li>
# MAGIC       <li><strong>Continuous</strong> — Live inventory, fraud detection, personalization at request time, any feature where stale data causes visible errors</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Creating a Synced Table
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - An active Lakebase project with at least one branch and endpoint
# MAGIC - `USE_SCHEMA` and `CREATE_TABLE` permissions on the target schema
# MAGIC - For Triggered or Continuous modes: Change Data Feed (CDF) enabled on the source table
# MAGIC
# MAGIC **Steps (UI):**
# MAGIC 1. Navigate to **Catalog** → select your Unity Catalog table
# MAGIC 2. Click **Create** → **Synced table**
# MAGIC 3. Fill in: synced table name, compute type (Lakebase Serverless), sync mode, project, branch, database, primary key
# MAGIC 4. Click **Create**
# MAGIC
# MAGIC **Enable CDF (Triggered/Continuous only):**
# MAGIC ```sql
# MAGIC ALTER TABLE analytics.gold.user_profiles
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC **Operational limits:**
# MAGIC
# MAGIC | Limit | Value |
# MAGIC |-------|-------|
# MAGIC | Max total data across all synced tables | 8 TB |
# MAGIC | Recommended max per refreshable table | 1 TB |
# MAGIC | DB connections per synced table | Up to 16 |
# MAGIC | Schema evolution support | Additive changes only (Triggered/Continuous) |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Data Types & Incompatibility
# MAGIC
# MAGIC Lakebase automatically maps Unity Catalog types to Postgres equivalents. Most types map cleanly, but there are a few things to watch out for:
# MAGIC
# MAGIC | Unity Catalog Type | Postgres Type | Notes |
# MAGIC |--------------------|--------------|-------|
# MAGIC | `STRING` | `TEXT` | Null bytes (0x00) not allowed |
# MAGIC | `ARRAY`, `MAP`, `STRUCT` | `JSONB` | Nested types serialized as JSON |
# MAGIC | `GEOGRAPHY`, `GEOMETRY` | Not supported | |
# MAGIC | `VARIANT`, `OBJECT` | Not supported | |
# MAGIC
# MAGIC The most common issue is **null bytes (`0x00`)** — valid in Delta strings but rejected by Postgres `TEXT`. Clean at the source:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT REPLACE(column_name, CAST(CHAR(0) AS STRING), '') AS cleaned_column
# MAGIC FROM analytics.gold.user_profiles;
# MAGIC ```
# MAGIC
# MAGIC > Always clean problematic columns **in the source Unity Catalog table**, not in Postgres.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On: Spring Sale Promotions via Reverse ETL
# MAGIC
# MAGIC **The Challenge:**
# MAGIC DataCart's marketing team has prepared Spring Sale promotions in the data warehouse —
# MAGIC product discounts, sale badges, and limited-time offers computed by analytics pipelines.
# MAGIC These need to appear on the customer-facing storefront **instantly**, without
# MAGIC requiring application code changes or manual database inserts.
# MAGIC
# MAGIC **The Lakebase Solution: Synced Tables (Reverse ETL)**
# MAGIC Lakebase synced tables create a managed copy of a Unity Catalog Delta table inside
# MAGIC Lakebase Postgres. The data flows automatically from the lakehouse to the application
# MAGIC database, enabling sub-10ms queries from the storefront.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      DATA LAKEHOUSE                            │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  Unity Catalog: <your-catalog>                          │   │
# MAGIC │  │  └── ecommerce.promotions (Delta table)                 │   │
# MAGIC │  │       • badge_text, discount_pct, sale_price            │   │
# MAGIC │  │       • Updated by marketing analytics pipelines        │   │
# MAGIC │  └─────────────────────────┬───────────────────────────────┘   │
# MAGIC │                            │ Synced Table (Reverse ETL)        │
# MAGIC │                            ▼                                   │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  Lakebase: production branch                            │   │
# MAGIC │  │  └── ecommerce.promotions (Postgres, read-only)         │   │
# MAGIC │  │       • Auto-synced from Delta table                    │   │
# MAGIC │  │       • Sub-10ms queries for the storefront             │   │
# MAGIC │  └─────────────────────────┬───────────────────────────────┘   │
# MAGIC │                            │                                   │
# MAGIC └────────────────────────────┼───────────────────────────────────┘
# MAGIC                              ▼
# MAGIC                    ┌──────────────────┐
# MAGIC                    │ DataCart          │
# MAGIC                    │ Storefront       │
# MAGIC                    │ • Sale badges    │
# MAGIC                    │ • Discount prices│
# MAGIC                    │ • Promo banners  │
# MAGIC                    └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Install Dependencies & Configure

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC For this lab we'll seed a Delta table in the Lakehouse and sync it into Lakebase for the
# MAGIC storefront. Set your target catalog below — the lab creates the `ecommerce` schema inside
# MAGIC it for you (you just need `CREATE SCHEMA` privileges on the catalog).

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time
import psycopg2

w = WorkspaceClient()

# Bundle-deployed Lakebase project (datacart-storefront/databricks.yml)
# Project name is auto-derived per user from ${workspace.current_user.id}
project_name = f"lakebase-workshop-{w.current_user.me().id}"
db_user = w.current_user.me().user_name

# Unity Catalog configuration — set the catalog before running
UC_CATALOG = "<add-your-catalog-name-here>"
UC_SCHEMA = "ecommerce"
UC_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.promotions"

# Lakebase configuration
db_schema = "ecommerce"

# Create the ecommerce schema in the chosen catalog (idempotent).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")

print(f"✅ SDK initialized")
print(f"   User:         {w.current_user.me().user_name}")
print(f"   UC Schema:    {UC_CATALOG}.{UC_SCHEMA} (created if missing)")
print(f"   UC Table:     {UC_TABLE}")
print(f"   Lakebase:     {project_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Promotions Delta Table
# MAGIC
# MAGIC The marketing team maintains a `promotions` table in Unity Catalog. This table
# MAGIC contains active promotional offers — sale badges, discount percentages, and sale prices
# MAGIC for products in the Spring Sale.
# MAGIC
# MAGIC We create this as a Delta table so it integrates with the lakehouse ecosystem
# MAGIC (governance, lineage, versioning) while being syncable to Lakebase for low-latency serving.
# MAGIC
# MAGIC > Note that we enable **Change Data Feed** (`delta.enableChangeDataFeed = true`) — this is
# MAGIC > required for **Triggered** and **Continuous** sync modes as discussed above.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_TABLE} (
        id INT,
        product_id INT,
        badge_text STRING,
        discount_pct DECIMAL(5,2),
        sale_price DECIMAL(10,2),
        promo_type STRING,
        is_active BOOLEAN,
        start_date TIMESTAMP,
        end_date TIMESTAMP
    )
    USING DELTA
    COMMENT 'Spring Sale promotions - synced to Lakebase for storefront display'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"✅ Created Delta table: {UC_TABLE}")
print(f"   Change Data Feed enabled (required for Triggered/Continuous sync modes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Seed Spring Sale Promotions
# MAGIC
# MAGIC The marketing team has identified 12 products for the Spring Sale with varying
# MAGIC discount levels. These span multiple categories to create a visually rich storefront.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, TimestampType
from datetime import datetime, timedelta
from decimal import Decimal

now = datetime.now()
end_date = now + timedelta(days=14)  # 2-week sale

promotions = [
    # Electronics deals
    Row(id=1,  product_id=1,  badge_text="SPRING SALE",   discount_pct=Decimal("20.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=2,  product_id=2,  badge_text="HOT DEAL",      discount_pct=Decimal("30.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=3,  product_id=6,  badge_text="SPRING SALE",   discount_pct=Decimal("15.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Clothing deals
    Row(id=4,  product_id=11, badge_text="CLEARANCE",     discount_pct=Decimal("40.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=5,  product_id=14, badge_text="SPRING SALE",   discount_pct=Decimal("25.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Books deals
    Row(id=6,  product_id=21, badge_text="LIMITED TIME",  discount_pct=Decimal("10.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=7,  product_id=23, badge_text="SPRING SALE",   discount_pct=Decimal("20.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Home deals
    Row(id=8,  product_id=31, badge_text="FLASH SALE",    discount_pct=Decimal("35.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=9,  product_id=33, badge_text="SPRING SALE",   discount_pct=Decimal("15.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    # Sports deals
    Row(id=10, product_id=41, badge_text="HOT DEAL",      discount_pct=Decimal("25.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=11, product_id=45, badge_text="SPRING SALE",   discount_pct=Decimal("20.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=12, product_id=50, badge_text="CLEARANCE",     discount_pct=Decimal("50.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("badge_text", StringType(), True),
    StructField("discount_pct", DecimalType(5, 2), True),
    StructField("sale_price", DecimalType(10, 2), True),
    StructField("promo_type", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
])

df = spark.createDataFrame(promotions, schema=schema)

# Compute sale_price from product prices in Lakebase
# For now, write without sale_price — we'll compute it after sync or in a separate step
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UC_TABLE)

print(f"✅ Seeded {len(promotions)} Spring Sale promotions")
display(spark.table(UC_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compute Sale Prices
# MAGIC
# MAGIC We need the actual product prices from Lakebase to compute sale prices.
# MAGIC Let's connect and update the Delta table with pre-computed sale prices.

# COMMAND ----------

def connect_to_branch(branch_id, wait_seconds=300):
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur
    branch_full = f"projects/{project_name}/branches/{branch_id}"
    min_cu, max_cu, suspend_timeout_seconds = 0.5, 4.0, 1800
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    if not endpoints:
        ep_id = f"ep-{branch_id}"
        print(f"🔄 Creating compute endpoint for branch '{branch_id}'...")
        w.postgres.create_endpoint(
            parent=branch_full,
            endpoint=Endpoint(spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
                suspend_timeout_duration=Dur(seconds=suspend_timeout_seconds)
            )),
            endpoint_id=ep_id
        ).wait()
        endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    ep = endpoints[0]
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        for i in range(wait_seconds // 10):
            time.sleep(10)
            endpoints = list(w.postgres.list_endpoints(parent=branch_full))
            ep = endpoints[0]
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                break
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    conn = psycopg2.connect(host=host, port=5432, dbname="databricks_postgres",
                            user=db_user, password=cred.token, sslmode="require")
    conn.autocommit = True
    print(f"✅ Connected to branch '{branch_id}'")
    return conn, host, ep.name

conn_prod, _, _ = connect_to_branch("production")

# COMMAND ----------

conn_prod, _, _ = connect_to_branch("production")

# COMMAND ----------

# Get product prices from Lakebase
with conn_prod.cursor() as cur:
    cur.execute(f"SELECT id, price FROM {db_schema}.products ORDER BY id")
    product_prices = {row[0]: float(row[1]) for row in cur.fetchall()}

# Update Delta table with computed sale prices
from pyspark.sql.functions import col, round as spark_round, lit

promo_df = spark.table(UC_TABLE)
# Create a mapping DataFrame
price_rows = [Row(product_id=pid, original_price=price) for pid, price in product_prices.items()]
prices_df = spark.createDataFrame(price_rows)

# Join and compute sale_price
updated_df = (
    promo_df.join(prices_df, "product_id", "left")
    .withColumn("sale_price",
        spark_round(col("original_price") * (1 - col("discount_pct") / 100), 2)
    )
    .drop("original_price")
)

updated_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UC_TABLE)

print("✅ Sale prices computed and saved!")
display(spark.sql(f"""
    SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
    FROM {UC_TABLE}
    ORDER BY discount_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Sync the Promotions Table to Production
# MAGIC
# MAGIC Now set up the **reverse ETL pipeline** that syncs the Delta `promotions` table directly
# MAGIC into the production Lakebase branch. The storefront reads from production, so once the
# MAGIC sync completes the sale badges go live.
# MAGIC
# MAGIC **Follow these steps in the Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Catalog** in the left sidebar
# MAGIC 2. Browse to your catalog > schema > `promotions`
# MAGIC 3. Click on the `promotions` table
# MAGIC 4. Click **Create** > **Synced table**
# MAGIC 5. In the dialog:
# MAGIC    - **Table name**: input **promotions_synced_prod**
# MAGIC    - **Database type**: Select **Lakebase Serverless (Autoscaling)**
# MAGIC    - **Project**: Select your workshop project (`lakebase-workshop-<FirstName>-<LastName>`)
# MAGIC    - **Branch**: Select **production**
# MAGIC    - **Sync mode**: Select **Snapshot** (full copy, simplest for demo)
# MAGIC    - **Primary key**: Verify `id` is selected
# MAGIC 6. Click **Create**
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px;">Sync Mode Reminder</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     We're using <strong>Snapshot</strong> mode here (full table replacement on each refresh). Since we enabled Change Data Feed in Step 1, you could switch to <strong>Triggered</strong> (scheduled incremental) or <strong>Continuous</strong> (near real-time streaming) later.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC **Wait for the sync to complete before continuing.** Check status in the Catalog UI.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Deploy synced tables as code
# MAGIC
# MAGIC The UI flow above is the fastest way to provision a synced table. For production, you'd
# MAGIC typically declare the sync alongside the rest of your infrastructure as code so it lives
# MAGIC in Git and deploys through CI/CD.
# MAGIC
# MAGIC > **Declarative Automation Bundles**: not supported for synced tables today — coming soon.
# MAGIC > For now, use Terraform if you want an IaC-managed sync.
# MAGIC
# MAGIC #### Terraform
# MAGIC
# MAGIC The Databricks Terraform provider includes `databricks_postgres_synced_table`, which
# MAGIC references the production branch of your Lakebase project directly.
# MAGIC ([typical project setup](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/terraform-typical-project))
# MAGIC
# MAGIC ```hcl
# MAGIC # Reference the bundle-deployed Lakebase Autoscaling project.
# MAGIC data "databricks_current_user" "me" {}
# MAGIC
# MAGIC locals {
# MAGIC   project_id     = "lakebase-workshop-${data.databricks_current_user.me.id}"
# MAGIC   production_arn = "projects/${local.project_id}/branches/production"
# MAGIC }
# MAGIC
# MAGIC variable "uc_catalog" {
# MAGIC   description = "The UC catalog containing your ecommerce.promotions Delta table"
# MAGIC   type        = string
# MAGIC }
# MAGIC
# MAGIC resource "databricks_postgres_synced_table" "promotions" {
# MAGIC   synced_table_id = "${var.uc_catalog}.ecommerce.promotions_synced_prod"
# MAGIC
# MAGIC   spec = {
# MAGIC     branch                             = local.production_arn
# MAGIC     postgres_database                  = "databricks_postgres"
# MAGIC     source_table_full_name             = "${var.uc_catalog}.ecommerce.promotions"
# MAGIC     primary_key_columns                = ["id"]
# MAGIC     scheduling_policy                  = "SNAPSHOT"   # SNAPSHOT | TRIGGERED | CONTINUOUS
# MAGIC     create_database_objects_if_missing = true
# MAGIC
# MAGIC     new_pipeline_spec = {
# MAGIC       storage_catalog = var.uc_catalog
# MAGIC       storage_schema  = "ecommerce"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Apply with:
# MAGIC
# MAGIC ```bash
# MAGIC terraform apply -var="uc_catalog=<your-catalog>"
# MAGIC ```
# MAGIC
# MAGIC > **Heads up.** `scheduling_policy` accepts `SNAPSHOT`, `TRIGGERED`, or `CONTINUOUS`. Triggered
# MAGIC > and Continuous require **Change Data Feed** on the source Delta table (we enabled this in
# MAGIC > Step 1). The `new_pipeline_spec.storage_catalog` / `storage_schema` is where the sync
# MAGIC > pipeline persists checkpoints — it must be a UC catalog where you have `CREATE TABLE` rights.
# MAGIC
# MAGIC For the rest of this workshop we use the UI-created synced table. The next step
# MAGIC (granting the SP access) is the same regardless of which path created the synced table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Grant SP Access to the Synced Table
# MAGIC
# MAGIC **This is a critical step.** Synced tables are created by the Lakebase sync pipeline —
# MAGIC a different internal role than your user account. This means the `ALTER DEFAULT PRIVILEGES`
# MAGIC grants from Lab 2 **do not apply** to synced tables, because those defaults only cover
# MAGIC tables created by your user.
# MAGIC
# MAGIC We need to re-run `GRANT ALL ON ALL TABLES` to include the newly synced `promotions` table.
# MAGIC Without this, the storefront's service principal can't read the promotions.
# MAGIC
# MAGIC > **Key takeaway:** After every new synced table is created, re-grant table permissions
# MAGIC > to the app's SP. This is a one-time step per synced table.

# COMMAND ----------

# Get the app's SP client ID
APP_NAME = f"storefront-{w.current_user.me().id}"
app_info = w.apps.get(APP_NAME)
SP_CLIENT_ID = app_info.service_principal_client_id
print(f"App SP: {SP_CLIENT_ID}")

# Connect as the project owner to grant permissions
conn_prod, _, _ = connect_to_branch("production")

with conn_prod.cursor() as cur:
    sp_role = f'"{SP_CLIENT_ID}"'

    # Re-grant ALL on ALL tables — this picks up the new synced table
    cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {db_schema} TO {sp_role};")
    print(f"✅ Granted ALL on ALL tables in {db_schema} (includes synced tables)")

    cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {db_schema} TO {sp_role};")
    print(f"✅ Granted ALL on ALL sequences in {db_schema}")

print(f"\n🎉 SP can now read the promotions synced table!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Promotions on Production

# COMMAND ----------

# Find the promotions table (may be named 'promotions' or 'promotions_synced_prod')
with conn_prod.cursor() as cur:
    cur.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{db_schema}' AND table_name LIKE '%promotions%'
        ORDER BY table_name
    """)
    promo_tables = [r[0] for r in cur.fetchall()]
    print(f"📋 Promotions tables found: {promo_tables}")

    if promo_tables:
        promo_table = promo_tables[0]
        cur.execute(f"""
            SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
            FROM {db_schema}.{promo_table}
            WHERE is_active = true
            ORDER BY discount_pct DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        print(f"\n✅ Promotions live on production! {len(rows)} active promotions in '{promo_table}':\n")
        for row in rows:
            r = dict(zip(cols, row))
            print(f"   Product {r['product_id']:3d} | {r['badge_text']:14s} | -{r['discount_pct']}% | Sale: ${r['sale_price']}")
    else:
        print("⏳ No promotions table found on production yet.")
        print("   Make sure you completed Step 6 (sync to production branch).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Spring Sale Goes Live!
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the promotions appearing:
# MAGIC
# MAGIC 1. **Homepage** — A new "Spring Sale Deals" section shows promoted products with discount badges
# MAGIC 2. **Product cards** — Sale badges (e.g., "SPRING SALE -20%") appear on promoted products
# MAGIC 3. **Product cards** — Original prices are crossed out with sale prices shown
# MAGIC 4. **Product detail** — Promotion alert with discount details
# MAGIC 5. **Cart** — Promoted items show the discounted sale price
# MAGIC
# MAGIC > The storefront auto-detected the new `promotions` table within 30 seconds.
# MAGIC > No app redeployment was needed — the reverse ETL pipeline handled everything.
# MAGIC
# MAGIC > **Key insight:** The marketing team updated a Delta table in the lakehouse.
# MAGIC > The synced table pipeline pushed the data to Lakebase. The storefront detected
# MAGIC > the new table and rendered promotions. **Zero application code changes required.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Update Promotions (Simulate Marketing Campaign Change)
# MAGIC
# MAGIC The marketing team decides to add a **flash sale** on more products and increase
# MAGIC the discount on an existing promotion. Let's update the Delta table and trigger a re-sync.

# COMMAND ----------

from pyspark.sql.functions import when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, TimestampType
from datetime import datetime, timedelta
from decimal import Decimal

now = datetime.now()
end_date = now + timedelta(days=14) # 2-week sale

# Add new flash sale promotions
new_promos = [
    Row(id=13, product_id=3, badge_text="FLASH SALE", discount_pct=Decimal("45.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=14, product_id=7, badge_text="FLASH SALE", discount_pct=Decimal("35.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
    Row(id=15, product_id=35, badge_text="MEGA DEAL", discount_pct=Decimal("60.00"), sale_price=None, promo_type="percentage", is_active=True, start_date=now, end_date=end_date),
]

schema = StructType([
    StructField("id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("badge_text", StringType()),
    StructField("discount_pct", DecimalType(5, 2)),
    StructField("sale_price", DecimalType(10, 2)),
    StructField("promo_type", StringType()),
    StructField("is_active", BooleanType()),
    StructField("start_date", TimestampType()),
    StructField("end_date", TimestampType()),
])

new_df = spark.createDataFrame(new_promos, schema=schema)

# Compute sale prices for new promos
new_with_prices = (
    new_df.join(
        spark.createDataFrame([Row(product_id=pid, original_price=price) for pid, price in product_prices.items()]),
        "product_id", "left"
    )
    .withColumn("sale_price", spark_round(col("original_price") * (1 - col("discount_pct") / 100), 2))
    .drop("original_price")
)

# Append new promos to the existing table
new_with_prices.write.mode("append").saveAsTable(UC_TABLE)

# Also update an existing promo - increase Product 1 discount from 20% to 35%
spark.sql(f"""
    UPDATE {UC_TABLE}
    SET discount_pct = 35.00,
        badge_text = 'MEGA DEAL',
        sale_price = ROUND(sale_price / (1 - 0.20) * (1 - 0.35), 2)
    WHERE id = 1
""")

print("\u2705 Marketing team updated promotions:")
print("   \u2022 Added 3 new flash sale products")
print("   \u2022 Increased Product 1 discount from 20% to 35%")
display(spark.table(UC_TABLE).orderBy("discount_pct", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Trigger Re-Sync
# MAGIC
# MAGIC For **Snapshot** mode, we need to manually trigger a refresh. For **Triggered** or
# MAGIC **Continuous** modes, this would happen automatically — matching the sync mode
# MAGIC decision guide from the lecture section.

# COMMAND ----------

SYNCED_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.promotions_synced_prod"

# COMMAND ----------

# Trigger the sync pipeline to pick up the changes
try:
    table_info = w.database.get_synced_database_table(name=SYNCED_TABLE)
    pipeline_id = table_info.data_synchronization_status.pipeline_id
    print(f"🔄 Triggering sync pipeline: {pipeline_id}")
    w.pipelines.start_update(pipeline_id=pipeline_id)
    print("✅ Sync triggered! Waiting for completion...")

    # Wait for the sync to complete
    for i in range(30):
        time.sleep(10)
        table_info = w.database.get_synced_database_table(name=UC_TABLE)
        status = table_info.data_synchronization_status
        if status and status.last_sync_time:
            print(f"\n✅ Sync completed!")
            break
        print(f"   Still syncing... ({(i+1)*10}s)")
except Exception as e:
    print(f"⚠️ Could not trigger sync automatically: {e}")
    print("   You can trigger a refresh manually in the Catalog UI:")
    print(f"   Navigate to {UC_TABLE} → Synced table tab → Refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify Updated Promotions in Lakebase

# COMMAND ----------

conn_prod, _, _ = connect_to_branch("production")

with conn_prod.cursor() as cur:
    cur.execute(f"""
        SELECT id, product_id, badge_text, discount_pct, sale_price, is_active
        FROM {db_schema}.promotions_synced_prod
        WHERE is_active = true
        ORDER BY discount_pct DESC
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

print(f"✅ Updated promotions synced to Lakebase! {len(rows)} active promotions:\n")
for row in rows:
    r = dict(zip(cols, row))
    print(f"   Product {r['product_id']:3d} | {r['badge_text']:14s} | -{r['discount_pct']}% | Sale: ${r['sale_price']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Updated Promotions
# MAGIC
# MAGIC Refresh the **DataCart Storefront** and observe:
# MAGIC
# MAGIC - **3 new products** now show flash sale badges
# MAGIC - **Product 1** now shows "MEGA DEAL -35%" instead of "SPRING SALE -20%"
# MAGIC - The storefront updated **without any code changes or redeployment**
# MAGIC
# MAGIC > This is the power of reverse ETL: your analytics team modifies a Delta table,
# MAGIC > the sync pipeline pushes it to Lakebase, and the application reflects the change
# MAGIC > automatically. The storefront code never changed — it just queries the same
# MAGIC > database and renders whatever promotions are active.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Cleanup (Optional)
# MAGIC
# MAGIC > Uncomment to remove the synced table and Delta table.

# COMMAND ----------

# Uncomment to clean up:
# spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE}")
# print(f"🗑️ Dropped Delta table: {UC_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Create Delta table** | `promotions` table in Unity Catalog with Spring Sale data |
# MAGIC | **Create synced table** | Reverse ETL pipeline syncs Delta → Lakebase Postgres |
# MAGIC | **Verify sync** | Queried `ecommerce.promotions` directly in Lakebase |
# MAGIC | **Storefront impact** | Sale badges, discount prices, promo banners appeared automatically |
# MAGIC | **Update & re-sync** | Added new promotions, triggered refresh, storefront updated |
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - **Reverse ETL** — moving curated analytics data from the lakehouse to OLTP for low-latency serving
# MAGIC - **Synced Tables** — managed, read-only copies of Unity Catalog tables in Lakebase
# MAGIC - **Sync Modes** — Snapshot (batch), Triggered (scheduled incremental), Continuous (real-time streaming)
# MAGIC - **Decision guide** — choose Snapshot for high churn, Triggered for periodic freshness, Continuous for real-time
# MAGIC - **Data type mapping** — Unity Catalog to Postgres type compatibility and null byte handling
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Synced tables bridge the lakehouse and application layers** — analytics data serves live applications
# MAGIC 2. **No application code changes needed** — the storefront's schema detector picks up new tables automatically
# MAGIC 3. **Change Data Feed** enables incremental sync for near real-time updates
# MAGIC 4. **Unity Catalog governance** applies — access control, lineage, and auditing on the source data
# MAGIC 5. **Sub-10ms query latency** — Lakebase serves the synced data with OLTP-grade performance
