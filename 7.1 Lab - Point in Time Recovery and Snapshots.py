# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 7.1: Point-in-Time Recovery (PITR) & Snapshots
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This lab covers two key data protection features of Lakebase: **Point-in-Time Recovery (PITR)**
# MAGIC and **Snapshots**. You'll learn the concepts behind each, then apply PITR hands-on by simulating
# MAGIC a production disaster and recovering from it.
# MAGIC
# MAGIC ## Why this lab matters for data-centric teams
# MAGIC
# MAGIC In a data-centric workshop, PITR is more than a database recovery feature — it's the test of
# MAGIC whether your downstream data flows are *resilient*. After Labs 4.1 and 5.1, you have a
# MAGIC federated catalog and a Lakehouse Sync pipeline both reading from production. When production
# MAGIC has an outage, **what happens to those flows, and do they recover automatically?** We'll
# MAGIC observe that explicitly during the disaster.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what PITR is and how the restore window works
# MAGIC 2. **Explain** what Snapshots are and when to use them vs. PITR
# MAGIC 3. **Create** a PITR recovery branch from a specific point in time
# MAGIC 4. **Restore** production data after an accidental destructive operation
# MAGIC 5. **Re-apply** post-recovery migrations to bring production back to full feature state
# MAGIC
# MAGIC > **Docs**: [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore) | [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Point-in-Time Recovery (PITR)
# MAGIC
# MAGIC PITR lets you restore a branch to **any exact moment** within a configurable window. It is powered by the same transaction log that Lakebase maintains for all root branches — no extra setup required.
# MAGIC
# MAGIC ### What is a Restore Window?
# MAGIC
# MAGIC The **restore window** is how far back in time you can recover. It is configurable from **0 to 30 days** and applies uniformly across *all* branches in the project.
# MAGIC
# MAGIC | Setting | Effect |
# MAGIC |---------|--------|
# MAGIC | Longer window (e.g. 30 days) | More recovery flexibility, but higher storage cost |
# MAGIC | Shorter window (e.g. 1 day) | Lower storage cost, but limited recovery range |
# MAGIC | 0 days | PITR is effectively disabled |
# MAGIC
# MAGIC > The restore window is a **project-level setting** — you cannot set different windows per branch.
# MAGIC
# MAGIC ### How to Perform a Restore
# MAGIC
# MAGIC PITR can be performed through the **Lakebase UI** or the **SDK** (as we'll do in this lab):
# MAGIC
# MAGIC 1. Open your project → **Backup & Restore**
# MAGIC 2. Select your source branch
# MAGIC 3. Use the date/time picker to choose your restore point
# MAGIC 4. Click **Restore to point in time**
# MAGIC
# MAGIC <img src="Includes/images/pitr/backup_restore_1.png" alt="Backup & Restore UI" width="800">
# MAGIC <img src="Includes/images/pitr/backup_restore_2.png" alt="Select branch and time" width="800">
# MAGIC <img src="Includes/images/pitr/backup_restore_3.png" alt="Confirm restore" width="800">
# MAGIC
# MAGIC ### What Happens After a Restore?
# MAGIC
# MAGIC A restore **never modifies your existing branch**. Instead:
# MAGIC
# MAGIC | Outcome | Detail |
# MAGIC |---------|--------|
# MAGIC | **New root branch created** | Contains the full database state from the specified point in time |
# MAGIC | **Original branch unchanged** | Your production branch keeps running without interruption |
# MAGIC | **Existing connections unaffected** | Apps connected to the original branch see no disruption |
# MAGIC | **Manual cutover required** | To use the restored data, update your app's connection string to the new branch |
# MAGIC
# MAGIC > Projects support a maximum of **3 root branches**. If you're at the limit, delete one before restoring.
# MAGIC
# MAGIC > A restore recovers **all databases** within a branch — you cannot restore a single database in isolation.
# MAGIC
# MAGIC ### When to Use PITR
# MAGIC
# MAGIC PITR is optimized for **unexpected, unplanned events** where you need to recover to a precise moment:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |----------|---------|
# MAGIC | Accidental data deletion | `DELETE FROM orders` without a `WHERE` clause |
# MAGIC | Destructive schema changes | `DROP TABLE inventory_main` run on the wrong environment |
# MAGIC | Application bug corruption | A code deploy that wrote bad data for 10 minutes |
# MAGIC | ~~Planned pre-change backup~~ | Use a **Snapshot** instead — it's more explicit and cheaper |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Snapshots
# MAGIC
# MAGIC A snapshot is an **explicit, named point-in-time capture** of a root branch. Unlike the continuous PITR transaction log, snapshots are discrete restore points you create on demand or on a schedule.
# MAGIC
# MAGIC ### Key Properties
# MAGIC
# MAGIC | Property | Detail |
# MAGIC |----------|--------|
# MAGIC | **Instant creation** | Snapshots are created immediately with minimal performance impact |
# MAGIC | **Root branches only** | Snapshots can only be taken on root (production-level) branches |
# MAGIC | **Manual limit** | Up to **10 manual snapshots** per project |
# MAGIC | **Scheduled snapshots** | Do not count toward the 10-snapshot limit |
# MAGIC | **Deletion is permanent** | Deleted snapshots cannot be recovered |
# MAGIC
# MAGIC ### Snapshot Schedules
# MAGIC
# MAGIC Automated snapshots run at regular intervals so you always have a recent restore point:
# MAGIC
# MAGIC 1. Open your project → **Backup & Restore**
# MAGIC 2. Click **Edit schedule**
# MAGIC 3. Choose frequency: **Daily** | **Weekly** | **Monthly**
# MAGIC 4. Set your retention period → **Update Schedule**
# MAGIC
# MAGIC > When a scheduled snapshot's retention period expires, it is **automatically deleted**. Manual snapshots persist until you explicitly delete them.
# MAGIC
# MAGIC ### Restoring from a Snapshot
# MAGIC
# MAGIC The same non-destructive restore model applies as PITR — a new root branch is created, named `branch_from_snapshot_<timestamp>`. Your original branch continues operating normally.
# MAGIC
# MAGIC <img src="Includes/images/pitr/restore_1.png" alt="Restore from Snapshot" width="800">
# MAGIC
# MAGIC ### When to Use Snapshots
# MAGIC
# MAGIC Snapshots are optimized for **planned, proactive backups**:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |----------|---------|
# MAGIC | Before risky schema migrations | `ALTER TABLE` that drops columns or changes types |
# MAGIC | Before a major deployment | Spring Sale go-live cutover |
# MAGIC | Regular scheduled backups | Daily snapshot at 02:00 UTC as a safety net |
# MAGIC | Compliance checkpoints | End-of-month data freeze for audit purposes |
# MAGIC | ~~Recovering from an unknown moment~~ | Use **PITR** — you need granularity beyond snapshot frequency |

# COMMAND ----------

# MAGIC %md
# MAGIC ## PITR vs. Snapshots — Quick Reference
# MAGIC
# MAGIC | | **PITR** | **Snapshots** |
# MAGIC |---|---|---|
# MAGIC | **Best for** | Unexpected events (accidents, bugs) | Planned events (deployments, migrations) |
# MAGIC | **Granularity** | Any second within the restore window | Discrete named points |
# MAGIC | **Window / Limit** | 0-30 days (project-wide) | 10 manual + unlimited scheduled |
# MAGIC | **Storage cost** | Increases with longer window | Per-snapshot overhead |
# MAGIC | **Setup required** | None — always on for root branches | Scheduled or manual creation |
# MAGIC | **Restore target** | New root branch | New root branch |
# MAGIC | **Original branch** | Unchanged | Unchanged |
# MAGIC | **Root branch limit** | Max 3 per project | Max 3 per project |
# MAGIC
# MAGIC > **Rule of thumb:** Use Snapshots *before* you make a change. Use PITR *after* something goes wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On: Disaster Recovery with PITR
# MAGIC
# MAGIC **The Challenge:**
# MAGIC A DevOps engineer accidentally executes `DROP TABLE orders` instead of a temp staging table.
# MAGIC The production app starts throwing 500 errors. Customer orders are gone. Revenue reporting is broken.
# MAGIC
# MAGIC **The Lakebase Solution: Point-in-Time Recovery**
# MAGIC With Lakebase PITR, the team creates a branch from **1 minute before the disaster**, verifies the
# MAGIC data is intact, and restores the orders table — all without downtime or backup tapes.
# MAGIC
# MAGIC ```
# MAGIC Timeline:
# MAGIC ──────────────────────────────────────────────────────────────────
# MAGIC   T-1min          T=0 (disaster)        T+5min (recovery)
# MAGIC   ───┬──────────────┬──────────────────────┬───
# MAGIC      │              │                      │
# MAGIC      │         DROP TABLE orders      CREATE PITR branch
# MAGIC      │                                from T-1min
# MAGIC      │                                     │
# MAGIC      └─── PITR branch has orders! ─────────┘
# MAGIC                                            │
# MAGIC                                     Copy data back
# MAGIC                                     to production
# MAGIC ```
# MAGIC
# MAGIC > **Key Insight:** Lakebase retains a full history of changes (configurable retention, default 24h).
# MAGIC > You can create a branch from **any point in time** within that window.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Install Dependencies & Configure Helpers

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time
import psycopg2

w = WorkspaceClient()

# Bundle-deployed Lakebase project (datacart-storefront/databricks.yml)
# Project name is auto-derived per user from ${workspace.current_user.id}
project_name = f"lakebase-workshop-{w.current_user.me().id}"
db_user = w.current_user.me().user_name

# List branches — the default 'production' branch should exist
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

print(f"📋 Branches in '{project_name}':")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    is_default = "⭐ default" if b.status and b.status.default else ""
    print(f"   • {branch_id} {is_default}")

# Get the production branch (the default one, or fallback to the first)
prod_branch = next(
    (b for b in branches if b.status and b.status.default),
    branches[0]
)
prod_branch_name = prod_branch.name
print(f"\n✅ Production branch: {prod_branch_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Connect to Any Branch

# COMMAND ----------

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

def connect_to_branch(branch_id, wait_seconds=300):
    """
    Connect to a Lakebase branch endpoint.
    Automatically creates a compute endpoint if none exists.
    """
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur

    branch_full = f"projects/{project_name}/branches/{branch_id}"

    # Check if an endpoint already exists
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
        print(f"   ✅ Compute endpoint created!")
        endpoints = list(w.postgres.list_endpoints(parent=branch_full))

    ep = endpoints[0]
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        print(f"⏳ Waiting for endpoint to become ready...")
        for i in range(wait_seconds // 10):
            time.sleep(10)
            endpoints = list(w.postgres.list_endpoints(parent=branch_full))
            ep = endpoints[0]
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                break
            print(f"   Still waiting... ({(i+1)*10}s)")

    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        raise Exception(f"Endpoint not ready for branch '{branch_id}' after {wait_seconds}s")

    host = ep.status.hosts.host

    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    branch_conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=db_user,
        password=cred.token,
        sslmode="require"
    )
    branch_conn.autocommit = True

    print(f"✅ Connected to branch '{branch_id}'")
    print(f"   Host: {host}")
    return branch_conn, host, ep.name

def delete_branch_safe(branch_id, max_retries=6, wait_between=30):
    """Delete a branch, retrying if the endpoint is still reconciling."""
    branch_full = f"projects/{project_name}/branches/{branch_id}"

    for attempt in range(max_retries):
        try:
            w.postgres.delete_branch(name=branch_full).wait()
            print(f"🗑️ Branch '{branch_id}' deleted.")
            return
        except Exception as e:
            if "reconciliation" in str(e).lower() and attempt < max_retries - 1:
                print(f"   ⏳ Endpoint still reconciling, retrying in {wait_between}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_between)
            else:
                raise

def print_table(cols, rows, max_rows=30):
    if not cols:
        print("(no results)")
        return
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(sep)
    print("|" + "|".join(f" {c:<{widths[i]}} " for i, c in enumerate(cols)) + "|")
    print(sep)
    for row in rows[:max_rows]:
        print("|" + "|".join(f" {str(v):<{widths[i]}} " for i, v in enumerate(row)) + "|")
    print(sep)

print("🔧 Helpers defined: connect_to_branch(), delete_branch_safe(), print_table()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Verify Production is Healthy
# MAGIC
# MAGIC Before we simulate the disaster, let's confirm production data is intact.
# MAGIC We'll record the current state so we can verify our recovery later.

# COMMAND ----------

conn_prod, _, _ = connect_to_branch('production')

# COMMAND ----------

print("📊 Current production state:\n")

with conn_prod.cursor() as cur:
    for table in ['customers', 'products', 'orders']:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   ✅ {table}: {count} rows")

    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    revenue = cur.fetchone()[0]
    print(f"\n   💰 Total revenue: ${revenue:,.2f}")

print("\n✅ Production is healthy. All tables present and populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 1: Everything is Healthy
# MAGIC
# MAGIC Open the **DataCart Storefront** now and note the full feature set:
# MAGIC - Products display with star ratings, stock badges, and "Earn X pts" labels
# MAGIC - Best Sellers and Top Rated sections work on the homepage
# MAGIC - Orders page shows order history with priority badges
# MAGIC - Cart and checkout function normally
# MAGIC
# MAGIC > Take a mental snapshot. In a moment, things will break.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Record the "Before" Timestamp
# MAGIC
# MAGIC This is the critical step. We record the current time **before** the disaster.
# MAGIC This timestamp will be used to create the PITR branch later.
# MAGIC
# MAGIC > In a real incident, you'd check your monitoring/alerting to determine when the problem started.

# COMMAND ----------

import datetime

with conn_prod.cursor() as cur:
    cur.execute("SELECT NOW()")
    before_timestamp = cur.fetchone()[0]

# Convert to epoch seconds for the SDK call
before_epoch = int(before_timestamp.timestamp())

print(f"⏱️  Recording 'before' timestamp: {before_timestamp}")
print(f"   Epoch seconds: {before_epoch}")
print(f"\n   This is our recovery point. Everything before this moment is safe.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Simulate the Disaster
# MAGIC
# MAGIC A DevOps engineer accidentally drops the `orders` table.
# MAGIC This is the kind of mistake that can happen with manual SQL scripts or misconfigured CI/CD.
# MAGIC
# MAGIC > **WARNING:** This will actually drop the orders table on the production branch!

# COMMAND ----------

print("💥 DISASTER SCENARIO: DevOps engineer runs the wrong script...")
print("   Intended: DROP TABLE staging.temp_orders")
print("   Actual:   DROP TABLE ecommerce.orders CASCADE\n")

with conn_prod.cursor() as cur:
    cur.execute(f"DROP TABLE IF EXISTS {db_schema}.orders CASCADE")

print("   🔴 TABLE DROPPED: ecommerce.orders")
print("\n   The production app is now broken. Customers can't see their orders.")
print("   Revenue reporting shows $0. The team is panicking.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Confirm the Damage
# MAGIC
# MAGIC Let's verify that the orders table is actually gone and queries fail.
# MAGIC
# MAGIC > Open the **DataCart Dashboard** and refresh — you should see orders = 0 and revenue = $0.

# COMMAND ----------

print("🔍 Checking production state after disaster:\n")

with conn_prod.cursor() as cur:
    for table in ['customers', 'products', 'orders']:
        try:
            cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
            count = cur.fetchone()[0]
            print(f"   ✅ {table}: {count} rows")
        except Exception as e:
            print(f"   🔴 {table}: MISSING — {str(e).splitlines()[0]}")
            # Reset the connection after the error
            conn_prod.rollback() if not conn_prod.autocommit else None

print("\n🚨 IMPACT: Orders table is gone. The app is serving errors.")
print("   Revenue: $0 | Orders: 0 | Customer orders page: broken")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 2: The Disaster
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the graceful degradation:
# MAGIC
# MAGIC | Page | What You'll See |
# MAGIC |------|----------------|
# MAGIC | **Home** | Top Rated still works, but Best Sellers shows "temporarily unavailable" |
# MAGIC | **Shop** | Products still browsable with stock badges and ratings |
# MAGIC | **Cart** | Your cart items are still there, but checkout shows an error |
# MAGIC | **Orders** | "Orders Service Unavailable" with a "Continue Shopping" button |
# MAGIC
# MAGIC > The storefront degrades gracefully — products are still browsable even though
# MAGIC > orders are gone. This is exactly what real customers would experience.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Flow Impact: What Happens to Federation and Lakehouse Sync?
# MAGIC
# MAGIC With `orders` dropped, run the cells below to **observe what your downstream data flows are
# MAGIC doing right now**. This is the moment that proves they're built on solid foundations.

# COMMAND ----------

# MAGIC %md
# MAGIC > Federation and sync respond to the outage in *different* ways:
# MAGIC > - Federation breaks immediately on every query — there is no copy.
# MAGIC > - Sync goes stale but its already-replicated data remains queryable in Delta.
# MAGIC >
# MAGIC > For consumers who can tolerate stale data during an outage, sync degrades more gracefully.
# MAGIC > For consumers who need live data, federation is honest about the failure. Both are fine
# MAGIC > behaviors — pick the right tool per use case.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Create a PITR Recovery Branch
# MAGIC
# MAGIC Here's where Lakebase saves the day. We create a new branch using the
# MAGIC `source_branch_time` parameter — this creates a branch from the state of
# MAGIC production **at the timestamp we recorded before the disaster**.
# MAGIC
# MAGIC The PITR branch is a full copy of production as it was at that moment,
# MAGIC including the orders table with all its data.
# MAGIC
# MAGIC > This is the same non-destructive restore model from the lecture: a **new root branch** is created, and the original production branch is unchanged.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Timestamp, Duration

PITR_BRANCH = "pitr-recovery"

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{PITR_BRANCH}").wait()
    print(f"🧹 Cleaned up existing PITR branch")
except Exception:
    pass

print(f"🔄 Creating PITR branch from production at {before_timestamp}...")
print(f"   Recovery point: {before_epoch} (epoch seconds)")

w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        source_branch_time=Timestamp(seconds=before_epoch),
        ttl=Duration(seconds=86400),  # 24-hour TTL for recovery branch
    )),
    branch_id=PITR_BRANCH,
).wait()

print(f"\n✅ PITR branch '{PITR_BRANCH}' created!")
print(f"   This branch contains production data from BEFORE the disaster.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Data on the PITR Branch
# MAGIC
# MAGIC Connect to the PITR branch and confirm the orders table exists with all its data.
# MAGIC
# MAGIC > Run the Setup SP Roles notebook now so the DataCart Dashboard can connect to the PITR branch.
# MAGIC > Then open the PITR Recovery page in the app to see the comparison.

# COMMAND ----------

conn_pitr, _, _ = connect_to_branch(PITR_BRANCH)

# COMMAND ----------

print("📊 PITR branch state (recovered data):\n")

pitr_counts = {}
with conn_pitr.cursor() as cur:
    for table in ['customers', 'products', 'orders']:
        try:
            cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
            count = cur.fetchone()[0]
            pitr_counts[table] = count
            print(f"   ✅ {table}: {count} rows")
        except Exception as e:
            pitr_counts[table] = 0
            print(f"   🔴 {table}: {str(e).splitlines()[0]}")

    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    revenue = cur.fetchone()[0]
    print(f"\n   💰 Revenue on PITR branch: ${revenue:,.2f}")

print("\n✅ All data is intact on the PITR branch!")
print("   The orders table was recovered from the point-in-time snapshot.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Restore Production
# MAGIC
# MAGIC Now we restore the orders table to production by:
# MAGIC 1. Getting the full schema from the PITR branch
# MAGIC 2. Recreating the table on production
# MAGIC 3. Copying data from the PITR branch using INSERT
# MAGIC
# MAGIC > In practice, you could also use `pg_dump`/`pg_restore` or application-level data migration.
# MAGIC > For this workshop, we'll use the simplest approach: recreate + INSERT.

# COMMAND ----------

# First, get the DDL from the PITR branch
print("🔄 Step 7a: Getting table schema from PITR branch...\n")

with conn_pitr.cursor() as cur:
    # Get column definitions
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()

    for col in columns:
        print(f"   {col[0]}: {col[1]} {'NOT NULL' if col[2] == 'NO' else 'NULL'}")

# COMMAND ----------

print("🔄 Step 7b: Recreating orders table on production...\n")

# Recreate the orders table on production with the same schema
with conn_prod.cursor() as cur:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.orders (
            id              SERIAL PRIMARY KEY,
            customer_id     INTEGER NOT NULL REFERENCES {db_schema}.customers(id),
            product_id      INTEGER NOT NULL REFERENCES {db_schema}.products(id),
            quantity         INTEGER NOT NULL DEFAULT 1,
            total            NUMERIC(10, 2) NOT NULL,
            currency         VARCHAR(3) NOT NULL DEFAULT 'USD',
            order_date       DATE NOT NULL DEFAULT CURRENT_DATE,
            status           VARCHAR(20) NOT NULL DEFAULT 'pending'
        )
    """)
    print("   ✅ Orders table recreated on production")

# COMMAND ----------

print("🔄 Step 7c: Copying data from PITR branch to production...\n")

# Read all orders from PITR branch
with conn_pitr.cursor() as cur:
    cur.execute(f"""
        SELECT id, customer_id, product_id, quantity, total, currency, order_date, status
        FROM {db_schema}.orders
        ORDER BY id
    """)
    orders_data = cur.fetchall()
    print(f"   📦 Read {len(orders_data)} orders from PITR branch")

# Insert into production
with conn_prod.cursor() as cur:
    for row in orders_data:
        cur.execute(f"""
            INSERT INTO {db_schema}.orders (id, customer_id, product_id, quantity, total, currency, order_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, row)

    # Reset the sequence to the correct value
    cur.execute(f"""
        SELECT setval(
            pg_get_serial_sequence('{db_schema}.orders', 'id'),
            (SELECT MAX(id) FROM {db_schema}.orders)
        )
    """)

print(f"   ✅ Inserted {len(orders_data)} orders into production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Recovery
# MAGIC
# MAGIC Let's confirm that production is fully restored.
# MAGIC
# MAGIC > Refresh the **DataCart Dashboard** — you should see orders and revenue are back!

# COMMAND ----------

print("📊 Production state AFTER recovery:\n")

with conn_prod.cursor() as cur:
    for table in ['customers', 'products', 'orders']:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   ✅ {table}: {count} rows")

    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    revenue = cur.fetchone()[0]
    print(f"\n   💰 Total revenue: ${revenue:,.2f}")

print("\n" + "=" * 60)
print("🎉 RECOVERY COMPLETE!")
print("   All orders have been restored from the PITR branch.")
print("   The DataCart Storefront is fully operational again.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 3: Recovery Complete
# MAGIC
# MAGIC Refresh the **DataCart Storefront**:
# MAGIC - Orders page is back with full order history
# MAGIC - Best Sellers on the homepage works again
# MAGIC - Checkout is functional again
# MAGIC
# MAGIC > The storefront detected the restored tables within 30 seconds and automatically recovered.
# MAGIC >
# MAGIC > **Note:** The priority badges on orders have disappeared — PITR restored the database
# MAGIC > to a point in time before Lab 6.3 added the `priority` column. This is expected behavior
# MAGIC > and illustrates that PITR is a true point-in-time snapshot, not just data recovery.
# MAGIC >
# MAGIC > Run the next step to re-apply the missing migrations and bring production back to its full feature set.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Re-apply Post-Recovery Migrations
# MAGIC
# MAGIC PITR restored the data, but the schema is from an earlier point in time. Any migrations
# MAGIC applied **after** the recovery point need to be replayed. This is the same pattern as
# MAGIC Lab 6.2 — idempotent DDL that's safe to run multiple times.
# MAGIC
# MAGIC This is a key operational takeaway: **after PITR recovery, always check which migrations
# MAGIC need to be re-applied.**

# COMMAND ----------

print("🔄 Re-applying post-recovery migrations...\n")

POST_RECOVERY_SQL = f"""
-- From Lab 6.3: Add email_verified to customers
ALTER TABLE {db_schema}.customers
ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE;

UPDATE {db_schema}.customers
SET email_verified = TRUE
WHERE id % 3 = 0;

-- From Lab 6.3: Add priority to orders
ALTER TABLE {db_schema}.orders
ADD COLUMN IF NOT EXISTS priority VARCHAR(10) DEFAULT 'normal';

UPDATE {db_schema}.orders
SET priority = CASE
    WHEN total > 500 THEN 'high'
    WHEN total > 200 THEN 'medium'
    ELSE 'normal'
END;
"""

with conn_prod.cursor() as cur:
    cur.execute(POST_RECOVERY_SQL)

# Verify
with conn_prod.cursor() as cur:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position
    """)
    order_cols = [row[0] for row in cur.fetchall()]

    cur.execute(f"""
        SELECT priority, COUNT(*) FROM {db_schema}.orders GROUP BY priority ORDER BY priority
    """)
    priorities = cur.fetchall()

print("✅ Post-recovery migrations applied!")
print(f"   Orders columns: {order_cols}")
print(f"   Priority distribution:")
for row in priorities:
    print(f"      {row[0]:10s} {row[1]:4d} orders")

print(f"\n🎉 Production is fully restored with ALL features!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 4: Full Feature Restore
# MAGIC
# MAGIC Refresh the **DataCart Storefront** one more time:
# MAGIC - Priority badges are back on the Orders page
# MAGIC - Verified badge is back in the navbar
# MAGIC - All features from Labs 3.3 and 3.4 are restored
# MAGIC
# MAGIC > **Key Takeaway:** PITR recovers your data to a point in time. Post-recovery,
# MAGIC > you re-apply any migrations that happened after the recovery point — just like
# MAGIC > replaying a git rebase after resetting a branch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Cleanup (Optional)
# MAGIC
# MAGIC > Uncomment to clean up the PITR branch.

# COMMAND ----------

# Uncomment to clean up:
# conn_pitr.close()
# conn_prod.close()
# delete_branch_safe(PITR_BRANCH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Record timestamp** | Captured `NOW()` before the disaster as our recovery point |
# MAGIC | **Simulate disaster** | `DROP TABLE orders CASCADE` on production |
# MAGIC | **Confirm damage** | Orders gone, revenue $0, app broken |
# MAGIC | **Create PITR branch** | Branch from production at the pre-disaster timestamp |
# MAGIC | **Verify PITR data** | All 22 orders intact on the recovery branch |
# MAGIC | **Restore production** | Recreated table + copied data from PITR branch |
# MAGIC | **Re-apply migrations** | Replayed post-recovery DDL for full feature set |
# MAGIC | **Verify recovery** | Production fully restored — 22 orders, revenue back |
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - **PITR** — recover to any second within the restore window, non-destructive (new branch created)
# MAGIC - **Snapshots** — planned, named restore points for proactive backups before risky operations
# MAGIC - **PITR vs. Snapshots** — use Snapshots *before* changes, use PITR *after* something goes wrong
# MAGIC - **Post-recovery migrations** — always check which migrations need to be re-applied after PITR
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Lakebase retains full history** — you can recover from any point within the retention window (default 24h)
# MAGIC 2. **PITR branches are instant** — no waiting for backup restores or point-in-time replay
# MAGIC 3. **Zero-copy snapshots** — the PITR branch doesn't duplicate data, it references the historical state
# MAGIC 4. **Non-destructive recovery** — you verify data on the branch before touching production
# MAGIC 5. **Record timestamps proactively** — monitoring and alerting help identify the right recovery point
