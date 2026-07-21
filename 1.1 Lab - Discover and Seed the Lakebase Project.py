# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1.1: Discover and Seed the Lakebase Autoscaling Project
# MAGIC
# MAGIC This notebook gives you a hands-on tour of your Lakebase Autoscaling project. You'll discover
# MAGIC the project, connect via OAuth, seed an e-commerce schema, and explore Postgres system
# MAGIC metadata — the same metadata your analytics tooling will see once we register Lakebase in
# MAGIC Unity Catalog (Lab 4.1).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what a Lakebase Autoscaling project is and how it differs from Lakebase Provisioned
# MAGIC 2. **Discover** your Lakebase project in the workspace
# MAGIC 3. **Connect** to a Lakebase database using OAuth token authentication
# MAGIC 4. **Create and populate** PostgreSQL tables using native PL/pgSQL with features like SERIAL keys and constraints
# MAGIC 5. **Explore PostgreSQL system metadata** through `pg_catalog`, `information_schema`, and `pg_stat_statements`
# MAGIC 6. **Understand** how Lakebase becomes addressable from the Lakehouse (direct connect, UC registration, Lakehouse Sync)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Discovers your Lakebase project
# MAGIC 2. Connects via OAuth token authentication (fully automated)
# MAGIC 3. Seeds 5 tables with realistic e-commerce data
# MAGIC 4. Explores PostgreSQL system metadata (`pg_catalog`, `information_schema`, `pg_stat_statements`)
# MAGIC 5. Verifies everything is ready for the remaining workshop labs
# MAGIC
# MAGIC > **Setup expectation**: Your Lakebase project and the DataCart Storefront app should already be
# MAGIC > set up before running this notebook. If they aren't, see `WORKSHOP_SETUP.md`.
# MAGIC
# MAGIC > **Docs**: [Lakebase Autoscaling Projects](https://docs.databricks.com/aws/en/oltp/projects/) | [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) | [API Reference](https://docs.databricks.com/api/workspace/postgres)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px;">
# MAGIC     Warning - PLEASE READ IF RUNNING IN YOUR OWN WORKSPACE
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC       <ul>
# MAGIC         <li>
# MAGIC           <strong>When you finish the lab, make sure to delete your Lakebase resources</strong>
# MAGIC           to avoid unnecessary costs and to prevent hitting workspace limits.
# MAGIC         </li>
# MAGIC         <li>
# MAGIC           Each workspace supports a <strong>maximum of 1000 Lakebase Autoscaling Database Projects</strong>.
# MAGIC           Leaving unused instances running might incur unnecessary costs.
# MAGIC         </li>
# MAGIC       </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Lakebase Autoscaling?
# MAGIC
# MAGIC Lakebase Autoscaling is **100% standard PostgreSQL** with automatic scaling, instant branching, and deep integration with the Databricks platform. It is designed for **OLTP (Online Transaction Processing)** workloads — the transactional layer that powers applications, APIs, and real-time services.
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px;">
# MAGIC     Lakebase Autoscaling vs. Lakebase Provisioned
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     There are two versions of Lakebase. In this workshop we use <strong>Lakebase Autoscaling</strong>, which is the recommended option. It supports additional features such as instant branching, scale-to-zero, and point-in-time restore. Use Lakebase Provisioned only if Autoscaling is not available in your region or you need a specific feature only available in the provisioned offering.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC **Key capabilities of Lakebase Autoscaling:**
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---|---|
# MAGIC | **Autoscaling compute** | Automatically adjusts compute resources (CUs) based on workload demand |
# MAGIC | **Scale-to-zero** | Suspends inactive computes to minimize costs |
# MAGIC | **Instant branching** | Create isolated development/test branches from production in seconds |
# MAGIC | **Read replicas** | Scale read operations with read-only replicas |
# MAGIC | **Point-in-time restore** | Restore or branch from any point within your restore window |
# MAGIC | **Unity Catalog integration** | Register Lakebase databases in Unity Catalog for federated queries and governance |
# MAGIC | **PostgreSQL 17** | Full compatibility with standard PostgreSQL clients, tools, and extensions |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Prerequisites
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">PREREQUISITES</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - **Cluster**: Any Databricks cluster with Python 3.10+
# MAGIC - **Region**: Workspace must be in a supported region:
# MAGIC   `us-east-1`, `us-east-2`, `eu-central-1`, `eu-west-1`, `eu-west-2`,
# MAGIC   `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`
# MAGIC - Permission to create a **Lakebase Autoscaling Project** in your workspace
# MAGIC - Unity Catalog enabled in your workspace
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ## Architecture After Setup
# MAGIC ```
# MAGIC Lakebase Project: lakebase-workshop-<your-user-id>             ← set up before the workshop
# MAGIC └── production (default branch)
# MAGIC     └── ecommerce (schema)                    ← created in this lab
# MAGIC         ├── customers    (100 rows)
# MAGIC         ├── products     (50 rows)
# MAGIC         ├── inventory    (50 rows - one per product)
# MAGIC         ├── orders       (22 rows)
# MAGIC         └── order_items  (~55 rows - line items per order)
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize SDK & Configuration
# MAGIC
# MAGIC The `WorkspaceClient` auto-authenticates when running inside a Databricks notebook —
# MAGIC no tokens or secrets needed.
# MAGIC
# MAGIC ### Two names, one project
# MAGIC
# MAGIC Lakebase projects have **two names** that you'll see in different places:
# MAGIC
# MAGIC | Name | Where you see it | What it is |
# MAGIC |---|---|---|
# MAGIC | **`project_id`** | SDK calls, URLs, the `project_name` variable below | Resource ID — must be lowercase + hyphens (DNS-compliant). Auto-derived from your numeric Databricks user ID, so it looks like `lakebase-workshop-6530815146371371` |
# MAGIC | **`display_name`** | Lakebase UI, Catalog Explorer, "Lakebase Postgres" page | Human-readable label — auto-derived from your first + last name, so it looks like `Lakebase Workshop — Jane Doe` |
# MAGIC
# MAGIC Both refer to **the same Lakebase project**. The labs work with the `project_id` because that's what the API expects.
# MAGIC
# MAGIC > **If you see "Lakebase Workshop — Your Name" in the UI but the notebook prints `lakebase-workshop-<long-number>`, that's expected — they're the same project.**

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Your Lakebase project — name auto-derived from your numeric user ID.
# This is the project_id (DNS-compliant, used by the SDK), not the display_name.
project_name = f"lakebase-workshop-{w.current_user.me().id}"

# Fixed configuration
db_schema = "ecommerce"

print(f"✅ SDK initialized")
print(f"   Workspace: {w.config.host}")
print(f"   User:      {w.current_user.me().user_name}")
print(f"")
print("📋 Configuration:")
print(f"   Project ID:        {project_name}        ← used by SDK / URLs")
print(f"   DB Schema:         {db_schema}")
print(f"")
print(f"💡 In the Lakebase UI, this project shows as 'Lakebase Workshop — <your name>'.")
print(f"   Both names point at the same project.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Discover Your Lakebase Project
# MAGIC
# MAGIC Your workspace already has a Lakebase Autoscaling project set up for the workshop. In this
# MAGIC step we **look it up by name** so the rest of the lab can use it. This is the data-centric
# MAGIC workshop model: infrastructure ready in advance, schema work in the labs.
# MAGIC
# MAGIC **What's already in place:**
# MAGIC - A **PostgreSQL 17** project with **0.5–2 CU autoscaling** and **300s scale-to-zero**
# MAGIC - A default **`production` branch** with a primary R/W compute endpoint
# MAGIC - A **Postgres role** for your Databricks identity (the project owner)
# MAGIC - A default **`databricks_postgres`** database
# MAGIC
# MAGIC > If the project doesn't exist yet, see `WORKSHOP_SETUP.md` for setup instructions.
# MAGIC
# MAGIC > **Docs:** [Lakebase Autoscaling Projects](https://docs.databricks.com/aws/en/oltp/projects/)

# COMMAND ----------

# Look up your Lakebase project by name.
existing_projects = list(w.postgres.list_projects())
project_obj = next(
    (p for p in existing_projects if p.name == f"projects/{project_name}"),
    None,
)

if project_obj is None:
    raise RuntimeError(
        f"Project '{project_name}' not found. See WORKSHOP_SETUP.md for setup instructions."
    )

project_uid = project_obj.uid
workspace_host = w.config.host.rstrip("/")
lakebase_url = f"{workspace_host}/lakebase/projects/{project_uid}"

print(f"✅ Found project '{project_name}'")
print(f"   UID: {project_uid}")
print(f"\n🔗 Lakebase UI: {lakebase_url}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Understanding Your Project Settings
# MAGIC
# MAGIC After your project is created, you can explore its settings in the Lakebase UI by clicking **Settings** in the left panel. Here's what you'll find:
# MAGIC
# MAGIC <img src="./Includes/images/core_concepts/landing_page.png"
# MAGIC      alt="Lakebase Project Dashboard"
# MAGIC      width="1200">
# MAGIC
# MAGIC #### Settings Overview
# MAGIC
# MAGIC | Setting | Description |
# MAGIC |---|---|
# MAGIC | **General** | Project name and unique **Project ID** |
# MAGIC | **Compute Defaults** | Min/Max CU and scale-to-zero timeout applied when creating new computes. Setting both min and max to the same value disables autoscaling. |
# MAGIC | **Instant Restore** | The restore window length — enables point-in-time restore, historical queries, and branching from past states. Increasing this increases storage costs across all branches. |
# MAGIC | **Project Permissions** | Controls who can access and manage the Lakebase project (create branches, manage computes, view connection details). |
# MAGIC
# MAGIC <img src="./Includes/images/core_concepts/compute_defaults.png"
# MAGIC      alt="Compute Defaults Settings"
# MAGIC      width="500">
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px;">
# MAGIC     Project Permissions vs. Database Access
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <strong>Project permissions</strong> control Lakebase platform actions (creating branches, managing computes), while <strong>database access</strong> is controlled by Postgres roles and their associated permissions. These are two separate layers.
# MAGIC     See <a href="https://docs.databricks.com/aws/en/oltp/projects/manage-roles" style="color: #1976d2;">Manage Postgres roles</a> and <a href="https://docs.databricks.com/aws/en/oltp/projects/manage-permissions" style="color: #1976d2;">Manage permissions</a>.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Project & Get Main Branch
# MAGIC
# MAGIC Every Lakebase project comes with a default `production` branch. Let's confirm it exists
# MAGIC and get its compute endpoint (we'll need the host to connect via `psycopg2`).

# COMMAND ----------

import time

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

# Get the compute endpoint for the production branch
endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))

if not endpoints:
    print("⏳ Compute endpoint not ready yet. Waiting...")
    for i in range(30):
        time.sleep(10)
        endpoints = list(w.postgres.list_endpoints(parent=prod_branch_name))
        if endpoints:
            break
        print(f"   Still waiting... ({(i+1)*10}s)")

if endpoints:
    prod_endpoint = endpoints[0]
    prod_endpoint_name = prod_endpoint.name
    prod_host = prod_endpoint.status.hosts.host
    print(f"✅ Compute endpoint ready!")
    print(f"   Endpoint: {prod_endpoint_name}")
    print(f"   Host: {prod_host}")
    print(f"   Port: 5432")
    print(f"   Database: databricks_postgres")
else:
    raise Exception("Compute endpoint not available after 5 minutes. Check the Lakebase UI for project status.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 4: Connect to the Database
# MAGIC
# MAGIC Lakebase supports **OAuth token-based authentication** — your Databricks identity is used
# MAGIC to generate short-lived database tokens. No passwords to manage!
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. When you create a project, a Postgres role for your Databricks identity is **automatically created**
# MAGIC 2. This role owns the default `databricks_postgres` database and is a member of `databricks_superuser`
# MAGIC 3. The SDK generates an OAuth token using `generate_database_credential`
# MAGIC 4. We connect via `psycopg2` using the token as the password
# MAGIC
# MAGIC > **Token lifetime**: Tokens auto-expire, so they're generated fresh each time.
# MAGIC > This is more secure than static passwords and fully automated.
# MAGIC
# MAGIC > **Docs**: [Query with Python in notebooks](https://docs.databricks.com/aws/en/oltp/projects/notebooks-python)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px;">
# MAGIC     Connection Methods
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     Lakebase supports multiple connection methods:
# MAGIC     <ul>
# MAGIC       <li><strong>OAuth role</strong> (used in this lab) — short-lived tokens tied to your Databricks identity</li>
# MAGIC       <li><strong>Native Postgres role</strong> — traditional username/password authentication</li>
# MAGIC     </ul>
# MAGIC     You can view connection snippets for various languages and frameworks in the Lakebase UI by clicking the <strong>Connect</strong> button on any branch.
# MAGIC     <br><br>
# MAGIC     See <a href="https://docs.databricks.com/aws/en/oltp/projects/connect-overview#choose-your-authentication-method" style="color: #1976d2;">Choose your authentication method</a> for details.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <img src="./Includes/images/core_concepts/connection_database.png"
# MAGIC      alt="Connection Dialog"
# MAGIC      width="400">

# COMMAND ----------

import psycopg2

# Connect as the current Databricks user (the project owner)
db_user = w.current_user.me().user_name

# Generate a fresh OAuth token
cred = w.postgres.generate_database_credential(endpoint=prod_endpoint_name)
db_token = cred.token
print(f"🔑 OAuth token generated (expires: {cred.expire_time})")

# Connect to the database
try:
    conn = psycopg2.connect(
        host=prod_host,
        port=5432,
        dbname="databricks_postgres",
        user=db_user,
        password=db_token,
        sslmode="require"
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

    print(f"✅ Connected to Lakebase!")
    print(f"   PostgreSQL: {version[:60]}...")
    print(f"   Host: {prod_host}")
    print(f"   User: {db_user}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print(f"\n   Troubleshooting:")
    print(f"   1. Is the endpoint active? Check the Lakebase UI.")
    print(f"   2. Does your user have permissions on this project?")
    print(f"   3. Check the Lakebase UI → Roles tab to verify your role exists.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed the E-Commerce Schema
# MAGIC
# MAGIC Now we'll create tables directly in the Lakebase database using native **PL/pgSQL** commands.
# MAGIC
# MAGIC In production environments, applications (like Databricks Apps) typically write transactional data to Lakebase OLTP databases — customer intake forms, order processing, real-time inventory updates, etc. For this workshop, we seed the data programmatically.
# MAGIC
# MAGIC We'll create 5 tables that model a realistic e-commerce application:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
# MAGIC │  customers   │     │   products   │     │  inventory   │
# MAGIC │──────────────│     │──────────────│     │──────────────│
# MAGIC │ id (PK)      │     │ id (PK)      │◄────│ product_id   │
# MAGIC │ name         │     │ name         │     │ quantity     │
# MAGIC │ email        │     │ price        │     │ warehouse    │
# MAGIC │ created_at   │     │ category     │     │ reorder_lvl  │
# MAGIC └──────┬───────┘     └──────┬───────┘     │ last_restock │
# MAGIC        │                    │              └──────────────┘
# MAGIC        │                    │
# MAGIC ┌──────┴───────┐  ┌─────┴─────────┐
# MAGIC │   orders     │  │ order_items    │
# MAGIC │──────────────│  │───────────────│
# MAGIC │ id (PK)      │  │ id (PK)       │
# MAGIC │ customer_id  │  │ order_id      │
# MAGIC │ order_date   │  │ product_id    │
# MAGIC │ status       │  │ quantity      │
# MAGIC │ total        │  │ unit_price    │
# MAGIC │ currency     │  │ line_total    │
# MAGIC └──────────────┘  └───────────────┘
# MAGIC ```
# MAGIC
# MAGIC **PostgreSQL features used:**
# MAGIC - `SERIAL` — auto-incrementing primary keys
# MAGIC - `REFERENCES` — foreign key constraints
# MAGIC - `CHECK` — data validation constraints
# MAGIC - `UNIQUE` — uniqueness constraints
# MAGIC - `ON DELETE CASCADE` — cascading deletes
# MAGIC
# MAGIC > This schema is intentionally rich — later labs will evolve it
# MAGIC > (adding columns, new tables, backfilling data) to demonstrate branching workflows.

# COMMAND ----------

# --- Schema SQL (embedded for portability) ---

SEED_SCHEMA_SQL = f"""
-- Create schema (avoids permission issues on 'public')
CREATE SCHEMA IF NOT EXISTS {db_schema};

-- Set search path so all subsequent commands use this schema
SET search_path TO {db_schema};

-- Drop tables if they exist (idempotent)
DROP TABLE IF EXISTS {db_schema}.order_items CASCADE;
DROP TABLE IF EXISTS {db_schema}.inventory CASCADE;
DROP TABLE IF EXISTS {db_schema}.orders CASCADE;
DROP TABLE IF EXISTS {db_schema}.products CASCADE;
DROP TABLE IF EXISTS {db_schema}.customers CASCADE;

-- Customers
CREATE TABLE {db_schema}.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Products
CREATE TABLE {db_schema}.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50)
);

-- Inventory (stock levels per product)
CREATE TABLE {db_schema}.inventory (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES {db_schema}.products(id),
    quantity INT NOT NULL DEFAULT 0,
    warehouse VARCHAR(50) NOT NULL DEFAULT 'US-East',
    reorder_level INT NOT NULL DEFAULT 10,
    last_restocked TIMESTAMP DEFAULT NOW(),
    UNIQUE(product_id, warehouse)
);

-- Orders (header)
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

-- Order Items (line items per order)
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

print(f"✅ Schema '{db_schema}' created with tables:")
print(f"   • {db_schema}.customers")
print(f"   • {db_schema}.products")
print(f"   • {db_schema}.inventory")
print(f"   • {db_schema}.orders")
print(f"   • {db_schema}.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Seed Sample Data
# MAGIC
# MAGIC We'll insert realistic e-commerce data:
# MAGIC - **100 customers** with unique names and emails
# MAGIC - **50 products** across 5 categories (Electronics, Clothing, Books, Home, Sports)
# MAGIC - **50 inventory records** with stock levels and warehouse locations
# MAGIC - **22 orders** with varying statuses and currencies
# MAGIC - **~55 order items** (line items per order, 1-4 items each)
# MAGIC
# MAGIC > This data will be used across all workshop labs. Later labs will add a
# MAGIC > `loyalty_tier` column and backfill it based on order history.

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
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}.{i}@example.com"
        customers.append((name, email))

    cur.executemany(
        f"INSERT INTO {db_schema}.customers (name, email) VALUES (%s, %s)",
        customers
    )
    print(f"✅ Inserted {len(customers)} customers")

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
            price = round(random.uniform(5.99, 299.99), 2)
            products.append((item, price, category))

    cur.executemany(
        f"INSERT INTO {db_schema}.products (name, price, category) VALUES (%s, %s, %s)",
        products
    )
    print(f"✅ Inserted {len(products)} products")

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
    """
    )

    cur.execute(f"""
    SELECT o.id, u.name AS customer, p.name AS product, o.quantity,
           o.total, o.currency, o.status
    FROM orders o
    JOIN customers    u ON u.id = o.customer_id
    JOIN products p ON p.id = o.product_id
    ORDER BY o.id;
    """)
    cols, rows = [d[0] for d in cur.description], cur.fetchall()

print(f"✅ Inserted {len(rows)} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed Inventory & Order Items

# COMMAND ----------

with conn.cursor() as cur:

    # --- Inventory (50 products x 1 warehouse each) ---
    warehouses = ["US-East", "US-West", "EU-Central"]
    inventory_rows = []
    for product_id in range(1, 51):
        qty = random.randint(0, 200)
        wh = warehouses[product_id % len(warehouses)]
        reorder = random.choice([5, 10, 15, 20])
        inventory_rows.append((product_id, qty, wh, reorder))

    cur.executemany(
        f"INSERT INTO {db_schema}.inventory (product_id, quantity, warehouse, reorder_level) "
        f"VALUES (%s, %s, %s, %s)",
        inventory_rows
    )
    print(f"✅ Inserted {len(inventory_rows)} inventory records")

    # --- Order Items (line items for each order) ---
    # First get the order count
    cur.execute(f"SELECT id, product_id, quantity, total FROM {db_schema}.orders ORDER BY id")
    orders = cur.fetchall()

    order_items = []
    for order_id, orig_product_id, orig_qty, orig_total in orders:
        # Each order gets 1-4 line items
        num_items = random.randint(1, 4)
        product_ids = random.sample(range(1, 51), num_items)
        # Make sure the original product is included
        if orig_product_id not in product_ids:
            product_ids[0] = orig_product_id

        remaining_total = float(orig_total)
        for i, pid in enumerate(product_ids):
            qty = random.randint(1, 3)
            if i == len(product_ids) - 1:
                # Last item gets the remaining total
                unit_price = round(max(remaining_total / qty, 1.00), 2)
                line_total = round(unit_price * qty, 2)
            else:
                unit_price = round(random.uniform(9.99, 199.99), 2)
                line_total = round(unit_price * qty, 2)
                remaining_total -= line_total

            order_items.append((order_id, pid, qty, unit_price, line_total))

    cur.executemany(
        f"INSERT INTO {db_schema}.order_items (order_id, product_id, quantity, unit_price, line_total) "
        f"VALUES (%s, %s, %s, %s, %s)",
        order_items
    )
    print(f"✅ Inserted {len(order_items)} order items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Explore PostgreSQL System Metadata
# MAGIC
# MAGIC One of the key advantages of Lakebase is that it behaves **exactly like standard PostgreSQL**. Let's verify this by exploring the system metadata — the same `pg_catalog` and `information_schema` views you'd use in any PostgreSQL database.
# MAGIC
# MAGIC This exploration demonstrates that Lakebase gives you a real PostgreSQL environment, not a proprietary abstraction.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Explore Available Schemas
# MAGIC
# MAGIC Every Lakebase database includes these standard PostgreSQL schemas:
# MAGIC - **`__db_system`** — Databricks system metadata
# MAGIC - **`information_schema`** — SQL standard metadata views
# MAGIC - **`pg_catalog`** — PostgreSQL system catalog
# MAGIC - **`pg_toast`** — Internal TOAST (The Oversized-Attribute Storage Technique) tables
# MAGIC - **`public`** — Default user schema
# MAGIC - **`ecommerce`** — The schema we just created

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name;
    """)
    schemas = cur.fetchall()

print("📋 Available schemas in databricks_postgres:")
for row in schemas:
    print(f"   • {row[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Query the PostgreSQL System Catalog (`pg_catalog`)
# MAGIC
# MAGIC The `pg_catalog` schema contains PostgreSQL internal system tables and views that provide metadata about database objects, users, and configuration. These are the same system tables you'd find in any standard PostgreSQL installation.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute("""
        SELECT schemaname, tablename, tableowner
        FROM pg_tables
        WHERE schemaname = 'pg_catalog'
        ORDER BY tablename
        LIMIT 15;
    """)
    rows = cur.fetchall()

print("📋 Sample pg_catalog tables (first 15):")
print(f"   {'Schema':<15} {'Table':<30} {'Owner'}")
print(f"   {'-'*15} {'-'*30} {'-'*20}")
for row in rows:
    print(f"   {row[0]:<15} {row[1]:<30} {row[2]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c. Query `information_schema`
# MAGIC
# MAGIC The `information_schema` provides standardized views of database metadata, making it easier to write portable queries across different database systems. Let's see what user-created tables exist.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', '__db_system', 'pg_toast')
        ORDER BY table_schema, table_name;
    """)
    rows = cur.fetchall()

print("📋 User tables via information_schema:")
print(f"   {'Schema':<15} {'Table':<25} {'Type'}")
print(f"   {'-'*15} {'-'*25} {'-'*15}")
for row in rows:
    print(f"   {row[0]:<15} {row[1]:<25} {row[2]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d. Inspect Column Details
# MAGIC
# MAGIC Let's examine the column metadata for one of our tables to see the PostgreSQL-specific data types and constraints in action.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{db_schema}' AND table_name = 'orders'
        ORDER BY ordinal_position;
    """)
    rows = cur.fetchall()

print(f"📋 Column details for {db_schema}.orders:")
print(f"   {'Column':<15} {'Type':<20} {'Nullable':<10} {'Default'}")
print(f"   {'-'*15} {'-'*20} {'-'*10} {'-'*30}")
for row in rows:
    default = str(row[3])[:30] if row[3] else ""
    print(f"   {row[0]:<15} {row[1]:<20} {row[2]:<10} {default}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7e. Query Performance Stats (`pg_stat_statements`)
# MAGIC
# MAGIC `pg_stat_statements` records aggregate execution statistics for every distinct query the
# MAGIC database has run — total calls, total time, mean time, rows returned. It's the single most
# MAGIC useful view for finding slow queries on a Postgres server, and it's pre-installed on every
# MAGIC Lakebase Autoscaling project.
# MAGIC
# MAGIC We'll come back to this in **Lab 8 — Monitoring**, but it's worth seeing now so you know
# MAGIC the data is already accumulating.

# COMMAND ----------

# with conn.cursor() as cur:
#     cur.execute("""
#         SELECT
#             substring(query, 1, 80) AS query_excerpt,
#             calls,
#             round(total_exec_time::numeric, 2) AS total_ms,
#             round(mean_exec_time::numeric, 2) AS mean_ms,
#             rows
#         FROM pg_stat_statements
#         WHERE query NOT LIKE '%pg_stat_statements%'
#         ORDER BY total_exec_time DESC
#         LIMIT 5;
#     """)
#     rows = cur.fetchall()

# print("📋 Top 5 queries by total execution time (pg_stat_statements):")
# print(f"   {'Query (first 80 chars)':<82} {'Calls':>6} {'Total ms':>10} {'Mean ms':>8} {'Rows':>6}")
# print(f"   {'-'*82} {'-'*6} {'-'*10} {'-'*8} {'-'*6}")
# for row in rows:
#     print(f"   {row[0]:<82} {row[1]:>6} {row[2]:>10} {row[3]:>8} {row[4]:>6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Setup
# MAGIC
# MAGIC Let's confirm everything is in place — tables exist, data is populated,
# MAGIC and the project is ready for the remaining workshop labs.

# COMMAND ----------

print("=" * 60)
print(f"  PROJECT SUMMARY: {project_name}")
print("=" * 60)

with conn.cursor() as cur:
    # Table row counts
    tables = ["customers", "products", "inventory", "orders", "order_items"]
    print(f"\n📊 Tables (schema: {db_schema}):")
    for table in tables:
        cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
        count = cur.fetchone()[0]
        print(f"   • {db_schema}.{table:20s} {count:>6} rows")

    # Sample data preview
    print("\n👤 Sample Customers (first 5):")
    cur.execute(f"SELECT id, name, email FROM {db_schema}.customers ORDER BY id LIMIT 5")
    for row in cur.fetchall():
        print(f"   {row[0]:3d} | {row[1]:20s} | {row[2]}")

    # Order stats
    print("\n📦 Order Status Distribution:")
    cur.execute(f"""
        SELECT status, count(*) as cnt, ROUND(AVG(total), 2) as avg_total
        FROM {db_schema}.orders GROUP BY status ORDER BY status
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:4d} orders  (avg ${row[2]})")

    # Top categories
    print("\n🏷️  Product Categories:")
    cur.execute(f"""
        SELECT category, count(*) as cnt,
               ROUND(MIN(price), 2) as min_price,
               ROUND(MAX(price), 2) as max_price
        FROM {db_schema}.products GROUP BY category ORDER BY category
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:15s} {row[1]:3d} products  (${row[2]} – ${row[3]})")

print("\n" + "=" * 60)
print(f"  ✅ Project '{project_name}' is READY!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Appendix: Querying Lakebase from the Lakehouse
# MAGIC
# MAGIC Beyond connecting programmatically (as we did above), there are two ways to query a Lakebase database from the **Lakehouse SQL Editor**:
# MAGIC
# MAGIC ### Option A: Connect Directly to Lakebase Compute
# MAGIC
# MAGIC 1. Open the **SQL Editor** in the Lakehouse sidebar
# MAGIC 2. From the **Connect** drop-down, select **More...**
# MAGIC 3. Select **Lakebase Postgres** → **Autoscaling** → your **Project** and **Branch**
# MAGIC 4. Click **Attach** and run queries directly
# MAGIC
# MAGIC <img src="./Includes/images/core_concepts/select_more.png"
# MAGIC      alt="SQL Editor Connect"
# MAGIC      width="500">
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px;">
# MAGIC     Limitations with direct connection
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <ul>
# MAGIC       <li><strong>No federated queries</strong> — you can only query the connected Lakebase project and branch, not combine with other Unity Catalog tables.</li>
# MAGIC       <li><strong>No Postgres meta-commands</strong> — commands like <code>\dt</code>, <code>\d</code>, <code>\l</code> are only supported in the Lakebase SQL Editor.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC ### Option B: Register Database in Unity Catalog
# MAGIC
# MAGIC This registers your Lakebase database as a Unity Catalog catalog, enabling federated queries and unified governance.
# MAGIC
# MAGIC 1. In **Catalog Explorer**, click the **+** icon → **Create a catalog**
# MAGIC 2. Enter a catalog name (e.g., `yourname-lakebase-catalog`)
# MAGIC 3. Select **Lakebase Postgres** → **Autoscaling** → your project, branch, and database
# MAGIC 4. Click **Create**
# MAGIC
# MAGIC After registration, query your Lakebase data using SQL warehouses or any Unity Catalog-connected tool.
# MAGIC
# MAGIC > **You'll do this hands-on in Lab 4.1** (Register Lakebase in Unity Catalog) — including a federated join scenario that's the entire reason data-centric users care about this feature.
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px;">
# MAGIC     Limitations with Unity Catalog registration
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <ul>
# MAGIC       <li><strong>Read-only access</strong> — catalogs registered from Lakebase are read-only through Unity Catalog.</li>
# MAGIC       <li><strong>Single database per catalog</strong> — each UC catalog represents one Lakebase database.</li>
# MAGIC       <li><strong>Metadata sync</strong> — UC caches metadata. New objects may not appear immediately; trigger a full refresh to see them.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cleanup Instructions
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px;">
# MAGIC     Warning - PLEASE READ IF RUNNING IN YOUR OWN WORKSPACE
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC     If you have finished working with your database or cannot continue to the remaining labs, please clean up your resources:
# MAGIC     <ul>
# MAGIC       <li>Each workspace supports a maximum of <strong>1000 database projects</strong></li>
# MAGIC       <li>Navigate to <strong>Settings → Delete Project</strong> and confirm</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC In this lab, you completed the full lifecycle of setting up a **Databricks Lakebase Autoscaling** project:
# MAGIC
# MAGIC **Key accomplishments:**
# MAGIC - **Learned** what Lakebase Autoscaling is, its key features (autoscaling, branching, scale-to-zero, point-in-time restore), and how it differs from Lakebase Provisioned
# MAGIC - **Discovered** your Lakebase PostgreSQL 17 project
# MAGIC - **Connected** using OAuth token-based authentication — no passwords needed
# MAGIC - **Created and populated** 5 PostgreSQL tables with realistic e-commerce data, using native PL/pgSQL features (SERIAL keys, CHECK constraints, foreign keys, cascading deletes)
# MAGIC - **Explored system metadata** through `pg_catalog`, `information_schema`, and `pg_stat_statements` — confirming Lakebase is standard PostgreSQL
# MAGIC - **Understood** how to query Lakebase from the Lakehouse via direct connection and Unity Catalog registration
# MAGIC
# MAGIC Your project is now ready for the remaining workshop labs on **reverse ETL**, **UC registration**,
# MAGIC **Lakehouse Sync**, **branching**, **schema migration**, **branch reset**, and **PITR**.

# COMMAND ----------

conn.close()
