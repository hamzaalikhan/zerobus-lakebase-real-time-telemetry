# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakebase Workshop
# MAGIC
# MAGIC For decades, **databases** have been the backbone of software, yet while we've completely reinvented how applications are built, the underlying databases have changed very little since the 1980s — suffering from fragile and costly operations, clunky development experiences, and extreme vendor lock-in.
# MAGIC
# MAGIC **Lakebase** represents a new approach: an open database architecture that brings together the reliability of transactional systems with the scalability and cost efficiency of the data lake. At its core is a rethinking of how databases are built — decoupling compute from storage so that data lives in affordable cloud object storage using open formats, while the database engine operates independently on top. This design removes much of the overhead, rigidity, and lock-in that traditional databases have carried for decades.
# MAGIC
# MAGIC **Databricks Lakebase** delivers this architecture as a fully managed, serverless Postgres database. Compute resources scale up automatically to meet demand and scale back to zero when not in use, so you only pay for what you consume. This makes it well suited for variable workloads, developer sandboxes, and AI agents that need to spin up isolated environments on the fly.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC                         ┌─────────────────────────────────────────────────────────────┐
# MAGIC                         │                        COMPUTE LAYER                        │
# MAGIC                         │  ┌─────────────┐   ┌─────────────┐   ┌─────────────────┐    │
# MAGIC                         │  │ Endpoint A  │   │ Endpoint B  │   │   Endpoint C    │    │
# MAGIC                         │  │ (read-write)│   │ (read-only) │   │  (read-write)   │    │
# MAGIC                         │  └──────┬──────┘   └──────┬──────┘   └────────┬────────┘    │
# MAGIC                         │         │                 │                   │             │
# MAGIC                         ├─────────┼─────────────────┼───────────────────┼─────────────┤
# MAGIC                         │         │         STORAGE LAYER               │             │
# MAGIC                         │  ┌──────▼─────────────────▼───────────────────▼────────┐    │
# MAGIC                         │  │            Shared Object Storage (S3 / ADLS)        │    │
# MAGIC                         │  │   Branch: production  │  Branch: dev-feature  │ ... │    │
# MAGIC                         │  └─────────────────────────────────────────────────────┘    │
# MAGIC                         └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### What this means in practice
# MAGIC
# MAGIC | Concept | Explanation |
# MAGIC |---------|-------------|
# MAGIC | **Compute (Endpoints)** | Each branch has one or more *endpoints* — PostgreSQL-compatible connection targets that process queries. Endpoints can scale up, scale down, or reach zero when idle, without touching your data. |
# MAGIC | **Storage (Branches)** | All branch data lives in object storage. Because storage is separate from compute and Lakebase uses copy-on-write technology, creating a branch is **zero-copy** — no data is physically duplicated. A branch only consumes extra storage as changes diverge from its source. |
# MAGIC | **Scale-to-Zero** | When there is no traffic or activity, the compute layer scales to zero, eliminating cost. The data remains safely in storage and the endpoint resumes instantly on the next connection. |
# MAGIC | **Independent Scaling** | You can attach multiple endpoints to a single branch (e.g. one read-write, one read-only for additional reaad operations). |

# COMMAND ----------

# MAGIC %md
# MAGIC # DataCart: Modernizing E-Commerce Database Operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Challenge
# MAGIC
# MAGIC DataCart's data team is unifying its operational database (Lakebase Postgres) with the lakehouse so that analytics, applications, and OLTP all sit on one governed platform. Their first goal is to get **data flowing in every direction** between Lakebase and Unity Catalog. Once those flows are in place, the engineering team can confidently evolve the OLTP schema for the Spring Sale launch — new product reviews, a loyalty program, and disaster-recovery readiness — without breaking the analytics consumers downstream.
# MAGIC
# MAGIC #### The Reverse ETL Scenario
# MAGIC
# MAGIC The marketing team has prepared Spring Sale promotions — product discounts, sale badges, and limited-time offers — in a Delta table in the data lakehouse. Using **Lakebase Synced Tables**, these promotions are pushed to the production database and instantly appear on the storefront with sale badges and discounted prices — without any application code changes.
# MAGIC
# MAGIC #### The Unity Catalog Federation Scenario
# MAGIC
# MAGIC The analytics team wants to join live order data from Lakebase with marketing-campaign Delta tables for real-time attribution dashboards — without standing up another ETL pipeline. By **registering Lakebase as a Unity Catalog foreign catalog**, any SQL warehouse can query live OLTP data with full UC governance, and join it against Delta in a single statement.
# MAGIC
# MAGIC #### The Lakehouse Sync Scenario
# MAGIC
# MAGIC The BI and ML teams need to run heavy analytical workloads against OLTP data without putting load on the storefront database. **Lakehouse Sync** continuously mirrors Lakebase tables into Delta inside Unity Catalog, so analytics consumers hit columnar lakehouse storage with photon performance while the storefront keeps serving customers.
# MAGIC
# MAGIC #### Parallel Development
# MAGIC
# MAGIC With the data flows wired up, three developers can now safely evolve the OLTP schema in parallel without blocking each other or risking production stability:
# MAGIC
# MAGIC | Developer | Team | Task |
# MAGIC |---|---|---|
# MAGIC | Developer A | Loyalty Team | Add a new `loyalty_members` table, `loyalty_points` column, and seed product reviews |
# MAGIC | Developer B | Global Team | Modify the `orders` table to change the `currency` column from a fixed string to a foreign key linked to a new `exchange_rates` table |
# MAGIC | Developer C | Performance Team | Create new indexes on the `products` table to handle the high-traffic surge expected during the sale |
# MAGIC
# MAGIC #### The "Code Red" Disaster Scenario
# MAGIC
# MAGIC During the final Spring Sale deployment, a DevOps engineer accidentally executes `DROP TABLE orders CASCADE;` instead of dropping a temporary staging table. The production storefront immediately begins throwing errors — customers cannot view their orders or complete purchases, and every second of downtime means thousands of dollars in lost revenue.
# MAGIC
# MAGIC In a traditional database, the team would need to find the last nightly backup, provision a new instance, restore the data (which could take hours), and replay logs. With **Lakebase PITR**, the process to handle this is much smoother — and the downstream UC foreign catalog and Lakehouse Sync pipeline both heal automatically once production is restored.
# MAGIC
# MAGIC ### The DataCart Storefront
# MAGIC
# MAGIC Throughout this workshop, you'll interact with the **DataCart Storefront** — a live customer-facing e-commerce web application connected to your Lakebase project. As you run each lab, the storefront **evolves in real time**:
# MAGIC
# MAGIC | Lab | What Happens |
# MAGIC |-----|-------------|
# MAGIC | **1.1 Setup** | Basic storefront — products, stock, cart, orders (no ratings yet) |
# MAGIC | **2.1 Permissions** | Storefront comes online once the service principal has database access |
# MAGIC | **3.1 Reverse ETL** | Sale badges, discount prices, "Spring Sale Deals" section appear |
# MAGIC | **4.1 UC Registration** | Lakebase becomes queryable from any SQL warehouse via a UC foreign catalog |
# MAGIC | **5.1 Lakehouse Sync** | Lakebase tables continuously mirror to Delta in Unity Catalog |
# MAGIC | **6.1 Parallel Dev** | No storefront change — branches are isolated from production |
# MAGIC | **6.2 Schema to Prod** | Star ratings, loyalty badges, "Earn pts" labels appear |
# MAGIC | **6.3 Branch Reset** | Priority badges on orders, verified badge in navbar |
# MAGIC | **7.1 PITR Disaster** | Orders page breaks → gracefully degrades → recovers after PITR |
# MAGIC
# MAGIC > The storefront auto-detects schema changes every 30 seconds. No redeployment needed.
# MAGIC
# MAGIC ### This Workshop
# MAGIC
# MAGIC This workshop places you in the role of a database engineer at **DataCart**, a rapidly growing global e-commerce platform preparing for a major "Spring Sale" launch. You'll experience firsthand how Lakebase reverse ETL, UC registration, Lakehouse Sync, branching, and PITR address real-world development and operational challenges.
# MAGIC
# MAGIC ### Key Learning Objectives
# MAGIC
# MAGIC | Topic | Description |
# MAGIC |---|---|
# MAGIC | **Reverse ETL (UC → Lakebase)** | Serving lakehouse analytics data to applications via synced tables |
# MAGIC | **UC Registration (Lakehouse Federation)** | Querying live Lakebase data from any UC SQL warehouse, with governance |
# MAGIC | **Lakehouse Sync (Lakebase → UC)** | Continuously mirroring OLTP tables to Delta for analytical workloads |
# MAGIC | **Branching** | Creating isolated environments for parallel schema evolution across multiple developer teams |
# MAGIC | **Point-in-Time Recovery** | Recovering from catastrophic human error without downtime using PITR |
# MAGIC | **Roles & Permissions** | Managing access control across branches to enforce governance |

