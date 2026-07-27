# Zerobus + Lakebase App Workshop — Plan & Status

Working notes for adapting the Lakebase workshop into a Zerobus-fronted, app-centric
workshop. Living document — update as the build progresses.

## Why this exists (context)

- **Goal:** show the full **collect → aggregate → present** loop on a single governed
  platform, driven by a live app — the pattern behind "ingest operational/behavioural data,
  aggregate it in the lakehouse, and serve it back to a customer-facing app."
- **Audience:** software/product engineers who are already comfortable with the Databricks
  lakehouse (Unity Catalog, DLT, Medallion). Assumes a foundational Databricks session has
  been covered separately, so **no foundational lakehouse selling needed** here.
- **Positioning for a lakehouse-native shop with no existing Postgres:** Lakebase is a *new
  capability* — a serverless Postgres serving tier fed by the lakehouse — **not a swap** for
  an existing OLTP database. Frame it as additive; the scale-to-zero cost story is a hook.
- **Session:** ~half day (3–4h).

## The narrative we settled on (the "loop")

Frame everything as ONE loop the app itself drives, not a menu of features:

```
App (shopper + supplier/dealer)  ──emit clickstream──▶  Zerobus  ──▶  Delta (UC)
        ▲                                                              │  DLT/SQL rollup
        │                                                              ▼
   reads from Lakebase  ◀──synced table──  gold: product_demand (aggregated)
```

- **Serve** — app reads catalogue/orders from Lakebase (OLTP serving tier).
- **Collect** — app emits clickstream events, ingested straight to the lakehouse (Zerobus,
  no message bus).
- **Aggregate** — light transforms → demand signals (most-viewed / trending products).
- **Present back** — a synced table pushes the aggregated view back into Lakebase; it surfaces
  live in the app, zero app code change.

**Resolved design tension:** presenting a shopper's own clicks back to that same shopper is
circular and doesn't flow. The believable consumer of the aggregated data is a
**supplier/dealer** — someone who wants to see demand across all shoppers. So the aggregated
`product_demand` view surfaces in a **`/dealer` (supplier/dealer) analytics view**, not (only)
as shopper "trending". This is **one app, one Lakebase, two personas reading different
tables** — the storefront and the supplier dashboard sit over one data foundation. (Dealer
auth is faked for the demo; note "governed via UC + Postgres roles in prod".)

## Key product facts / constraints (verified via research)

- **Zerobus:** serverless push API (gRPC/REST) → writes straight into **UC-managed Delta
  tables**, no Kafka. GA Feb 2026, **AWS + Azure**. SDKs incl. **Java** and **Python**.
  - **Region-gated:** workspace + target table must be in the **same** region — VERIFY on the
    actual workshop workspace.
  - **Target Delta table must be pre-created** (Zerobus does NOT auto-create tables).
  - **At-least-once** delivery (mention dedup/idempotency if shown downstream).
  - **No direct Zerobus→Lakebase path.** Always Zerobus→Delta→(synced table)→Lakebase.
    Design it as a *prequel bolted onto the front* of the Lakebase labs, not a replacement.
  - Verify SDK API names against `github.com/databricks/zerobus-sdk` before writing a live
    producer — do not trust reconstructed signatures.
- **Base workshop:** the storefront (`datacart-storefront/`) is React + FastAPI + psycopg3 +
  OAuth. It auto-detects schema changes every 30s via `server/schema_detector.py` and lights
  up new features with no redeploy — reuse this to surface the supplier/dealer view.

## Repo / git state

- **Fork:** `hamzaalikhan/zerobus-lakebase-real-time-telemetry` (a fork of the EMPTY
  `bnwokele/zerobus-lakebase-real-time-telemetry`, so a PR from here merges cleanly back to
  bnwokele's zerobus repo).
- **Local clone:** `.../workshops/lakebase/zerobus-lakebase-app`, pushed via the
  `hamza_personal` SSH key (remote uses the `github.com-hamzaalikhan` alias).
- **Branch:** `feature/zerobus-lakebase-app` (pushed, tracking origin).
- **Auth note:** `gh` CLI is logged in only as `hamza-khan_data`, NOT `hamzaalikhan`. Repo
  creation/forking must be done in the web UI as `hamzaalikhan`; git transport uses the SSH
  alias. `hamzaalikhan` has NO write access to bnwokele's repos (hence the fork route).
- **Done so far:** commit `2a69bc1` seeds the full **data-centric** workshop content into the
  fork (all labs, storefront, Includes, docs) + `.gitignore` (excludes `.claude/`, `.DS_Store`).

## LOCKED PLAN — build the net-new Zerobus/Lakeflow lab (decided 2026-07-26)

Scope is the FULL loop: app emits clickstream → Zerobus → bronze → DLT(SQL) silver/gold →
reverse-ETL synced back to Lakebase → supplier view reads it. All decisions below are locked
with the user; do NOT re-litigate them — just build.

### Lab numbering (user already renamed on disk — verified via git)
- `1.1` Setup & Connect — EDIT: also create the bronze clickstream UC Delta table here +
  grant the app SP Zerobus-ingest permission on it.
- `2.1` Reverse ETL with Synced Tables — renamed from old 3.1 (git: D old 3.1, ?? new 2.1). Teaches synced-table mechanism FIRST.
- `3.1` **NEW** — Clickstream → Zerobus → Medallion → Sync back (THE build target).
- `4.1` Lakehouse Sync, `5.1` Connect Apps — unchanged.

### The events (all same shape → ONE bronze table, ONE Zerobus stream)
- Emit `view` + `click` + `add_to_cart` from the app. Shape: `(event_type, product_id, event_ts)`.
  event_type column discriminates. NOT purchases (conversion comes from seeded orders join).
- Lab note: "when you'd split into multiple tables" = only when event SHAPES differ (e.g. search
  has a query string, purchase has an amount). For these 3, one table.

### No aggregation in the app — ALL transforms in the pipeline, DLT authored in SQL
- Bronze: plain UC Delta table (Zerobus target, created in 1.1, NOT a DLT table — DLT reads it as streaming source).
- Silver: `CREATE OR REFRESH STREAMING TABLE` — dedup Zerobus at-least-once (DISTINCT), JOIN seeded `products` for name/category.
- Gold `product_demand` (MATERIALIZED VIEW / STREAMING TABLE): per-product `views/clicks/add_to_carts`
  via `COUNT(*) FILTER (WHERE event_type=...)`, `cart_rate`, + LEFT JOIN seeded `order_items` (units_sold/conversion)
  + LEFT JOIN seeded `inventory` (in_stock / demand-vs-stock). Pure SQL, no PySpark (audience may not know Spark).
- `product_demand` IS the gold deliverable — the end output of the lab.

### Present back (reverse ETL, reuse the 2.1 synced-table mechanism)
- Sync gold `product_demand` → Lakebase synced table. Supplier view (`server/routes/supplier.py`)
  reads it as a simple table dump (nothing fancy). REMOVE the app's old Postgres `get_demand()` aggregation +
  the `product_events` Postgres table from `server/events.py` — app now only EMITS.

### Files
- NEW `3.1 Lab - Clickstream Ingestion with Zerobus ...py`: explain Zerobus (pre-created table,
  at-least-once, JSON ingest), ~500-session SEED GENERATOR as an in-notebook cell (visible to attendees,
  `random.seed(42)`, category-weighted, funnel drop-off), DLT-SQL bronze→silver→gold, verify gold, sync back.
- NEW `datacart-storefront/server/zerobus_producer.py`: best-effort producer, verified SDK
  (`from zerobus.sdk.sync import ZerobusSdk`, JSON `ingest_record_offset`) — see [[zerobus-python-sdk-api]].
  Reuse app SP client_id/client_secret (SP needs ingest grant from 1.1). Mirror events.py never-break-shopper discipline.
- EDIT `1.1`: bronze table + SP grant. EDIT `server/events.py`: thin emit-to-Zerobus, drop aggregation.
  EDIT `server/routes/supplier.py`: read synced product_demand. EDIT `0 Workshop Introduction.py`: add 3.1 to loop table.

### Framing to teach (audience is lakehouse-native, will ask "why not just GROUP BY in Postgres?")
Answer in the lab: aggregating in the OLTP tier competes with transactional storefront queries;
push behavioral data to lakehouse, aggregate there (Photon, governance, lineage, Delta joins),
sync only the small result back. That's the "Lakebase + lakehouse together" story.

### Build order (next session, fresh — bypassPermissions now set so runs uninterrupted)
1. VERIFY on `fevm-hamza-ai-lab` (profile `hamza_fevm_ai_workshop`): Zerobus region-enabled + get
   `server_endpoint` URL. User said "do it." If not enabled → fallback: pre-populate bronze + narrate (repo risk note).
2. Bronze table + SP grant into 1.1.
3. Seed generator + `zerobus_producer.py`; wire view/click/add_to_cart emits in shop.py/cart.py.
4. DLT-SQL medallion (silver + gold).
5. Present-back synced table + point supplier view at it; strip app aggregation.
6. Write the 3.1 lab notebook + update intro.
7. **Spin up a VERIFIER AGENT at the end** — live bar: deploy, seed, run pipeline, confirm gold
   populates + supplier view updates on the real workspace (user approved live verification).
- No incremental commits (user: "no need to commit"). No PR yet.

## Risks / open questions

- **Setup is the #1 risk**, not content: DABs deploy + runtime project discovery assume
  workspace/CLI comfort. Pre-deploy for attendees or budget facilitated time. SDK fallback
  notebook exists.
- **Verify on the target workspace before the session:** Zerobus region-enabled; Lakehouse
  Sync feature-flag/region-enabled (Lab 5.1 dies live otherwise); serverless SQL warehouse
  with federation (Lab 4.1).
- **Live streaming demo is fragile.** Fallback: pre-populate the Delta table and narrate
  ingestion rather than run it live. For the actual half-day, consider Zerobus as a
  5-min narrative + architecture diagram (finale), and build the full module as a follow-up
  deliverable.
