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

## Plan / next steps

1. **Simplify pass (do first — clean base):** remove schema-migration/branching/PITR and
   heavy bonus labs — candidates: `6.1`, `6.2`, `6.3` (branching/migration), `7.1` (PITR),
   possibly `8` (monitoring). Keep the collect→aggregate→present spine:
   `1.1` seed → `2.1` connect app → `3.1` reverse ETL → `5.1` lakehouse sync → `9` connect
   apps. Trim `4.1` federation to optional.
2. **Build the Zerobus module (net-new):**
   - Pre-create a `clickstream` / `product_events` Delta table in UC.
   - Producer (Python or Java) using `databricks/zerobus-sdk` — queue-then-flush.
   - DLT/SQL rollup → gold `product_demand`.
   - Reuse the Lab 3.1 synced-table plumbing to push `product_demand` back into Lakebase.
3. **Add the supplier/dealer view to the app:** new `/dealer` React route +
   `server/routes/dealer.py` endpoint (mirror `shop.py`/`orders.py`), querying
   `product_demand` via the existing pool. `schema_detector` auto-lights it up.
4. **Rewrite intro + README** around the loop narrative; recast the hero as a **product
   engineer** (borrow the app-centric variant's framing). Keep the DataCart e-commerce theme
   throughout — shopper storefront + supplier/dealer demand view over one Lakebase.
5. Hand back to `bnwokele` via PR from this branch.

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
