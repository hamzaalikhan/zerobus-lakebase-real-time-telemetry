-- Lakeflow Declarative Pipeline — Clickstream Medallion (Lab 3.1)
-- =============================================================================
-- This is the "aggregate" step of the workshop loop. It reads the raw clickstream
-- that the storefront pushes into the bronze table via Zerobus, cleans and enriches
-- it, and rolls it up into a per-product demand table the Supplier View consumes.
--
-- Authored entirely in SQL (no PySpark) — the audience is lakehouse-native and may
-- not know Spark. Bronze → Silver → Gold:
--
--   clickstream_bronze  (plain Delta, Zerobus target, created in Lab 1.1)
--         │  STREAM()  — incremental, append-only
--         ▼
--   clickstream_silver  (STREAMING TABLE) — dedup at-least-once, cast, enrich w/ product name+category
--         │
--         ▼
--   product_demand      (MATERIALIZED VIEW) — views/clicks/carts per product, cart_rate,
--                         + seeded order_items (units_sold, conversion) + inventory (stock, demand-vs-stock)
--
-- Pipeline configuration expected (set in the pipeline settings / Lab 3.1 notebook):
--   source_catalog : the UC catalog holding the ecommerce schema (e.g. your workshop catalog)
--   source_schema  : "ecommerce"
-- The pipeline's own target (where silver/gold are created) should be the SAME
-- catalog + schema, so the storefront's downstream sync finds product_demand there.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SILVER — clean, dedup, enrich
-- -----------------------------------------------------------------------------
-- Zerobus delivers at-least-once, so the bronze stream can contain duplicate rows.
-- We deduplicate on the full natural key (event_type, product_id, event_ts). The
-- @dlt expectations drop rows that are structurally unusable (null product / type /
-- timestamp, or an unknown event type) rather than let them poison the rollups.
CREATE OR REFRESH STREAMING TABLE clickstream_silver
  (
    CONSTRAINT valid_event_type   EXPECT (event_type IN ('view', 'click', 'add_to_cart')) ON VIOLATION DROP ROW,
    CONSTRAINT valid_product_id   EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_event_ts     EXPECT (event_ts IS NOT NULL) ON VIOLATION DROP ROW
  )
  COMMENT 'Cleaned, deduplicated clickstream enriched with product name + category.'
AS
WITH deduped AS (
  SELECT DISTINCT
    event_type,
    CAST(product_id AS INT)          AS product_id,
    CAST(event_ts   AS TIMESTAMP)    AS event_ts
  FROM STREAM(${source_catalog}.${source_schema}.clickstream_bronze)
)
SELECT
  d.event_type,
  d.product_id,
  d.event_ts,
  p.name      AS product_name,
  p.category  AS category
FROM deduped d
LEFT JOIN ${source_catalog}.${source_schema}.products p
  ON p.id = d.product_id;

-- -----------------------------------------------------------------------------
-- GOLD — per-product demand signal (the lab's deliverable)
-- -----------------------------------------------------------------------------
-- Pivots the event stream into per-product counts with COUNT(*) FILTER, derives a
-- cart-conversion rate, and joins the seeded transactional context (order_items for
-- realized sales, inventory for stock) so a supplier sees demand AND whether stock
-- can cover it — the "demand vs. stock / revenue-at-risk" story.
CREATE OR REFRESH MATERIALIZED VIEW product_demand
  COMMENT 'Per-product demand: behavioural clickstream signal joined with seeded orders + inventory. Synced back to Lakebase for the Supplier View.'
AS
WITH clicks AS (
  SELECT
    product_id,
    ANY_VALUE(product_name)                                    AS product_name,
    ANY_VALUE(category)                                        AS category,
    COUNT(*) FILTER (WHERE event_type = 'view')                AS views,
    COUNT(*) FILTER (WHERE event_type = 'click')               AS clicks,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart')         AS add_to_carts
  -- Reference the pipeline's own silver table by name (the legacy LIVE.* prefix
  -- is deprecated for new Lakeflow pipelines).
  FROM clickstream_silver
  GROUP BY product_id
),
sold AS (
  SELECT
    product_id,
    SUM(quantity)   AS units_sold,
    SUM(line_total) AS revenue
  FROM ${source_catalog}.${source_schema}.order_items
  GROUP BY product_id
),
stock AS (
  SELECT
    product_id,
    SUM(quantity)      AS in_stock,
    MIN(reorder_level) AS reorder_level
  FROM ${source_catalog}.${source_schema}.inventory
  GROUP BY product_id
)
SELECT
  c.product_id,
  c.product_name,
  c.category,
  c.views,
  c.clicks,
  c.add_to_carts,
  -- Cart rate = add-to-carts per view, as a percentage (guard divide-by-zero).
  ROUND(100.0 * c.add_to_carts / NULLIF(c.views, 0), 1)        AS cart_rate,
  COALESCE(s.units_sold, 0)                                    AS units_sold,
  COALESCE(s.revenue, 0)                                       AS revenue,
  -- Conversion = realized sales per add-to-cart (behaviour → transaction).
  ROUND(100.0 * COALESCE(s.units_sold, 0) / NULLIF(c.add_to_carts, 0), 1) AS conversion_rate,
  COALESCE(k.in_stock, 0)                                      AS in_stock,
  k.reorder_level,
  -- Demand-vs-stock flag: high intent but low stock = restock signal for the supplier.
  CASE
    WHEN COALESCE(k.in_stock, 0) <= COALESCE(k.reorder_level, 0) AND c.add_to_carts > 0
      THEN true ELSE false
  END                                                          AS restock_needed
FROM clicks c
LEFT JOIN sold  s ON s.product_id = c.product_id
LEFT JOIN stock k ON k.product_id = c.product_id;
