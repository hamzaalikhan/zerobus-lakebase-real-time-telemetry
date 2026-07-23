"""Lightweight in-app clickstream capture for the Supplier View.

Every product view and add-to-cart the shopper performs is recorded here, so a
supplier/dealer can see aggregate demand across all shoppers. Events are written
to an `ecommerce.product_events` table in Lakebase (the same OLTP tier the
storefront already reads from), which makes this the "collect" step of the
workshop loop done entirely inside the app.

Design notes:
  * The table is created on first use. If the app's service principal lacks
    CREATE on the schema, we fall back to in-memory counters so the storefront
    never breaks — the Supplier View simply shows this-process-only numbers.
  * Recording is best-effort and never raises into the request path: a shopper
    action must never fail because telemetry failed.
  * This is deliberately the piece a real deployment would replace with Zerobus
    (push events straight to a governed Delta table). Here it stays in-app.
"""

import logging
import threading

from server.db import pool, DB_SCHEMA

logger = logging.getLogger(__name__)

EVENTS_TABLE = "product_events"
VIEW = "view"
ADD_TO_CART = "add_to_cart"

# In-memory fallback: {product_id: {event_type: count}}. Only used if the
# Lakebase table can't be created/written (e.g. missing CREATE privilege).
_mem_counts: dict[int, dict[str, int]] = {}
_mem_lock = threading.Lock()

# Tri-state: None = not yet checked, True = table usable, False = using fallback.
_table_ready: bool | None = None
_init_lock = threading.Lock()


def _ensure_table() -> bool:
    """Create the events table if needed. Returns True if the table is usable."""
    global _table_ready
    if _table_ready is not None:
        return _table_ready
    with _init_lock:
        if _table_ready is not None:
            return _table_ready
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{EVENTS_TABLE} (
                            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            event_type  TEXT NOT NULL,
                            product_id  INTEGER NOT NULL,
                            ts          TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {EVENTS_TABLE}_product_idx "
                        f"ON {DB_SCHEMA}.{EVENTS_TABLE} (product_id)"
                    )
                conn.commit()
            _table_ready = True
            logger.info(f"product_events table ready in schema {DB_SCHEMA}")
        except Exception as e:
            _table_ready = False
            logger.warning(
                f"Could not create/access {DB_SCHEMA}.{EVENTS_TABLE} "
                f"({e}); falling back to in-memory event counts."
            )
        return _table_ready


def record_event(event_type: str, product_id: int) -> None:
    """Record a single clickstream event. Best-effort; never raises."""
    try:
        if _ensure_table():
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO {DB_SCHEMA}.{EVENTS_TABLE} (event_type, product_id) "
                        f"VALUES (%s, %s)",
                        (event_type, product_id),
                    )
                conn.commit()
            return
    except Exception as e:
        logger.warning(f"Event insert failed ({e}); using in-memory fallback.")
    # Fallback path (table unavailable or insert failed).
    with _mem_lock:
        _mem_counts.setdefault(product_id, {}).setdefault(event_type, 0)
        _mem_counts[product_id][event_type] += 1


def get_demand() -> list[dict]:
    """Return per-product demand aggregates, most-viewed first.

    Each row: {product_id, product_name, category, views, add_to_cart, cart_rate}.
    Products with no events are omitted.
    """
    rows_by_product: dict[int, dict[str, int]] = {}

    if _ensure_table():
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT product_id,
                               COUNT(*) FILTER (WHERE event_type = %s) AS views,
                               COUNT(*) FILTER (WHERE event_type = %s) AS adds
                        FROM {DB_SCHEMA}.{EVENTS_TABLE}
                        GROUP BY product_id
                        """,
                        (VIEW, ADD_TO_CART),
                    )
                    for pid, views, adds in cur.fetchall():
                        rows_by_product[pid] = {"views": int(views), "adds": int(adds)}
        except Exception as e:
            logger.warning(f"Demand query failed ({e}); using in-memory counts.")

    if not rows_by_product:
        with _mem_lock:
            for pid, counts in _mem_counts.items():
                rows_by_product[pid] = {
                    "views": counts.get(VIEW, 0),
                    "adds": counts.get(ADD_TO_CART, 0),
                }

    if not rows_by_product:
        return []

    # Join product names/categories from the catalog for a readable table.
    names: dict[int, tuple[str, str]] = {}
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                ids = list(rows_by_product.keys())
                placeholders = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"SELECT id, name, category FROM {DB_SCHEMA}.products "
                    f"WHERE id IN ({placeholders})",
                    ids,
                )
                for pid, name, category in cur.fetchall():
                    names[pid] = (name, category)
    except Exception as e:
        logger.warning(f"Could not enrich demand with product names ({e}).")

    result = []
    for pid, counts in rows_by_product.items():
        name, category = names.get(pid, (f"Product #{pid}", ""))
        views = counts["views"]
        adds = counts["adds"]
        cart_rate = round(100 * adds / views, 1) if views else 0.0
        result.append({
            "product_id": pid,
            "product_name": name,
            "category": category,
            "views": views,
            "add_to_cart": adds,
            "cart_rate": cart_rate,
        })

    result.sort(key=lambda r: (r["views"], r["add_to_cart"]), reverse=True)
    return result


def storage_mode() -> str:
    """'lakebase' if events persist to the table, else 'in-memory (fallback)'."""
    if _table_ready is True:
        return "lakebase"
    if _table_ready is False:
        return "in-memory (fallback)"
    return "not yet initialized"
