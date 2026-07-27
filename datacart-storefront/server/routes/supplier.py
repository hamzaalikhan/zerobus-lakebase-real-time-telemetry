"""Supplier/Dealer View — aggregate product demand across all shoppers.

The storefront pushes every shopper's product views, clicks, and add-to-cart
actions into the lakehouse via Zerobus (see server/zerobus_producer.py). A
Lakeflow pipeline (Lab 3.1) aggregates that clickstream — joined with seeded
orders and inventory — into a gold `product_demand` table, which is synced back
into Lakebase. This view is a plain read of that synced table.

The app does NO aggregation. It only emits raw events; all the demand math lives
in the governed lakehouse pipeline (Photon, lineage, Delta joins), and only the
small aggregated result is served back here. That's the "Lakebase + lakehouse
together" story — behavioural data doesn't compete with transactional storefront
queries in the OLTP tier.

Surfaced as a plain table at GET /supplier (standalone HTML, no React dependency).
JSON is also at GET /api/supplier/demand.

Auth note: the "supplier" persona is faked for the demo. In production this would
be governed via Unity Catalog + Postgres roles — the supplier would see only
aggregated demand, never individual shopper data.
"""

import logging

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from server.db import pool, DB_SCHEMA
from server.schema_detector import get_demand_table

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/supplier/demand")
def supplier_demand():
    """Per-product demand aggregates, read from the synced `product_demand` table.

    Never raises: if the synced table isn't present yet (Lab 3.1 not run) or
    anything upstream fails, this returns an empty, well-formed payload rather
    than a 500. The Supplier View is additive — it must never break the app.
    """
    rows: list[dict] = []
    demand_table = None
    try:
        demand_table = get_demand_table()
        if demand_table:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT product_id, product_name, category,
                               views, clicks, add_to_carts, cart_rate,
                               units_sold, revenue, conversion_rate,
                               in_stock, reorder_level, restock_needed
                        FROM {DB_SCHEMA}.{demand_table}
                        ORDER BY views DESC NULLS LAST, add_to_carts DESC NULLS LAST
                        """
                    )
                    cols = [d.name for d in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        logger.warning(f"Supplier demand unavailable ({e}); returning empty view.")
        rows = []

    return {
        "demand": rows,
        "source": demand_table or "not synced yet",
        "total_views": sum((r.get("views") or 0) for r in rows),
        "total_add_to_cart": sum((r.get("add_to_carts") or 0) for r in rows),
        "restock_count": sum(1 for r in rows if r.get("restock_needed")),
    }


def _num(v) -> str:
    """Render a numeric cell, treating None as 0/em-dash-safe."""
    return str(v if v is not None else 0)


def _render_page(data: dict) -> str:
    rows = data["demand"]

    if rows:
        body_rows = "\n".join(
            f"""        <tr class="{ 'low' if r.get('restock_needed') else '' }">
          <td class="rank">{i}</td>
          <td class="name">{r.get('product_name') or ('Product #' + str(r.get('product_id')))}</td>
          <td class="cat">{r.get('category') or '&mdash;'}</td>
          <td class="num">{_num(r.get('views'))}</td>
          <td class="num">{_num(r.get('clicks'))}</td>
          <td class="num">{_num(r.get('add_to_carts'))}</td>
          <td class="num rate">{_num(r.get('cart_rate'))}%</td>
          <td class="num">{_num(r.get('units_sold'))}</td>
          <td class="num">{_num(r.get('in_stock'))}{ ' &#9888;' if r.get('restock_needed') else '' }</td>
        </tr>"""
            for i, r in enumerate(rows, start=1)
        )
        table = f"""      <table>
        <thead>
          <tr>
            <th>#</th><th>Product</th><th>Category</th>
            <th class="num">Views</th>
            <th class="num">Clicks</th>
            <th class="num">Add&nbsp;to&nbsp;Cart</th>
            <th class="num">Cart&nbsp;Rate</th>
            <th class="num">Units&nbsp;Sold</th>
            <th class="num">In&nbsp;Stock</th>
          </tr>
        </thead>
        <tbody>
{body_rows}
        </tbody>
      </table>"""
    else:
        table = """      <div class="empty">
        <p>No demand synced yet.</p>
        <p class="hint">Run <strong>Lab 3.1</strong> to stream the storefront
        clickstream through Zerobus, aggregate it in the lakehouse, and sync the
        <code>product_demand</code> table back to Lakebase. Then refresh this page.</p>
      </div>"""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>DataCart — Supplier Demand View</title>
  <style>
    :root {{ color-scheme: light; }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      margin: 0; background: #f6f7f9; color: #1a1a2e;
    }}
    header {{
      background: #0b1e3f; color: #fff; padding: 24px 32px;
    }}
    header h1 {{ margin: 0 0 4px; font-size: 22px; }}
    header p {{ margin: 0; color: #b9c4d6; font-size: 14px; }}
    main {{ max-width: 1040px; margin: 0 auto; padding: 32px; }}
    .summary {{
      display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap;
    }}
    .stat {{
      background: #fff; border: 1px solid #e3e6ea; border-radius: 10px;
      padding: 16px 20px; flex: 1; min-width: 160px;
    }}
    .stat .label {{ font-size: 12px; text-transform: uppercase; color: #6b7686;
      letter-spacing: .04em; }}
    .stat .value {{ font-size: 28px; font-weight: 700; margin-top: 4px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff;
      border: 1px solid #e3e6ea; border-radius: 10px; overflow: hidden; }}
    th, td {{ padding: 12px 16px; text-align: left; font-size: 14px; }}
    thead th {{ background: #f0f2f5; color: #475061;
      font-size: 12px; text-transform: uppercase; letter-spacing: .03em; }}
    tbody tr {{ border-top: 1px solid #eef0f3; }}
    tbody tr:nth-child(even) {{ background: #fafbfc; }}
    tbody tr.low {{ background: #fff5f5; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .rank {{ color: #9aa3b2; width: 32px; }}
    .name {{ font-weight: 600; }}
    .cat {{ color: #6b7686; }}
    .rate {{ font-weight: 600; color: #0b7a4b; }}
    .empty {{ background: #fff; border: 1px dashed #cbd2dc; border-radius: 10px;
      padding: 48px; text-align: center; color: #6b7686; }}
    .empty .hint {{ font-size: 13px; }}
    footer {{ max-width: 1040px; margin: 0 auto; padding: 0 32px 40px;
      color: #8b93a1; font-size: 12px; }}
    .badge {{ display: inline-block; background: #eef2ff; color: #3a4a8c;
      border-radius: 6px; padding: 2px 8px; font-size: 11px; margin-left: 8px; }}
  </style>
</head>
<body>
  <header>
    <h1>Supplier Demand View
      <span class="badge">source: {data['source']}</span>
    </h1>
    <p>Aggregate shopper demand across the DataCart storefront &mdash; views,
       clicks, and add-to-cart intent per product, joined with sales and stock.</p>
  </header>
  <main>
    <div class="summary">
      <div class="stat">
        <div class="label">Total Product Views</div>
        <div class="value">{data['total_views']}</div>
      </div>
      <div class="stat">
        <div class="label">Total Add-to-Cart</div>
        <div class="value">{data['total_add_to_cart']}</div>
      </div>
      <div class="stat">
        <div class="label">Products with Demand</div>
        <div class="value">{len(rows)}</div>
      </div>
      <div class="stat">
        <div class="label">Restock Needed</div>
        <div class="value">{data['restock_count']}</div>
      </div>
    </div>
{table}
  </main>
  <footer>
    Demand is aggregated by a Lakeflow pipeline in the lakehouse (clickstream via
    Zerobus, joined with orders and inventory) and synced back to Lakebase. The app
    performs no aggregation &mdash; it only emits raw events. In production this view
    would be governed via Unity Catalog + Postgres roles, and the supplier would see
    only aggregated demand &mdash; never individual shopper data.
  </footer>
</body>
</html>"""


@router.get("/supplier", response_class=HTMLResponse)
def supplier_page():
    """Standalone HTML supplier dashboard (no shopper frontend dependency).

    Always renders a page — even with zero demand or an upstream failure, the
    shopper sees the friendly "No demand synced yet" empty state, never a 500.
    """
    try:
        data = supplier_demand()
    except Exception as e:
        logger.warning(f"Supplier page fell back to empty view ({e}).")
        data = {"demand": [], "source": "unavailable",
                "total_views": 0, "total_add_to_cart": 0, "restock_count": 0}
    return HTMLResponse(content=_render_page(data))
