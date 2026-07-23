"""Supplier/Dealer View — aggregate product demand across all shoppers.

The storefront captures every shopper's product views and add-to-cart actions
(see server/events.py). A supplier or dealer doesn't care about one shopper's
clicks — they want to see demand in aggregate: which products are getting
attention, and how often that attention converts to intent (add-to-cart).

This surfaces that as a plain table at GET /supplier, served as a standalone
HTML page (no dependency on the shopper React bundle). The JSON is also
available at GET /api/supplier/demand.

Auth note: in this workshop the "supplier" persona is faked for the demo. In
production this view would be governed via Unity Catalog + Postgres roles, and
the supplier would only see aggregated demand — never individual shopper data.
"""

import logging

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from server import events

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/supplier/demand")
def supplier_demand():
    """Per-product demand aggregates (views, add-to-cart, conversion).

    Never raises: if no analytics have been captured yet (or anything upstream
    fails), this returns an empty, well-formed payload rather than a 500. The
    Supplier View is additive to the workshop — it must not break the app when
    the collect/aggregate pieces aren't in place yet.
    """
    try:
        rows = events.get_demand()
    except Exception as e:
        logger.warning(f"Supplier demand unavailable ({e}); returning empty view.")
        rows = []
    try:
        mode = events.storage_mode()
    except Exception:
        mode = "unavailable"
    return {
        "demand": rows,
        "storage_mode": mode,
        "total_views": sum(r["views"] for r in rows),
        "total_add_to_cart": sum(r["add_to_cart"] for r in rows),
    }


def _render_page(data: dict) -> str:
    rows = data["demand"]

    if rows:
        body_rows = "\n".join(
            f"""        <tr>
          <td class="rank">{i}</td>
          <td class="name">{r['product_name']}</td>
          <td class="cat">{r['category'] or '&mdash;'}</td>
          <td class="num">{r['views']}</td>
          <td class="num">{r['add_to_cart']}</td>
          <td class="num rate">{r['cart_rate']}%</td>
        </tr>"""
            for i, r in enumerate(rows, start=1)
        )
        table = f"""      <table>
        <thead>
          <tr>
            <th>#</th><th>Product</th><th>Category</th>
            <th class="num">Views</th>
            <th class="num">Add&nbsp;to&nbsp;Cart</th>
            <th class="num">Cart&nbsp;Rate</th>
          </tr>
        </thead>
        <tbody>
{body_rows}
        </tbody>
      </table>"""
    else:
        table = """      <div class="empty">
        <p>No demand recorded yet.</p>
        <p class="hint">Browse products and add a few to the cart in the
        storefront, then refresh this page.</p>
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
    main {{ max-width: 900px; margin: 0 auto; padding: 32px; }}
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
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .rank {{ color: #9aa3b2; width: 32px; }}
    .name {{ font-weight: 600; }}
    .cat {{ color: #6b7686; }}
    .rate {{ font-weight: 600; color: #0b7a4b; }}
    .empty {{ background: #fff; border: 1px dashed #cbd2dc; border-radius: 10px;
      padding: 48px; text-align: center; color: #6b7686; }}
    .empty .hint {{ font-size: 13px; }}
    footer {{ max-width: 900px; margin: 0 auto; padding: 0 32px 40px;
      color: #8b93a1; font-size: 12px; }}
    .badge {{ display: inline-block; background: #eef2ff; color: #3a4a8c;
      border-radius: 6px; padding: 2px 8px; font-size: 11px; margin-left: 8px; }}
  </style>
</head>
<body>
  <header>
    <h1>Supplier Demand View
      <span class="badge">storage: {data['storage_mode']}</span>
    </h1>
    <p>Aggregate shopper demand across the DataCart storefront &mdash;
       views and add-to-cart intent per product.</p>
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
    </div>
{table}
  </main>
  <footer>
    Demand is aggregated from in-app clickstream capture. In production this
    view would be governed via Unity Catalog + Postgres roles, and the supplier
    would see only aggregated demand &mdash; never individual shopper data.
  </footer>
</body>
</html>"""


@router.get("/supplier", response_class=HTMLResponse)
def supplier_page():
    """Standalone HTML supplier dashboard (no shopper frontend dependency).

    Always renders a page — even with zero demand or an upstream failure, the
    shopper sees the friendly "No demand recorded yet" empty state, never a 500.
    """
    try:
        data = supplier_demand()
    except Exception as e:
        logger.warning(f"Supplier page fell back to empty view ({e}).")
        data = {"demand": [], "storage_mode": "unavailable",
                "total_views": 0, "total_add_to_cart": 0}
    return HTMLResponse(content=_render_page(data))
