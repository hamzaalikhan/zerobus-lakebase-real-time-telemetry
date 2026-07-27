"""Best-effort clickstream producer — pushes storefront events to Zerobus.

This is the "collect" step of the workshop loop, done the way a real deployment
would: every product view / click / add-to-cart the shopper performs is pushed
straight into a Unity Catalog Delta table via **Zerobus** — a serverless push
API (no Kafka, no message bus). From there Lab 3.1's DLT pipeline aggregates it
into demand signals and syncs the result back to Lakebase for the Supplier View.

Design discipline (mirrors server/events.py — telemetry must never break the shop):
  * Fully best-effort. Every public call is wrapped so a failure to emit an event
    can never raise into the shopper's request path.
  * Lazy, self-healing init. The Zerobus stream is created on first use. If the
    SDK isn't installed, the target table doesn't exist, the SP lacks MODIFY, or
    Zerobus isn't region-enabled, we log once and silently no-op thereafter — the
    storefront keeps serving; only the clickstream goes dark.
  * No aggregation here. The app only EMITS raw events. All rollups (dedup,
    joins, demand math) happen in the lakehouse pipeline (Lab 3.1), not in-app.

Configuration (all via env; set by the app deployment):
  * ZEROBUS_ENABLED       — "true" to turn the producer on (default off, so the
                            app runs unchanged until Lab 3.1 flips it on).
  * ZEROBUS_ENDPOINT      — region-specific server endpoint,
                            https://<workspace-id>.zerobus.<region>.cloud.databricks.com
  * ZEROBUS_BRONZE_TABLE  — fully-qualified UC target, <catalog>.ecommerce.clickstream_bronze
  * DATABRICKS_HOST       — workspace URL (auto-injected in Apps runtime)
  * DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET — the app SP's OAuth creds
                            (auto-injected in Apps runtime; used to authenticate ingest).
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Event type constants — one bronze table, discriminated by this column.
VIEW = "view"
CLICK = "click"
ADD_TO_CART = "add_to_cart"

# --- Configuration from the environment ---------------------------------------
def _normalize_url(u: str) -> str:
    """Ensure a workspace/endpoint URL has an https scheme and no trailing slash.

    Databricks Apps inject DATABRICKS_HOST, but not always with a scheme; the
    Zerobus SDK's OAuth token factory needs a full https URL or it fails with a
    URL 'builder error'.
    """
    u = (u or "").strip().rstrip("/")
    if u and not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


ENABLED = os.environ.get("ZEROBUS_ENABLED", "").lower() in ("1", "true", "yes")
ENDPOINT = _normalize_url(os.environ.get("ZEROBUS_ENDPOINT", ""))
BRONZE_TABLE = os.environ.get("ZEROBUS_BRONZE_TABLE", "").strip()
WORKSPACE_URL = _normalize_url(os.environ.get("DATABRICKS_HOST", ""))
CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

# Lazy singleton stream + a tri-state readiness flag.
#   None  = not yet initialized
#   True  = stream is live, emitting to Zerobus
#   False = init failed (SDK/config/region) — permanently no-op this process
_stream = None
_ready: bool | None = None
_lock = threading.Lock()

# Flush to Delta every N events so the clickstream appears promptly during the
# workshop (instead of only on shutdown). Small N = fresher data, more flush calls.
FLUSH_EVERY = 25
_since_flush = 0


def _config_complete() -> tuple[bool, str]:
    """Return (ok, reason). ok=False means we can't even attempt a stream."""
    if not ENABLED:
        return False, "ZEROBUS_ENABLED is not set — producer disabled"
    # "unset" is the bundle's non-empty placeholder (the Apps API rejects empty
    # env values); treat it as not-configured.
    missing = [
        name
        for name, val in (
            ("ZEROBUS_ENDPOINT", ENDPOINT),
            ("ZEROBUS_BRONZE_TABLE", BRONZE_TABLE),
            ("DATABRICKS_HOST", WORKSPACE_URL),
            ("DATABRICKS_CLIENT_ID", CLIENT_ID),
            ("DATABRICKS_CLIENT_SECRET", CLIENT_SECRET),
        )
        if not val or val == "unset"
    ]
    if missing:
        return False, f"missing/placeholder env: {', '.join(missing)}"
    return True, ""


def _init_stream() -> bool:
    """Create the Zerobus stream once. Returns True if it's usable."""
    global _stream, _ready
    if _ready is not None:
        return _ready
    with _lock:
        if _ready is not None:
            return _ready

        ok, reason = _config_complete()
        if not ok:
            _ready = False
            logger.info(f"Zerobus producer off ({reason}). Storefront unaffected.")
            return _ready

        try:
            # Imported lazily so the app runs even if the SDK isn't installed.
            from zerobus.sdk.sync import ZerobusSdk
            from zerobus.sdk.shared import (
                RecordType,
                StreamConfigurationOptions,
                TableProperties,
            )

            logger.info(
                f"Opening Zerobus stream: endpoint={ENDPOINT} workspace_url={WORKSPACE_URL} "
                f"table={BRONZE_TABLE}"
            )
            sdk = ZerobusSdk(ENDPOINT, WORKSPACE_URL, application_name="datacart-storefront/1.0")
            table_properties = TableProperties(BRONZE_TABLE)  # descriptor None => JSON mode
            options = StreamConfigurationOptions(record_type=RecordType.JSON)
            _stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)
            _ready = True
            logger.info(f"Zerobus stream open → {BRONZE_TABLE} (endpoint {ENDPOINT})")
        except Exception as e:
            _ready = False
            logger.warning(
                f"Could not open Zerobus stream ({e}); clickstream disabled. "
                f"Storefront continues serving normally."
            )
        return _ready


def emit(event_type: str, product_id: int) -> None:
    """Push a single clickstream event to Zerobus. Best-effort; never raises.

    JSON record type expects a JSON *string* payload (not a dict). The bronze
    table columns are (event_type, product_id, event_ts).
    """
    if not ENABLED:
        return
    global _since_flush
    try:
        if not _init_stream():
            return
        payload = json.dumps(
            {
                "event_type": event_type,
                "product_id": int(product_id),
                "event_ts": datetime.now(timezone.utc).isoformat(),
            }
        )
        # ingest_record_offset queues the record and returns immediately; the SDK
        # sends and tracks acks in the background. We do NOT flush per-event (that
        # would add latency to the shopper's request) — instead we flush every
        # FLUSH_EVERY events so the clickstream becomes visible in Delta within a
        # workshop's attention span rather than only on app shutdown.
        _stream.ingest_record_offset(payload)
        _since_flush += 1
        if _since_flush >= FLUSH_EVERY:
            _since_flush = 0
            _stream.flush()
    except Exception as e:
        # Downgrade a broken stream so we don't spam the same failure per request.
        _mark_broken(e)


def _mark_broken(err: Exception) -> None:
    """Tear down a failed stream and stop emitting for the rest of this process."""
    global _stream, _ready
    with _lock:
        if _ready is not False:
            logger.warning(f"Zerobus emit failed ({err}); disabling clickstream for this process.")
        _ready = False
        stream, _stream = _stream, None
    if stream is not None:
        try:
            stream.close()
        except Exception:
            pass


def shutdown() -> None:
    """Flush and close the stream on app shutdown. Best-effort."""
    global _stream, _ready
    with _lock:
        stream, _stream = _stream, None
        _ready = False
    if stream is not None:
        try:
            stream.flush()
        except Exception:
            pass
        try:
            stream.close()
        except Exception:
            pass


def status() -> str:
    """Human-readable producer state, for the Supplier View / health checks."""
    if not ENABLED:
        return "disabled (ZEROBUS_ENABLED off)"
    if _ready is True:
        return f"streaming → {BRONZE_TABLE}"
    if _ready is False:
        return "unavailable (see logs)"
    return "not yet initialized"


def debug_info() -> dict:
    """Diagnostic snapshot of the producer's resolved config (no secrets)."""
    return {
        "enabled": ENABLED,
        "endpoint": ENDPOINT,
        "workspace_url": WORKSPACE_URL,
        "bronze_table": BRONZE_TABLE,
        "client_id_present": bool(CLIENT_ID),
        "client_secret_present": bool(CLIENT_SECRET),
        "ready": _ready,
        "status": status(),
    }
