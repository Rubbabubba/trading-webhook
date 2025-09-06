# app.py
import os
import json
import time
import hashlib
from typing import Dict
import requests
from flask import Flask, request, jsonify

# Import our helpers (place order_mapper.py next to this file)
from order_mapper import build_entry_order_payload, under_position_caps

app = Flask(__name__)

# ===== Alpaca config =====
# You asked for the base to include /v2 explicitly:
ALPACA_BASE = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY", "")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Content-Type": "application/json",
}

# ===== Position caps (env-driven) =====
MAX_OPEN_POSITIONS        = int(os.getenv("MAX_OPEN_POSITIONS", 5))
MAX_POSITIONS_PER_SYMBOL  = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", 1))
MAX_POSITIONS_PER_STRATEGY= int(os.getenv("MAX_POSITIONS_PER_STRATEGY", 3))
STRICT_POSITION_CHECK     = os.getenv("STRICT_POSITION_CHECK", "false").lower() == "true"

# ===== Idempotency (basic de-dupe for rapid duplicate alerts) =====
# Keep a short-lived memory of alert signatures to avoid placing the same order twice.
_IDEM_CACHE: Dict[str, float] = {}
IDEM_TTL_SEC = int(os.getenv("IDEM_TTL_SEC", 5))  # de-dupe window (seconds)

def _idem_key(system: str, side: str, symbol: str, price: float, ts_hint: str = "") -> str:
    # Use a stable hash over the core alert fields within a tiny time window.
    payload = f"{system}|{side}|{symbol}|{price}|{ts_hint}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _idem_check_and_set(key: str) -> bool:
    # returns True if already seen (duplicate), else stores and returns False
    now = time.time()
    # purge expired
    to_del = [k for k, t in _IDEM_CACHE.items() if now - t > IDEM_TTL_SEC]
    for k in to_del:
        _IDEM_CACHE.pop(k, None)

    if key in _IDEM_CACHE:
        return True
    _IDEM_CACHE[key] = now
    return False

# ===== Helpers =====
def get_open_positions_snapshot():
    """
    Snapshot: counts for TOTAL / BY_SYMBOL / (optional) BY_STRATEGY.
    If Alpaca is down and STRICT_POSITION_CHECK=false, allow orders.
    """
    try:
        r = requests.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=5)
        r.raise_for_status()
        pos = r.json()
        by_symbol = {}
        for p in pos:
            sym = p.get("symbol")
            if not sym:
                continue
            by_symbol[sym] = by_symbol.get(sym, 0) + 1

        # BY_STRATEGY is left empty unless you tag orders with strategy in account/notes and fetch elsewhere
        snapshot = {
            "TOTAL": len(pos),
            "BY_SYMBOL": by_symbol,
            "BY_STRATEGY": {}
        }
        return snapshot
    except Exception as e:
        app.logger.error(f"list_positions error: {e}")
        if STRICT_POSITION_CHECK:
            # Enforce caps by pretending we are at the cap
            return {"TOTAL": 10**6, "BY_SYMBOL": {}, "BY_STRATEGY": {}}
        # Otherwise, allow trading even if positions endpoint errored
        return {"TOTAL": 0, "BY_SYMBOL": {}, "BY_STRATEGY": {}}

# ===== Health endpoint =====
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "equities-webhook", "time": int(time.time())}), 200

# ===== Webhook endpoint (TradingView -> Flask -> Alpaca) =====
@app.post("/webhook")
def webhook():
    """
    Expected JSON:
    {
      "system": "SMA10D_MACD" | "SPY_VWAP_EMA20",
      "side": "LONG" | "SHORT",
      "ticker": "COIN" | "SPY" | ...,
      "price": 123.45,
      "time": "2025-09-05T13:35:01Z"   # optional, but recommended
    }
    """
    data = request.get_json(force=True, silent=True) or {}
    system = data.get("system")
    side = data.get("side")
    symbol = data.get("ticker")
    price = data.get("price")
    tv_time = data.get("time", "")

    # Basic validation
    if not all([system, side, symbol, price is not None]):
        return jsonify({"ok": False, "error": "missing fields: system/side/ticker/price"}), 400

    # Basic idempotency (avoid duplicate placements within IDEM_TTL_SEC)
    key = _idem_key(system, side, symbol, float(price), tv_time[:19])  # trim to seconds
    if _idem_check_and_set(key):
        app.logger.info(f"SKIP: Idempotent duplicate {system} {side} {symbol} @{price}")
        return jsonify({"ok": True, "skipped": "duplicate"}), 200

    # Position caps
    open_pos = get_open_positions_snapshot()
    # We pass through our known env-driven caps using the helper
    # (under_position_caps reads env internally for max thresholds)
    if not under_position_caps(open_pos, symbol, system):
        app.logger.info("SKIP: Max positions reached")
        return jsonify({"ok": True, "skipped": "max_positions"}), 200

    # Build a VALID ENTRY bracket (fixes 422 'bracket orders must be entry orders')
    try:
        payload = build_entry_order_payload(symbol, side, float(price), system)
    except Exception as e:
        app.logger.error(f"PAYLOAD_ERROR: {e}")
        return jsonify({"ok": False, "error": f"payload_error: {e}"}), 400

    app.logger.info(f"PLACE: {symbol} {payload['side']} qty={payload['qty']} @{price} system={system} payload={payload}")

    # Place order to Alpaca
    try:
        o = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=8)
        if o.status_code >= 400:
            # Handle both JSON and text error bodies
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else o.text
            app.logger.error(f"ALPACA_POST_ERROR status={o.status_code} body={body} sent={payload}")
            return jsonify({"ok": False, "alpaca_error": body}), 422
        return jsonify({"ok": True, "order": o.json()}), 200
    except Exception as e:
        app.logger.error(f"ORDER ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Optional: simple performance stub (you likely have your own) =====
# Keep your existing /performance implementation if you already track trades.
# Below is a safe placeholder that won't break callers but returns minimal info.
@app.get("/performance")
def performance():
    # Non-invasive stub so existing dashboards don't 500 if you haven't wired a DB yet.
    days = request.args.get("days", "7")
    return jsonify({
        "ok": True,
        "days": int(days) if str(days).isdigit() else 7,
        "note": "Replace this stub with your real performance aggregation.",
        "by_strategy": {}
    }), 200

if __name__ == "__main__":
    # Bind 0.0.0.0 for Render/containers; default port 8080
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
