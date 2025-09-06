# app.py
import os
import json
import time
import hashlib
from typing import Dict
import requests
from flask import Flask, request, jsonify

# Import helpers from order_mapper.py (place this file next to app.py)
from order_mapper import (
    build_entry_order_payload,
    under_position_caps,
    load_strategy_config,  # used for TP/SL on 422 retry
)

app = Flask(__name__)

# ===== Alpaca config =====
# Base explicitly includes /v2 as requested
ALPACA_BASE = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY", "")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Content-Type": "application/json",
}

# ===== Position caps (env-driven) =====
MAX_OPEN_POSITIONS         = int(os.getenv("MAX_OPEN_POSITIONS", 5))
MAX_POSITIONS_PER_SYMBOL   = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", 1))
MAX_POSITIONS_PER_STRATEGY = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", 3))
STRICT_POSITION_CHECK      = os.getenv("STRICT_POSITION_CHECK", "false").lower() == "true"

# ===== Idempotency (basic de-dupe for rapid duplicate alerts) =====
_IDEM_CACHE: Dict[str, float] = {}
IDEM_TTL_SEC = int(os.getenv("IDEM_TTL_SEC", 5))  # window in seconds

def _idem_key(system: str, side: str, symbol: str, price: float, ts_hint: str = "") -> str:
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
        return {
            "TOTAL": len(pos),
            "BY_SYMBOL": by_symbol,
            "BY_STRATEGY": {},  # populate if you track per-strategy open positions
        }
    except Exception as e:
        app.logger.error(f"list_positions error: {e}")
        if STRICT_POSITION_CHECK:
            # Enforce caps by pretending we are at/over cap
            return {"TOTAL": 10**6, "BY_SYMBOL": {}, "BY_STRATEGY": {}}
        # Otherwise, allow trading even if positions endpoint errored
        return {"TOTAL": 0, "BY_SYMBOL": {}, "BY_STRATEGY": {}}

def adjust_bracket_to_base_price(payload: dict, side: str, base_price: float, tp_pct: float, sl_pct: float) -> dict:
    """
    Rebuild take_profit/stop_loss around Alpaca's base_price to satisfy bracket rules.
    We keep the entry side the same (buy for LONG, sell for SHORT), only TP/SL change.
    Adds $0.02 cushion to clear Alpaca's +/- $0.01 constraint.
    """
    side = side.upper()
    bp = float(base_price)

    if side == "LONG":
        # LONG: TP >= base_price + 0.01 ; SL <= base_price - 0.01
        tp = max(bp * (1 + tp_pct), bp + 0.02)
        sl = min(bp * (1 - sl_pct), bp - 0.02)
    elif side == "SHORT":
        # SHORT: TP <= base_price - 0.01 ; SL >= base_price + 0.01
        tp = min(bp * (1 - tp_pct), bp - 0.02)
        sl = max(bp * (1 + sl_pct), bp + 0.02)
    else:
        raise ValueError(f"Invalid side: {side}")

    new_payload = dict(payload)
    new_payload["take_profit"]["limit_price"] = f"{tp:.2f}"
    new_payload["stop_loss"]["stop_price"]    = f"{sl:.2f}"
    return new_payload

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
    system  = data.get("system")
    side    = data.get("side")
    symbol  = data.get("ticker")
    price   = data.get("price")
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

    # ===== Place order with one smart retry if TP/SL violate Alpaca base_price constraints =====
    try:
        o = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=8)

        if o.status_code == 422:
            # Parse JSON error if present
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else {"message": o.text}
            app.logger.error(f"ALPACA_POST_ERROR status=422 body={body} sent={payload}")

            # If Alpaca returned a base_price and the message mentions TP/SL constraint, rebuild around base_price and retry once
            err_obj = body if isinstance(body, dict) else {}
            alpaca_err = err_obj.get("alpaca_error", err_obj)
            msg = (alpaca_err.get("message") or "").lower()
            base_price = alpaca_err.get("base_price")

            retry_reasons = ("take_profit.limit_price must be", "stop_loss.stop_price must be")
            if base_price and any(r in msg for r in retry_reasons):
                # Load TP/SL pct from strategy config and adjust around base_price
                cfg = load_strategy_config(system)
                adj_payload = adjust_bracket_to_base_price(payload, side, float(base_price), cfg.tp_pct, cfg.sl_pct)
                app.logger.info(f"RETRY with base_price={base_price}: {adj_payload}")

                o2 = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(adj_payload), timeout=8)
                if o2.status_code >= 400:
                    ctype2 = o2.headers.get("content-type", "")
                    body2 = o2.json() if ctype2.startswith("application/json") else o2.text
                    app.logger.error(f"ALPACA_POST_ERROR (retry) status={o2.status_code} body={body2} sent={adj_payload}")
                    return jsonify({"ok": False, "alpaca_error": body2}), 422

                return jsonify({"ok": True, "order": o2.json(), "note": "placed on retry using Alpaca base_price"}), 200

            # No base_price or different error -> bubble up
            return jsonify({"ok": False, "alpaca_error": body}), 422

        if o.status_code >= 400:
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else o.text
            app.logger.error(f"ALPACA_POST_ERROR status={o.status_code} body={body} sent={payload}")
            return jsonify({"ok": False, "alpaca_error": body}), 422

        # Success
        return jsonify({"ok": True, "order": o.json()}), 200

    except Exception as e:
        app.logger.error(f"ORDER ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Optional: simple performance stub =====
@app.get("/performance")
def performance():
    # Non-invasive stub so existing dashboards don't 500 if you haven't wired a DB yet.
    days = request.args.get("days", "7")
    try:
        d = int(days)
    except Exception:
        d = 7
    return jsonify({
        "ok": True,
        "days": d,
        "note": "Replace this stub with your real performance aggregation.",
        "by_strategy": {}
    }), 200

if __name__ == "__main__":
    # Bind 0.0.0.0 for Render/containers; default port 8080
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
