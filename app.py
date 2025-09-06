# app.py
import os
import json
import time
import hashlib
from typing import Dict, Optional, List
import requests
from flask import Flask, request, jsonify

# Helpers from order_mapper.py (must be next to this file)
from order_mapper import (
    build_entry_order_payload,
    under_position_caps,
    load_strategy_config,  # used for TP/SL and qty
)

app = Flask(__name__)

# ===== Alpaca config =====
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

# ===== Optional OCO after plain entry =====
ATTACH_OCO_ON_PLAIN = os.getenv("ATTACH_OCO_ON_PLAIN", "true").lower() == "true"
OCO_TIF = os.getenv("OCO_TIF", "day")  # gtc|day

# ===== Wash-trade prevention: cancel any open orders before plain entry =====
CANCEL_OPEN_ORDERS_BEFORE_PLAIN = os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN", "true").lower() == "true"

# ===== Idempotency (suppress duplicate alerts briefly) =====
_IDEM_CACHE: Dict[str, float] = {}
IDEM_TTL_SEC = int(os.getenv("IDEM_TTL_SEC", 5))  # seconds

def _idem_key(system: str, side: str, symbol: str, price: float, ts_hint: str = "") -> str:
    payload = f"{system}|{side}|{symbol}|{price}|{ts_hint}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _idem_check_and_set(key: str) -> bool:
    now = time.time()
    # purge expired
    expired = [k for k, t in _IDEM_CACHE.items() if now - t > IDEM_TTL_SEC]
    for k in expired:
        _IDEM_CACHE.pop(k, None)
    if key in _IDEM_CACHE:
        return True
    _IDEM_CACHE[key] = now
    return False

# ===== Helpers =====
def get_open_positions_snapshot():
    """Counts for TOTAL / BY_SYMBOL; permissive if positions endpoint fails and STRICT is false."""
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
        return {"TOTAL": len(pos), "BY_SYMBOL": by_symbol, "BY_STRATEGY": {}}
    except Exception as e:
        app.logger.error(f"list_positions error: {e}")
        if STRICT_POSITION_CHECK:
            return {"TOTAL": 10**6, "BY_SYMBOL": {}, "BY_STRATEGY": {}}
        return {"TOTAL": 0, "BY_SYMBOL": {}, "BY_STRATEGY": {}}

def get_symbol_position(symbol: str) -> Optional[dict]:
    """Return the Alpaca position object for a symbol, or None if flat."""
    try:
        r = requests.get(f"{ALPACA_BASE}/positions/{symbol}", headers=HEADERS, timeout=5)
        if r.status_code == 404:
            return None  # flat
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        if getattr(e, "response", None) and e.response is not None and e.response.status_code == 404:
            return None
        app.logger.error(f"get_symbol_position error: {e}")
        return None if not STRICT_POSITION_CHECK else {"error": "unknown"}
    except Exception as e:
        app.logger.error(f"get_symbol_position error: {e}")
        return None if not STRICT_POSITION_CHECK else {"error": "unknown"}

def list_open_orders() -> List[dict]:
    """Return list of open orders (we'll filter by symbol)."""
    try:
        # Pull as many as practical; adjust if you have heavy order flow
        r = requests.get(f"{ALPACA_BASE}/orders?status=open&limit=200&nested=false", headers=HEADERS, timeout=6)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        app.logger.error(f"list_open_orders error: {e}")
        return []

def cancel_order(order_id: str):
    try:
        requests.delete(f"{ALPACA_BASE}/orders/{order_id}", headers=HEADERS, timeout=5)
    except Exception as e:
        app.logger.error(f"cancel_order {order_id} error: {e}")

def cancel_open_orders_for_symbol(symbol: str):
    """Cancel all open orders for a symbol. This prevents 'wash trade' rejections."""
    orders = list_open_orders()
    to_cancel = [o for o in orders if (o.get("symbol") == symbol)]
    for o in to_cancel:
        cancel_order(o.get("id"))
    if to_cancel:
        app.logger.info(f"CANCELLED {len(to_cancel)} open orders for {symbol}")

def build_plain_entry(symbol: str, side: str, price: float, system: str) -> dict:
    """
    Build a plain (non-bracket) order payload for cases where a position already exists.
    Uses the same per-strategy order_type/tif and qty envs as brackets.
    """
    cfg = load_strategy_config(system)
    payload = {
        "symbol": symbol,
        "qty": int(cfg.qty),
        "side": "buy" if side.upper() == "LONG" else "sell",
        "type": cfg.order_type,        # market or limit
        "time_in_force": cfg.tif,      # day or gtc
        "client_order_id": f"{system}-{symbol}-plain-{int(time.time()*1000)}",
    }
    # ✅ If entry is LIMIT, include limit_price
    if cfg.order_type.lower() == "limit":
        payload["limit_price"] = f"{float(price):.2f}"
    return payload


def adjust_bracket_to_base_price(payload: dict, side: str, base_price: float, tp_pct: float, sl_pct: float) -> dict:
    """
    Rebuild take_profit/stop_loss around Alpaca's base_price to satisfy bracket rules.
    Adds $0.02 cushion to clear +/-$0.01 constraint.
    """
    side = side.upper()
    bp = float(base_price)
    if side == "LONG":
        tp = max(bp * (1 + tp_pct), bp + 0.02)
        sl = min(bp * (1 - sl_pct), bp - 0.02)
    elif side == "SHORT":
        tp = min(bp * (1 - tp_pct), bp - 0.02)
        sl = max(bp * (1 + sl_pct), bp + 0.02)
    else:
        raise ValueError(f"Invalid side: {side}")
    new_payload = dict(payload)
    new_payload["take_profit"]["limit_price"] = f"{tp:.2f}"
    new_payload["stop_loss"]["stop_price"]    = f"{sl:.2f}"
    return new_payload

def build_oco_close(symbol: str, position_side: str, qty: int, ref_price: float, tp_pct: float, sl_pct: float, tif: str = "day") -> dict:
    """
    OCO close for an EXISTING position. position_side: 'long' or 'short'.
    qty is the FULL position size we want to protect.
    """
    pos_side = (position_side or "").lower()
    if pos_side not in ("long", "short"):
        raise ValueError(f"Invalid position_side for OCO: {position_side}")

    rp = float(ref_price)
    if pos_side == "long":
        # closing a long -> SELL OCO
        side = "sell"
        tp = rp * (1 + tp_pct)
        sl = rp * (1 - sl_pct)
    else:
        # closing a short -> BUY OCO
        side = "buy"
        tp = rp * (1 - tp_pct)
        sl = rp * (1 + sl_pct)

    return {
        "symbol": symbol,
        "qty": int(qty),
        "side": side,
        "time_in_force": tif,
        "order_class": "oco",
        "take_profit": {"limit_price": f"{tp:.2f}"},
        "stop_loss": {"stop_price": f"{sl:.2f}"},
        "client_order_id": f"OCO-{symbol}-{int(time.time()*1000)}",
    }

# ===== Health =====
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "equities-webhook", "time": int(time.time())}), 200

# ===== Webhook =====
@app.post("/webhook")
def webhook():
    """
    JSON:
    {
      "system": "SMA10D_MACD" | "SPY_VWAP_EMA20",
      "side": "LONG" | "SHORT",
      "ticker": "COIN" | "SPY" | ...,
      "price": 123.45,
      "time": "2025-09-05T13:35:01Z"   # optional
    }
    """
    data = request.get_json(force=True, silent=True) or {}
    system  = data.get("system")
    side    = data.get("side")
    symbol  = data.get("ticker")
    price   = data.get("price")
    tv_time = data.get("time", "")

    if not all([system, side, symbol, price is not None]):
        return jsonify({"ok": False, "error": "missing fields: system/side/ticker/price"}), 400

    # Idempotency
    key = _idem_key(system, side, symbol, float(price), tv_time[:19])
    if _idem_check_and_set(key):
        app.logger.info(f"SKIP: Idempotent duplicate {system} {side} {symbol} @{price}")
        return jsonify({"ok": True, "skipped": "duplicate"}), 200

    # Caps
    open_pos = get_open_positions_snapshot()
    if not under_position_caps(open_pos, symbol, system):
        app.logger.info("SKIP: Max positions reached")
        return jsonify({"ok": True, "skipped": "max_positions"}), 200

    # Choose bracket vs plain
    pos_before = get_symbol_position(symbol)
    use_bracket = pos_before is None  # flat => bracket; has position => plain

    # If we're going to place a PLAIN entry, cancel all open orders first (prevents wash-trade)
    if not use_bracket and CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
        cancel_open_orders_for_symbol(symbol)

    # Build payload
    try:
        if use_bracket:
            payload = build_entry_order_payload(symbol, side, float(price), system)
        else:
            payload = build_plain_entry(symbol, side, float(price), system)
    except Exception as e:
        app.logger.error(f"PAYLOAD_ERROR: {e}")
        return jsonify({"ok": False, "error": f"payload_error: {e}"}), 400

    app.logger.info(
        f"PLACE: {symbol} {payload['side']} qty={payload.get('qty')} @{price} "
        f"system={system} bracket={use_bracket} payload={payload}"
    )

    # Place order (with base_price retry ONLY for bracket orders)
    try:
        o = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=10)

        if use_bracket and o.status_code == 422:
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else {"message": o.text}
            app.logger.error(f"ALPACA_POST_ERROR status=422 body={body} sent={payload}")

            err_obj = body if isinstance(body, dict) else {}
            alpaca_err = err_obj.get("alpaca_error", err_obj)
            msg = (alpaca_err.get("message") or "").lower()
            base_price = alpaca_err.get("base_price")

            retry_reasons = ("take_profit.limit_price must be", "stop_loss.stop_price must be")
            if base_price and any(r in msg for r in retry_reasons):
                cfg = load_strategy_config(system)
                adj_payload = adjust_bracket_to_base_price(payload, side, float(base_price), cfg.tp_pct, cfg.sl_pct)
                app.logger.info(f"RETRY with base_price={base_price}: {adj_payload}")

                o2 = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(adj_payload), timeout=10)
                if o2.status_code >= 400:
                    ctype2 = o2.headers.get("content-type", "")
                    body2 = o2.json() if ctype2.startswith("application/json") else o2.text
                    app.logger.error(f"ALPACA_POST_ERROR (retry) status={o2.status_code} body={body2} sent={adj_payload}")
                    return jsonify({"ok": False, "alpaca_error": body2}), 422
                order_json = o2.json()
                # bracket success on retry
                return jsonify({"ok": True, "order": order_json, "note": "placed on retry using Alpaca base_price"}), 200

            # Different 422
            return jsonify({"ok": False, "alpaca_error": body}), 422

        # Other non-2xx
        if o.status_code >= 400:
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else o.text
            app.logger.error(f"ALPACA_POST_ERROR status={o.status_code} body={body} sent={payload}")
            return jsonify({"ok": False, "alpaca_error": body}), 422

        # ===== Success =====
        order_json = o.json()

        # If PLAIN and OCO enabled, re-attach a single OCO for the FULL current position
        if not use_bracket and ATTACH_OCO_ON_PLAIN:
            try:
                cfg = load_strategy_config(system)

                # Refresh the position AFTER entry so qty/avg_entry are up-to-date
                pos_after = get_symbol_position(symbol)
                if pos_after is None:
                    # Edge-case: position closed immediately or couldn't fetch; fallback to before
                    pos_after = pos_before

                # Determine side, qty, and reference price
                pos_side = (pos_after or {}).get("side") or ((pos_before or {}).get("side"))
                # qty in Alpaca positions is a string; abs to handle shorts
                qty_total = int(abs(float((pos_after or {}).get("qty") or (pos_before or {}).get("qty") or 0)))
                ref_price = float((pos_after or {}).get("avg_entry_price") or price)

                if qty_total > 0 and pos_side:
                    oco_payload = build_oco_close(
                        symbol=symbol,
                        position_side=pos_side,
                        qty=qty_total,
                        ref_price=ref_price,
                        tp_pct=cfg.tp_pct,
                        sl_pct=cfg.sl_pct,
                        tif=OCO_TIF,
                    )
                    app.logger.info(f"ATTACH OCO (full position): {oco_payload}")
                    oco_resp = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(oco_payload), timeout=10)
                    if oco_resp.status_code >= 400:
                        ctype3 = oco_resp.headers.get("content-type", "")
                        body3 = oco_resp.json() if ctype3.startswith("application/json") else oco_resp.text
                        app.logger.error(f"OCO_POST_ERROR status={oco_resp.status_code} body={body3} sent={oco_payload}")
                        return jsonify({"ok": True, "order": order_json, "oco_error": body3}), 200
                    else:
                        return jsonify({"ok": True, "order": order_json, "oco": oco_resp.json()}), 200
                else:
                    app.logger.info("OCO skipped: no position qty detected after plain entry.")
                    return jsonify({"ok": True, "order": order_json, "note": "no position qty detected for OCO"}), 200
            except Exception as e:
                app.logger.error(f"OCO_BUILD_OR_POST_ERROR: {e}")
                return jsonify({"ok": True, "order": order_json, "oco_error": str(e)}), 200

        # Bracket path or OCO disabled
        return jsonify({"ok": True, "order": order_json}), 200

    except Exception as e:
        app.logger.error(f"ORDER ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Performance stub =====
@app.get("/performance")
def performance():
    days = request.args.get("days", "7")
    try:
        d = int(days)
    except Exception:
        d = 7
    return jsonify({"ok": True, "days": d, "note": "Replace with real aggregation.", "by_strategy": {}}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
