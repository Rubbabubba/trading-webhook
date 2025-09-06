# app.py
import os
import json
import time
import hashlib
from typing import Dict, Optional
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
# Base explicitly includes /v2 per your requirement
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

def build_plain_entry(symbol: str, side: str, price: float, system: str) -> dict:
    """
    Build a plain (non-bracket) order payload for cases where a position already exists.
    Uses the same per-strategy order_type/tif and qty envs as brackets.
    """
    cfg = load_strategy_config(system)
    return {
        "symbol": symbol,
        "qty": int(cfg.qty),
        "side": "buy" if side.upper() == "LONG" else "sell",
        "type": cfg.order_type,        # market or limit
        "time_in_force": cfg.tif,      # day or gtc
        "client_order_id": f"{system}-{symbol}-plain-{int(time.time()*1000)}",
    }

def adjust_bracket_to_base_price(payload: dict, side: str, base_price: float, tp_pct: float, sl_pct: float) -> dict:
    """
    Rebuild take_profit/stop_loss around Alpaca's base_price to satisfy bracket rules.
    Keep the entry side the same (buy for LONG, sell for SHORT), only TP/SL change.
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

def build_oco_close(symbol: str, position_side: str, qty: int, ref_price: float, tp_pct: float, sl_pct: float, tif: str = "day") -> dict:
    """
    Build an OCO close order for an EXISTING position.
    position_side: 'long' or 'short' (as Alpaca returns in the position object)
    Ref price usually avg_entry_price; fallback to alert price if not available.
    """
    pos_side = (position_side or "").lower()
    if pos_side not in ("long", "short"):
        raise ValueError(f"Invalid position_side for OCO: {position_side}")

    rp = float(ref_price)
    if pos_side == "long":
        # closing a long -> place SELL OCO, TP above, SL below
        side = "sell"
        tp = rp * (1 + tp_pct)
        sl = rp * (1 - sl_pct)
    else:
        # closing a short -> place BUY OCO, TP below, SL above
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

    # Idempotency (avoid duplicate placements within IDEM_TTL_SEC)
    key = _idem_key(system, side, symbol, float(price), tv_time[:19])  # trim to seconds
    if _idem_check_and_set(key):
        app.logger.info(f"SKIP: Idempotent duplicate {system} {side} {symbol} @{price}")
        return jsonify({"ok": True, "skipped": "duplicate"}), 200

    # Position caps
    open_pos = get_open_positions_snapshot()
    if not under_position_caps(open_pos, symbol, system):
        app.logger.info("SKIP: Max positions reached")
        return jsonify({"ok": True, "skipped": "max_positions"}), 200

    # Decide bracket vs plain based on current position in this symbol
    pos = get_symbol_position(symbol)
    use_bracket = pos is None  # flat => bracket; has position => plain

    # Build payload
    try:
        if use_bracket:
            # Flat in this symbol -> bracket entry allowed
            payload = build_entry_order_payload(symbol, side, float(price), system)
        else:
            # Already have a position -> send a plain order (no bracket)
            payload = build_plain_entry(symbol, side, float(price), system)
    except Exception as e:
        app.logger.error(f"PAYLOAD_ERROR: {e}")
        return jsonify({"ok": False, "error": f"payload_error: {e}"}), 400

    app.logger.info(
        f"PLACE: {symbol} {payload['side']} qty={payload.get('qty')} @{price} "
        f"system={system} bracket={use_bracket} payload={payload}"
    )

    # ===== Place order (with one smart retry for bracket TP/SL base_price constraint) =====
    try:
        o = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=8)

        # Only bracket orders may need TP/SL correction using base_price
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

                o2 = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(adj_payload), timeout=8)
                if o2.status_code >= 400:
                    ctype2 = o2.headers.get("content-type", "")
                    body2 = o2.json() if ctype2.startswith("application/json") else o2.text
                    app.logger.error(f"ALPACA_POST_ERROR (retry) status={o2.status_code} body={body2} sent={adj_payload}")
                    return jsonify({"ok": False, "alpaca_error": body2}), 422

                # On success (retry), return success
                return jsonify({"ok": True, "order": o2.json(), "note": "placed on retry using Alpaca base_price"}), 200

            # Different 422 reason (or no base_price) -> bubble up
            return jsonify({"ok": False, "alpaca_error": body}), 422

        # Any other non-2xx
        if o.status_code >= 400:
            ctype = o.headers.get("content-type", "")
            body = o.json() if ctype.startswith("application/json") else o.text
            app.logger.error(f"ALPACA_POST_ERROR status={o.status_code} body={body} sent={payload}")
            return jsonify({"ok": False, "alpaca_error": body}), 422

        # ===== Success path =====
        order_json = o.json()

        # If this was a PLAIN entry and OCO attachment is enabled, try to post an OCO for the *newly added* qty
        if not use_bracket and ATTACH_OCO_ON_PLAIN:
            try:
                cfg = load_strategy_config(system)
                # Prefer the position avg entry (if we have it), else use alert price
                # Refresh position snapshot to include this new add (best-effort)
                pos_after = get_symbol_position(symbol) or pos  # fallback to earlier pos
                pos_side = (pos_after or {}).get("side") or ((pos or {}).get("side"))
                avg_entry = (pos_after or {}).get("avg_entry_price") or ((pos or {}).get("avg_entry_price")) or price
                qty_new = int(payload.get("qty", 1))

                oco_payload = build_oco_close(
                    symbol=symbol,
                    position_side=pos_side,
                    qty=qty_new,
                    ref_price=float(avg_entry),
                    tp_pct=cfg.tp_pct,
                    sl_pct=cfg.sl_pct,
                    tif=OCO_TIF,
                )
                app.logger.info(f"ATTACH OCO: {oco_payload}")
                oco_resp = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(oco_payload), timeout=8)
                if oco_resp.status_code >= 400:
                    ctype3 = oco_resp.headers.get("content-type", "")
                    body3 = oco_resp.json() if ctype3.startswith("application/json") else oco_resp.text
                    app.logger.error(f"OCO_POST_ERROR status={oco_resp.status_code} body={body3} sent={oco_payload}")
                    # Do not fail the original success; just include note
                    return jsonify({"ok": True, "order": order_json, "oco_error": body3}), 200
                else:
                    return jsonify({"ok": True, "order": order_json, "oco": oco_resp.json()}), 200
            except Exception as e:
                app.logger.error(f"OCO_BUILD_OR_POST_ERROR: {e}")
                return jsonify({"ok": True, "order": order_json, "oco_error": str(e)}), 200

        # If bracket or OCO disabled, just return the order success
        return jsonify({"ok": True, "order": order_json}), 200

    except Exception as e:
        app.logger.error(f"ORDER ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# ===== Performance stub (safe to keep if you don't have DB wired yet) =====
@app.get("/performance")
def performance():
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
