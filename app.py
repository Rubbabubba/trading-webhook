# app.py
import os
import json
import time
import hashlib
from typing import Dict, Optional, List
from datetime import datetime, timedelta, timezone
import requests
from flask import Flask, request, jsonify, make_response
from zoneinfo import ZoneInfo

# ==== Order helpers (must exist in order_mapper.py next to this file) ====
from order_mapper import (
    build_entry_order_payload,  # builds bracket payload for flat entries
    under_position_caps,        # checks MAX_* caps
    load_strategy_config,       # exposes qty, order_type, tif, tp_pct, sl_pct for a system
)

app = Flask(__name__)

# ===== Alpaca trading API config =====
# Note the explicit /v2 suffix
ALPACA_BASE = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_KEY = os.getenv("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY", "")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
    "Content-Type": "application/json",
}

# ===== Alpaca Market Data (scanner) =====
APCA_DATA_BASE_URL = os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets/v2")
APCA_DATA_FEED     = os.getenv("APCA_DATA_FEED", "iex")   # "iex" (paper/free) or "sip" (paid)
SELF_BASE_URL      = os.getenv("SELF_BASE_URL", "")       # e.g. https://trading-webhook-xxxx.onrender.com

# ===== Position caps (env-driven) =====
MAX_OPEN_POSITIONS         = int(os.getenv("MAX_OPEN_POSITIONS", 5))
MAX_POSITIONS_PER_SYMBOL   = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", 1))
MAX_POSITIONS_PER_STRATEGY = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", 3))
STRICT_POSITION_CHECK      = os.getenv("STRICT_POSITION_CHECK", "false").lower() == "true"

# ===== Optional OCO after plain entry =====
ATTACH_OCO_ON_PLAIN = os.getenv("ATTACH_OCO_ON_PLAIN", "true").lower() == "true"
OCO_TIF = os.getenv("OCO_TIF", "day")  # gtc|day

# ===== Wash-trade prevention: cancel open orders before plain entry =====
CANCEL_OPEN_ORDERS_BEFORE_PLAIN = os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN", "true").lower() == "true"

# ===== Idempotency (suppress duplicate alerts briefly) =====
_IDEM_CACHE: Dict[str, float] = {}
IDEM_TTL_SEC = int(os.getenv("IDEM_TTL_SEC", 5))  # seconds

def _idem_key(system: str, side: str, symbol: str, price: float, ts_hint: str = "") -> str:
    payload = f"{system}|{side}|{symbol}|{price}|{ts_hint}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _idem_check_and_set(key: str) -> bool:
    now = time.time()
    expired = [k for k, t in _IDEM_CACHE.items() if now - t > IDEM_TTL_SEC]
    for k in expired:
        _IDEM_CACHE.pop(k, None)
    if key in _IDEM_CACHE:
        return True
    _IDEM_CACHE[key] = now
    return False

# ===== Helpers (trading) =====
def get_open_positions_snapshot():
    """Counts for TOTAL / BY_SYMBOL; permissive if positions endpoint fails and STRICT is false."""
    try:
        r = requests.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=6)
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
        r = requests.get(f"{ALPACA_BASE}/positions/{symbol}", headers=HEADERS, timeout=6)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        if getattr(e, "response", None) is not None and e.response.status_code == 404:
            return None
        app.logger.error(f"get_symbol_position error: {e}")
        return None if not STRICT_POSITION_CHECK else {"error": "unknown"}
    except Exception as e:
        app.logger.error(f"get_symbol_position error: {e}")
        return None if not STRICT_POSITION_CHECK else {"error": "unknown"}

def list_open_orders() -> List[dict]:
    try:
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
    orders = list_open_orders()
    to_cancel = [o for o in orders if (o.get("symbol") == symbol)]
    for o in to_cancel:
        cancel_order(o.get("id"))
    if to_cancel:
        app.logger.info(f"CANCELLED {len(to_cancel)} open orders for {symbol}")

def build_plain_entry(symbol: str, side: str, price: float, system: str) -> dict:
    """Plain (non-bracket) entry when a position already exists. Adds limit_price if needed."""
    cfg = load_strategy_config(system)
    payload = {
        "symbol": symbol,
        "qty": int(cfg.qty),
        "side": "buy" if side.upper() == "LONG" else "sell",
        "type": cfg.order_type,
        "time_in_force": cfg.tif,
        "client_order_id": f"{system}-{symbol}-plain-{int(time.time()*1000)}",
    }
    if str(cfg.order_type).lower() == "limit":
        payload["limit_price"] = f"{float(price):.2f}"
    return payload

def adjust_bracket_to_base_price(payload: dict, side: str, base_price: float, tp_pct: float, sl_pct: float) -> dict:
    """Rebuild TP/SL around Alpaca base_price to satisfy bracket rules (+/- $0.02 cushion)."""
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

def build_oco_close(symbol: str, system: str, position_side: str, qty: int, ref_price: float,
                    tp_pct: float, sl_pct: float, tif: str = "day") -> dict:
    """OCO close for an EXISTING position; includes type='limit' and tags client_order_id with system."""
    pos_side = (position_side or "").lower()
    if pos_side not in ("long", "short"):
        raise ValueError(f"Invalid position_side for OCO: {position_side}")

    rp = float(ref_price)
    if pos_side == "long":
        side = "sell"; tp = rp * (1 + tp_pct); sl = rp * (1 - sl_pct)
    else:
        side = "buy";  tp = rp * (1 - tp_pct); sl = rp * (1 + sl_pct)

    return {
        "symbol": symbol,
        "qty": int(qty),
        "side": side,
        "type": "limit",  # required by Alpaca for OCO TP leg
        "time_in_force": tif,
        "order_class": "oco",
        "take_profit": {"limit_price": f"{tp:.2f}"},
        "stop_loss":  {"stop_price":  f"{sl:.2f}"},
        "client_order_id": f"OCO-{system}-{symbol}-{int(time.time()*1000)}",
    }

# ======= Performance helpers =======
KNOWN_SYSTEMS = {"SPY_VWAP_EMA20", "SMA10D_MACD"}

def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def _load_symbol_system_map():
    try:
        return json.loads(os.getenv("SYMBOL_SYSTEM_MAP", "{}"))
    except Exception:
        return {}

def _alpaca_get(path, params=None, timeout=10):
    return requests.get(f"{ALPACA_BASE}{path}", headers=HEADERS, params=params or {}, timeout=timeout)

def fetch_trade_activities(start_iso: str, end_iso: str):
    """Fetch fills from Alpaca activities."""
    items = []
    try:
        params = {"after": start_iso, "until": end_iso, "direction": "asc", "page_size": 100}
        r = _alpaca_get("/account/activities/trades", params=params)
        if r.status_code < 400:
            data = r.json()
            if isinstance(data, list):
                items = data
    except Exception:
        pass
    if not items:
        try:
            params = {"after": start_iso, "until": end_iso, "direction": "asc", "page_size": 100, "activity_types": "FILL"}
            r = _alpaca_get("/account/activities", params=params)
            if r.status_code < 400:
                data = r.json()
                if isinstance(data, list):
                    items = data
        except Exception:
            pass
    return items

def fetch_client_order_ids(order_ids):
    """Return {order_id: client_order_id}"""
    out = {}
    for oid in set([o for o in order_ids if o]):
        try:
            r = _alpaca_get(f"/orders/{oid}")
            if r.status_code < 400:
                out[oid] = r.json().get("client_order_id", "")
        except Exception:
            continue
    return out

def infer_system(symbol: str, client_id: str, sym_map: dict):
    if client_id:
        prefix = client_id.split("-", 1)[0]
        if prefix in KNOWN_SYSTEMS:
            return prefix
        parts = client_id.split("-", 3)
        if len(parts) >= 3 and parts[0] == "OCO" and parts[1] in KNOWN_SYSTEMS:
            return parts[1]
    return sym_map.get(symbol)

def compute_performance(trade_acts, client_ids_map, sym_map):
    """FIFO realized P&L from fills with strategy attribution."""
    fills = []
    for a in trade_acts:
        try:
            ts = a.get("transaction_time") or a.get("dt") or a.get("time")
            if not ts:
                continue
            price = float(a.get("price"))
            qty   = float(a.get("qty"))
            side  = (a.get("side") or "").lower()  # "buy" or "sell"
            symbol= a.get("symbol") or a.get("symbol_id")
            oid   = a.get("order_id")
            if not (symbol and side in ("buy","sell") and price and qty and oid):
                continue
            fills.append({"t": ts, "symbol": symbol, "side": side, "price": price, "qty": qty, "order_id": oid})
        except Exception:
            continue

    cid_map = client_ids_map or fetch_client_order_ids([f["order_id"] for f in fills])
    for f in fills:
        cid = cid_map.get(f["order_id"], "")
        sysname = infer_system(f["symbol"], cid, sym_map)
        f["system"] = sysname or "UNKNOWN"

    fills.sort(key=lambda x: x["t"])

    from collections import defaultdict, deque
    lots = defaultdict(deque)  # (system,symbol) -> deque of open lots
    trades = []

    def close_against(key, incoming_side, incoming_qty, incoming_price, tstamp):
        realized = []
        q = incoming_qty
        while q > 1e-9 and lots[key]:
            lot = lots[key][0]
            opposite = ("short" if incoming_side == "buy" else "long")
            if lot["side"] != opposite:
                break
            close_qty = min(q, lot["qty"])
            if opposite == "long":
                pnl = (incoming_price - lot["price"]) * close_qty  # sell closing long
                entry_side = "LONG"
            else:
                pnl = (lot["price"] - incoming_price) * close_qty  # buy closing short
                entry_side = "SHORT"
            realized.append({
                "system": key[0], "symbol": key[1],
                "entry_time": lot["t"], "entry_price": lot["price"],
                "exit_time": tstamp, "exit_price": incoming_price,
                "side": entry_side, "qty": close_qty, "pnl": pnl
            })
            lot["qty"] -= close_qty
            q -= close_qty
            if lot["qty"] <= 1e-9:
                lots[key].popleft()
        return realized, q

    for f in fills:
        sysname = f["system"]; symbol = f["symbol"]
        if sysname == "UNKNOWN":
            continue
        key = (sysname, symbol)
        side = f["side"]; price = f["price"]; qty = f["qty"]; t = f["t"]

        realized, rem_qty = close_against(key, side, qty, price, t)
        trades.extend(realized)

        if rem_qty > 1e-9:
            lots[key].append({
                "side": "long" if side == "buy" else "short",
                "qty": rem_qty, "price": price, "t": t
            })

    from collections import defaultdict
    by_strategy = defaultdict(lambda: {"trades": 0, "realized_pnl": 0.0, "wins": 0})
    for tr in trades:
        s = tr["system"]
        by_strategy[s]["trades"] += 1
        by_strategy[s]["realized_pnl"] += tr["pnl"]
        if tr["pnl"] > 0:
            by_strategy[s]["wins"] += 1

    for s, v in by_strategy.items():
        v["realized_pnl"] = round(v["realized_pnl"], 2)
        v["win_rate"] = round(100.0 * v["wins"]/v["trades"], 2) if v["trades"] else 0.0
        v.pop("wins", None)

    trades_sorted = sorted(trades, key=lambda x: x["exit_time"])
    eq = 0.0
    equity = []
    for tr in trades_sorted:
        eq += tr["pnl"]
        equity.append({"time": tr["exit_time"], "equity": round(eq, 2)})

    return {
        "by_strategy": by_strategy,
        "equity": equity,
        "trades": trades,
        "total_pnl": round(sum(t["pnl"] for t in trades), 2)
    }

def bucket_daily(trades):
    """Bucket realized round-trips into daily totals (UTC)."""
    from collections import defaultdict
    daily = defaultdict(lambda: {"pnl": 0.0, "by_strategy": {}})
    for t in trades:
        day = str(t["exit_time"]).split("T", 1)[0]
        daily[day]["pnl"] += t["pnl"]
        d = daily[day]["by_strategy"]
        d[t["system"]] = d.get(t["system"], 0.0) + t["pnl"]
    out = []
    for day in sorted(daily.keys()):
        out.append({
            "date": day,
            "pnl": round(daily[day]["pnl"], 2),
            "by_strategy": {k: round(v, 2) for k, v in daily[day]["by_strategy"].items()}
        })
    return out

# ====== Scanner helpers (S2 alert-less) ======
NY = ZoneInfo("America/New_York")

def _data_get(path, params=None, timeout=10):
    # Same auth headers work for data API
    return requests.get(f"{APCA_DATA_BASE_URL}{path}", headers=HEADERS, params=params or {}, timeout=timeout)

def _canon_timeframe(tf: str) -> str:
    tf = (tf or "").lower().strip()
    if tf in ("60", "60m", "1h", "1hour", "hour", "1hr"): return "1Hour"
    if tf in ("30", "30m", "0.5h"): return "30Min"
    if tf in ("120","120m","2h","2hour"): return "2Hour"
    if tf in ("d","1d","day","daily"): return "1Day"
    return "1Hour"

def fetch_bars(symbol: str, timeframe: str, limit: int = 300):
    """Return list of bars [{t,o,h,l,c,v}], newest last. Always send a start window."""
    tf = _canon_timeframe(timeframe)
    params = {"timeframe": tf, "limit": int(limit), "feed": APCA_DATA_FEED, "adjustment": "raw"}

    # Add a lookback window so Alpaca reliably returns data (paper/free feeds can be picky)
    now = datetime.now(timezone.utc)
    lookback_days = 150 if tf == "1Day" else 30
    start_dt = now - timedelta(days=lookback_days)
    params["start"] = start_dt.isoformat().replace("+00:00", "Z")

    r = _data_get(f"/stocks/{symbol}/bars", params=params)
    if r.status_code >= 400:
        raise RuntimeError(f"bars {symbol} {tf} {r.status_code}: {r.text}")
    js = r.json() or {}
    return js.get("bars", [])

def sma(series: List[float], length: int) -> List[float]:
    out: List[float] = []
    s = 0.0
    for i, x in enumerate(series):
        s += x
        if i >= length: s -= series[i-length]
        out.append(s/length if i+1 >= length else float('nan'))
    return out

def ema(series: List[float], length: int) -> List[float]:
    out: List[float] = []
    if length <= 1: return series[:]
    k = 2.0 / (length + 1.0)
    prev = None
    for x in series:
        prev = x if prev is None else (x - prev) * k + prev
        out.append(prev)
    return out

def macd_line(close: List[float], fast=12, slow=26) -> List[float]:
    ef = ema(close, fast); es = ema(close, slow)
    return [ (ef[i] - es[i]) for i in range(len(close)) ]

def macd_signal(macd: List[float], length=9) -> List[float]:
    return ema(macd, length)

def crossover(a: List[float], b: List[float]) -> bool:
    return len(a) >= 2 and len(b) >= 2 and (a[-2] <= b[-2]) and (a[-1] > b[-1])

def crossunder(a: List[float], b: List[float]) -> bool:
    return len(a) >= 2 and len(b) >= 2 and (a[-2] >= b[-2]) and (a[-1] < b[-1])

def in_regular_hours(utc_dt: datetime) -> bool:
    """9:30-16:00 America/New_York, weekdays."""
    local = utc_dt.astimezone(NY)
    if local.weekday() >= 5:  # 5=Sat,6=Sun
        return False
    hhmm = local.hour * 100 + local.minute
    return 930 <= hhmm <= 1600

def scan_s2_symbols(
    symbols: List[str],
    tf: str = os.getenv("S2_TF_DEFAULT", "60"),
    mode: str = os.getenv("S2_MODE_DEFAULT", "either"),
    use_rth: bool = os.getenv("S2_USE_RTH_DEFAULT", "true").lower() == "true",
    use_vol: bool = os.getenv("S2_USE_VOL_DEFAULT", "false").lower() == "true",
    dry_run: bool = True,
):
    """
    Return a dict with 'triggers' (what fired) and optionally self-POST to /webhook when dry_run=False.
    """
    tf_canon = _canon_timeframe(tf)
    results = {"ok": True, "timeframe": tf_canon, "mode": mode, "use_rth": use_rth, "use_vol": use_vol, "dry_run": dry_run, "checked": [], "triggers": []}

    for sym in [s.strip().upper() for s in symbols if s.strip()]:
        try:
            # --- Daily trend (SMA10)
            daily = fetch_bars(sym, "1Day", limit=15)
            d_close = [float(b["c"]) for b in daily]
            d_sma10 = sma(d_close, 10)
            if len(d_close) < 11 or len(d_sma10) < 10:
                results["checked"].append({sym: "insufficient daily bars"})
                continue
            daily_up   = d_close[-1] > d_sma10[-1]
            daily_down = d_close[-1] < d_sma10[-1]

            # --- Intraday MACD on tf_canon
            bars = fetch_bars(sym, tf_canon, limit=200)
            if len(bars) < 35:
                results["checked"].append({sym: "insufficient intraday bars"})
                continue

            last_bar = bars[-1]
            last_t = datetime.fromisoformat(str(last_bar["t"]).replace("Z","+00:00"))
            if use_rth and not in_regular_hours(last_t):
                results["checked"].append({sym: "outside RTH"})
                continue

            c = [float(b["c"]) for b in bars]
            v = [float(b.get("v", 0)) for b in bars]

            m  = macd_line(c, 12, 26)
            sg = macd_signal(m, 9)
            long_x  = crossover(m, sg)
            short_x = crossunder(m, sg)

            # Momentum fallback for "either" mode
            c_sma10 = sma(c, 10)
            mom_up  = len(c_sma10) and c[-1] > c_sma10[-1]
            mom_dn  = len(c_sma10) and c[-1] < c_sma10[-1]

            # Volume filter on TF
            vol_ok = True
            if use_vol:
                v_sma20 = sma(v, 20)
                vol_ok = len(v_sma20) and (v[-1] > v_sma20[-1])

            # Decide triggers
            strict_long  = daily_up   and long_x  and vol_ok
            strict_short = daily_down and short_x and vol_ok
            loose_long   = (daily_up   and vol_ok) and (long_x  or mom_up)
            loose_short  = (daily_down and vol_ok) and (short_x or mom_dn)

            go_long  = strict_long  if mode.lower()=="strict" else loose_long
            go_short = strict_short if mode.lower()=="strict" else loose_short

            fired = []
            if go_long:
                fired.append({"system":"SMA10D_MACD","side":"LONG","ticker":sym,"price":c[-1],"time":_iso(datetime.now(timezone.utc))})
            if go_short:
                fired.append({"system":"SMA10D_MACD","side":"SHORT","ticker":sym,"price":c[-1],"time":_iso(datetime.now(timezone.utc))})

            results["checked"].append({sym: {"daily_up": daily_up, "long_x": long_x, "short_x": short_x, "vol_ok": bool(vol_ok)}})

            for payload in fired:
                results["triggers"].append(payload)
                if not dry_run:
                    base = SELF_BASE_URL.rstrip("/") if SELF_BASE_URL else f"http://127.0.0.1:{int(os.getenv('PORT','8080'))}"
                    try:
                        pr = requests.post(f"{base}/webhook", headers={"Content-Type":"application/json"}, data=json.dumps(payload), timeout=10)
                        if pr.status_code >= 400:
                            app.logger.error(f"scan_s2 post error {sym}: {pr.status_code} {pr.text}")
                    except Exception as e:
                        app.logger.error(f"scan_s2 post exception {sym}: {e}")

        except Exception as e:
            app.logger.error(f"scan_s2 error {sym}: {e}")
            results["checked"].append({sym: f"error: {e}"})
            continue

    return results
    
def is_market_open_now() -> bool:
    try:
        r = requests.get(f"{ALPACA_BASE}/clock", headers=HEADERS, timeout=5)
        if r.status_code < 400:
            js = r.json() or {}
            return bool(js.get("is_open", False))
    except Exception as e:
        app.logger.error(f"clock error: {e}")
    return False
    

# ===== Routes =====
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "equities-webhook", "time": int(time.time())}), 200

@app.post("/webhook")
def webhook():
    """
    Expected JSON:
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

    # If going to place a PLAIN entry, cancel all open orders first (prevents wash-trade)
    if not use_bracket and CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
        cancel_open_orders_for_symbol(symbol)

    # Build payload
    try:
        if use_bracket:
            payload = build_entry_order_payload(symbol, side, float(price), system)
            if payload.get("type", "").lower() == "limit" and "limit_price" not in payload:
                payload["limit_price"] = f"{float(price):.2f}"
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
            body = o.json() if str(ctype).startswith("application/json") else {"message": o.text}
            app.logger.error(f"ALPACA_POST_ERROR status=422 body={body} sent={payload}")

            err_obj = body if isinstance(body, dict) else {}
            alpaca_err = err_obj.get("alpaca_error", err_obj)
            msg = (str(alpaca_err.get("message")) or "").lower()
            base_price = alpaca_err.get("base_price")

            retry_reasons = ("take_profit.limit_price must be", "stop_loss.stop_price must be")
            if base_price and any(r in msg for r in retry_reasons):
                cfg = load_strategy_config(system)
                adj_payload = adjust_bracket_to_base_price(payload, side, float(base_price), cfg.tp_pct, cfg.sl_pct)
                app.logger.info(f"RETRY with base_price={base_price}: {adj_payload}")

                o2 = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(adj_payload), timeout=10)
                if o2.status_code >= 400:
                    ctype2 = o2.headers.get("content-type", "")
                    body2 = o2.json() if str(ctype2).startswith("application/json") else o2.text
                    app.logger.error(f"ALPACA_POST_ERROR (retry) status={o2.status_code} body={body2} sent={adj_payload}")
                    return jsonify({"ok": False, "alpaca_error": body2}), 422
                return jsonify({"ok": True, "order": o2.json(), "note": "placed on retry using Alpaca base_price"}), 200

            return jsonify({"ok": False, "alpaca_error": body}), 422

        if o.status_code >= 400:
            ctype = o.headers.get("content-type", "")
            body = o.json() if str(ctype).startswith("application/json") else o.text
            app.logger.error(f"ALPACA_POST_ERROR status={o.status_code} body={body} sent={payload}")
            return jsonify({"ok": False, "alpaca_error": body}), 422

        order_json = o.json()

        # If PLAIN and OCO enabled, attach a single OCO for the FULL current position
        if not use_bracket and ATTACH_OCO_ON_PLAIN:
            try:
                cfg = load_strategy_config(system, symbol)
                # Refresh position AFTER entry so qty/avg_entry are up-to-date
                pos_after = get_symbol_position(symbol) or pos_before
                pos_side = (pos_after or {}).get("side") or ((pos_before or {}).get("side"))
                qty_total = int(abs(float((pos_after or {}).get("qty") or (pos_before or {}).get("qty") or 0)))
                ref_price = float((pos_after or {}).get("avg_entry_price") or price)

                if qty_total > 0 and pos_side:
                    oco_payload = build_oco_close(
                        symbol=symbol,
                        system=system,  # tag strategy
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
                        body3 = oco_resp.json() if str(ctype3).startswith("application/json") else oco_resp.text
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

        return jsonify({"ok": True, "order": order_json}), 200

    except Exception as e:
        app.logger.error(f"ORDER ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/performance")
def performance():
    """Realized P&L from Alpaca fills (FIFO)."""
    days_s = request.args.get("days", "7")
    include_trades = request.args.get("include_trades", "0") == "1"
    try:
        days = max(1, int(days_s))
    except Exception:
        days = 7

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    start_iso = _iso(start_dt)
    end_iso   = _iso(end_dt)

    acts = fetch_trade_activities(start_iso, end_iso)
    order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
    cid_map = fetch_client_order_ids(order_ids)
    sym_map = _load_symbol_system_map()
    perf = compute_performance(acts, cid_map, sym_map)

    out = {
        "ok": True,
        "days": days,
        "total_realized_pnl": perf["total_pnl"],
        "by_strategy": perf["by_strategy"],
        "equity": perf["equity"],
        "note": "Realized P&L from Alpaca fills (FIFO). Set SYMBOL_SYSTEM_MAP for fallback attribution.",
    }
    if include_trades:
        out["trades"] = perf["trades"]
    return jsonify(out), 200

@app.get("/performance/daily")
def performance_daily():
    """Daily buckets of realized P&L (UTC), with per-strategy splits."""
    days_s = request.args.get("days", "30")
    try:
        days = max(1, int(days_s))
    except Exception:
        days = 30

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    start_iso = _iso(start_dt); end_iso = _iso(end_dt)

    acts = fetch_trade_activities(start_iso, end_iso)
    order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
    cid_map = fetch_client_order_ids(order_ids)
    sym_map = _load_symbol_system_map()
    perf = compute_performance(acts, cid_map, sym_map)
    daily = bucket_daily(perf["trades"])

    return jsonify({"ok": True, "days": days, "daily": daily}), 200

@app.get("/positions")
def positions():
    """Proxy Alpaca positions (JSON)."""
    try:
        r = requests.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=6)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/open")
def orders_open():
    """Proxy Alpaca open orders (JSON)."""
    try:
        r = requests.get(f"{ALPACA_BASE}/orders?status=open&limit=200&nested=false", headers=HEADERS, timeout=6)
        return (r.text, r.status_code, {"Content-Type": r.headers.get("content-type","application/json")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/config")
def config():
    """Effective runtime config snapshot (JSON)."""
    def cfg_for(system):
        c = load_strategy_config(system)
        return {"qty": c.qty, "order_type": c.order_type, "tif": c.tif, "tp_pct": c.tp_pct, "sl_pct": c.sl_pct}
    out = {
        "alpaca_base": ALPACA_BASE,
        "safety": {
            "cancel_open_orders_before_plain": CANCEL_OPEN_ORDERS_BEFORE_PLAIN,
            "attach_oco_on_plain": ATTACH_OCO_ON_PLAIN,
            "oco_tif": OCO_TIF,
        },
        "caps": {
            "MAX_OPEN_POSITIONS": MAX_OPEN_POSITIONS,
            "MAX_POSITIONS_PER_SYMBOL": MAX_POSITIONS_PER_SYMBOL,
            "MAX_POSITIONS_PER_STRATEGY": MAX_POSITIONS_PER_STRATEGY,
        },
        "systems": {
            "SPY_VWAP_EMA20": cfg_for("SPY_VWAP_EMA20"),
            "SMA10D_MACD": cfg_for("SMA10D_MACD"),
        }
    }
    return jsonify(out), 200

@app.get("/dashboard")
def dashboard():
    """Dark-mode HTML dashboard with Monthly P&L Calendar, Summary, Charts, Trades."""
    try:
        html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Trading Dashboard — Dark</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {
      --bg:#0b0f14; --panel:#0f172a; --border:#1f2937; --ink:#e2e8f0; --muted:#94a3b8; --grid:#334155;
      --pos-h:142; --neg-h:0;
    }
    html, body { background: var(--bg); color: var(--ink); }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin: 0 0 16px 0; }
    h2 { margin: 0 0 12px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.35); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--border); }
    th { color: var(--muted); font-weight: 600; }
    .muted { color: var(--muted); font-size: 12px; }

    .cal-wrap { display: grid; gap: 12px; }
    .cal-head { display: flex; align-items: center; gap: 8px; justify-content: space-between; }
    .cal-title { font-weight: 700; font-size: 18px; }
    .cal-ctl button {
      padding: 6px 10px; border-radius: 8px; border: 1px solid var(--border);
      background: #0b1220; color: var(--ink); cursor: pointer;
    }
    .cal-grid { display: grid; grid-template-columns: repeat(7, 1fr); gap: 8px; }
    .dow { text-align:center; font-size:12px; color: var(--muted); margin-bottom: -6px; }
    .day {
      min-height: 82px; border: 1px solid var(--border); border-radius: 10px; padding: 6px 8px; position: relative;
      background: #0b1220; display:flex; flex-direction:column; justify-content:flex-end;
    }
    .day .date { position:absolute; top:6px; right:8px; font-size:12px; color: var(--muted); }
    .day .pl { font-weight:700; font-size:14px; color: var(--ink); }
    .day .trades { font-size:12px; color: var(--muted); }
    .day.today { outline: 2px dashed #8b5cf6; outline-offset: 2px; }
    .legend { display:flex; gap:10px; align-items:center; font-size:12px; color: var(--muted); }
    .swatch { width: 48px; height: 10px; border-radius: 6px; background: linear-gradient(90deg, hsl(var(--neg-h),80%,55%), #273041, hsl(var(--pos-h),70%,45%)); border:1px solid var(--border); }
    @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <h1>Trading Dashboard</h1>
  <div class="muted">Origin: <code id="origin"></code></div>
  <br/>

  <div class="card cal-wrap">
    <div class="cal-head">
      <div class="cal-title">Monthly P&amp;L Calendar: <span id="calMonth"></span></div>
      <div class="cal-ctl">
        <button id="prevBtn" title="Previous month">&#x276E;</button>
        <button id="todayBtn">Today</button>
        <button id="nextBtn" title="Next month">&#x276F;</button>
      </div>
    </div>
    <div class="legend">
      <span>Loss</span><span class="swatch"></span><span>Gain</span>
      <span class="muted">— intensity scales with |P&amp;L|</span>
    </div>
    <div class="cal-grid" id="dowRow"></div>
    <div class="cal-grid" id="calGrid"></div>
    <div class="muted" id="calTotals"></div>
  </div>

  <br/>

  <div class="grid">
    <div class="card">
      <h2>Summary (last 7 days)</h2>
      <div id="summary"></div>
      <h3>By Strategy</h3>
      <table id="byStrategy">
        <thead><tr><th>Strategy</th><th>Trades</th><th>Win %</th><th>Realized P&amp;L ($)</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card">
      <h2>Daily P&amp;L (last 30 days)</h2>
      <canvas id="dailyChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Equity Curve (realized)</h2>
      <canvas id="equityChart" height="200"></canvas>
    </div>

    <div class="card">
      <h2>Recent Realized Trades</h2>
      <table id="trades">
        <thead><tr><th>Exit Time (UTC)</th><th>System</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&amp;L</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script>
    const origin = window.location.origin;
    document.getElementById('origin').textContent = origin;

    async function getJSON(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    }
    function fmt(n) { return (n>=0?'+':'') + n.toFixed(2); }

    // Chart.js defaults for dark background
    Chart.defaults.color = '#e2e8f0';
    Chart.defaults.borderColor = '#334155';

    // Calendar helpers
    const DOW = ['Su','Mo','Tu','We','Th','Fr','Sa'];
    function ymd(d) { return d.toISOString().slice(0,10); }
    function endOfMonth(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth()+1, 0)); }
    function startOfMonth(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)); }
    function colorForPL(v, maxAbs) {
      if (!v) return null;
      const a = Math.min(1, Math.abs(v)/Math.max(1, maxAbs));
      const alpha = 0.15 + a*0.45;
      const hue = v >= 0 ? 142 : 0;
      return `hsla(${hue}, 70%, 45%, ${alpha})`;
    }

    (function() {
      const dowRow = document.getElementById('dowRow');
      DOW.forEach(d => {
        const el = document.createElement('div');
        el.className = 'dow';
        el.textContent = d;
        dowRow.appendChild(el);
      });
    })();

    (async () => {
      const perfAll = await getJSON(origin + '/performance?days=120&include_trades=1');
      const daily   = await getJSON(origin + '/performance/daily?days=120');

      const dayPNL = new Map();
      daily.daily.forEach(d => dayPNL.set(d.date, d.pnl));
      const dayTrades = new Map();
      (perfAll.trades || []).forEach(tr => {
        const day = String(tr.exit_time).slice(0,10);
        dayTrades.set(day, (dayTrades.get(day) || 0) + 1);
      });

      const maxAbs = Math.max(1, ...Array.from(dayPNL.values()).map(v => Math.abs(v)));

      let current = new Date(); // today UTC
      function setMonthLabel(dt) {
        const m = dt.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
        const y = dt.getUTCFullYear();
        document.getElementById('calMonth').textContent = m + ' ' + y;
      }

      function renderCalendar(dt) {
        const grid = document.getElementById('calGrid');
        grid.innerHTML = '';
        const start = startOfMonth(dt);
        const end = endOfMonth(dt);
        const firstDow = start.getUTCDay();
        const daysInMonth = end.getUTCDate();

        for (let i=0; i<firstDow; i++) grid.appendChild(document.createElement('div'));

        let totalMonth = 0; let totalTrades = 0;

        for (let d=1; d<=daysInMonth; d++) {
          const cDate = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), d));
          const key = ymd(cDate);
          const pnl = dayPNL.get(key) || 0;
          const trades = dayTrades.get(key) || 0;
          const cell = document.createElement('div');
          cell.className = 'day';
          const bg = colorForPL(pnl, maxAbs);
          if (bg) cell.style.background = bg;
          if (ymd(cDate) === ymd(new Date())) cell.classList.add('today');

          cell.innerHTML = `
            <div class="date">${d}</div>
            <div class="pl">$${(pnl||0).toFixed(2)}</div>
            <div class="trades">${trades} trades</div>
          `;
          grid.appendChild(cell);

          totalMonth += pnl; totalTrades += trades;
        }

        const footer = document.getElementById('calTotals');
        footer.textContent = 'Month total: $' + totalMonth.toFixed(2) + ' across ' + totalTrades + ' trades';
        setMonthLabel(dt);
      }

      renderCalendar(current);
      document.getElementById('prevBtn').onclick = () => { current = new Date(Date.UTC(current.getUTCFullYear(), current.getUTCMonth()-1, 1)); renderCalendar(current); };
      document.getElementById('nextBtn').onclick = () => { current = new Date(Date.UTC(current.getUTCFullYear(), current.getUTCMonth()+1, 1)); renderCalendar(current); };
      document.getElementById('todayBtn').onclick = () => { current = new Date(); renderCalendar(current); };

      // Summary (7d)
      const perf7 = await getJSON(origin + '/performance?days=7&include_trades=1');
      document.getElementById('summary').innerHTML = '<div>Total realized P&amp;L: <b>$' + perf7.total_realized_pnl.toFixed(2) + '</b></div>';

      // By strategy
      const tbody = document.querySelector('#byStrategy tbody');
      tbody.innerHTML = '';
      Object.entries(perf7.by_strategy).forEach(([k,v]) => {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>'+k+'</td><td>'+v.trades+'</td><td>'+v.win_rate+'%</td><td>'+v.realized_pnl.toFixed(2)+'</td>';
        tbody.appendChild(tr);
      });

      // Equity chart
      const eq = perf7.equity || [];
      const eqLabels = eq.map(x => x.time);
      const eqData = eq.map(x => x.equity);
      new Chart(document.getElementById('equityChart').getContext('2d'), {
        type: 'line',
        data: { labels: eqLabels, datasets: [{ label: 'Equity ($)', data: eqData }] },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#cbd5e1' }, grid: { color: '#334155' } },
            y: { ticks: { color: '#cbd5e1' }, grid: { color: '#334155' } }
          }
        }
      });

      // Daily (30d)
      const d30 = await getJSON(origin + '/performance/daily?days=30');
      const dLabels = d30.daily.map(x => x.date);
      const dData = d30.daily.map(x => x.pnl);
      new Chart(document.getElementById('dailyChart').getContext('2d'), {
        type: 'bar',
        data: { labels: dLabels, datasets: [{ label: 'Daily P&L ($)', data: dData }] },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#cbd5e1' }, grid: { color: '#334155' } },
            y: { ticks: { color: '#cbd5e1' }, grid: { color: '#334155' } }
          }
        }
      });

      // Trades table (last 25)
      const tBody = document.querySelector('#trades tbody');
      (perf7.trades || []).slice(-25).forEach(t => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${t.exit_time}</td>
          <td>${t.system}</td>
          <td>${t.symbol}</td>
          <td>${t.side}</td>
          <td>$${Number(t.entry_price).toFixed(2)}</td>
          <td>$${Number(t.exit_price).toFixed(2)}</td>
          <td>${fmt(Number(t.pnl))}</td>`;
        tBody.appendChild(tr);
      });
    })().catch(err => {
      document.body.insertAdjacentHTML('beforeend', '<pre style="color:#fca5a5;">Dashboard error: ' + err.message + '</pre>');
    });
  </script>
</body>
</html>
        """
        resp = make_response(html, 200)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    except Exception as e:
        return f"<pre>dashboard render error: {e}</pre>", 500

@app.get("/scan/s2")
def scan_s2_route():
    """
    Server-side scanner for Strategy 2 (MACD x SMA10 with daily trend filter).
    Query params:
      symbols=CSV            (default: env S2_WHITELIST)
      tf=60|30               (default: env S2_TF_DEFAULT)
      mode=strict|either     (default: env S2_MODE_DEFAULT)
      rth=true|false         (default: env S2_USE_RTH_DEFAULT)
      vol=true|false         (default: env S2_USE_VOL_DEFAULT)
      dry=1|0                (default: 1 = dry-run only)
    """
    default_syms = os.getenv("S2_WHITELIST", "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL")
    symbols = request.args.get("symbols", default_syms).split(",")
    tf      = request.args.get("tf", os.getenv("S2_TF_DEFAULT", "60"))
    mode    = request.args.get("mode", os.getenv("S2_MODE_DEFAULT", "either"))
    rth     = (request.args.get("rth", os.getenv("S2_USE_RTH_DEFAULT", "true")).lower() == "true")
    vol     = (request.args.get("vol", os.getenv("S2_USE_VOL_DEFAULT", "false")).lower() == "true")
    dry     = request.args.get("dry", "1") == "1"

    res = scan_s2_symbols(symbols, tf=tf, mode=mode, use_rth=rth, use_vol=vol, dry_run=dry)
    return jsonify(res), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
