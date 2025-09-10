# app.py
import os
import json
import time
import uuid
import math
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict, deque

import requests
from flask import Flask, request, jsonify, make_response

# ========= App & Logging =========
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("equities-webhook")

# ========= Environment / Config =========
APCA_API_KEY_ID     = os.getenv("APCA_API_KEY_ID", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "")
ALPACA_BASE         = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
DATA_BASE           = os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets/v2")
DATA_FEED           = os.getenv("APCA_DATA_FEED", "iex")  # iex (paper) or sip (live)

# Safety caps
MAX_OPEN_POSITIONS          = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
MAX_POSITIONS_PER_SYMBOL    = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", "1"))
MAX_POSITIONS_PER_STRATEGY  = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", "3"))

# Order behavior
CANCEL_OPEN_ORDERS_BEFORE_PLAIN = os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN", "true").lower() == "true"
ATTACH_OCO_ON_PLAIN             = os.getenv("ATTACH_OCO_ON_PLAIN", "true").lower() == "true"
OCO_TIF                         = os.getenv("OCO_TIF", "gtc")  # gtc recommended for exits
OCO_REQUIRE_POSITION            = os.getenv("OCO_REQUIRE_POSITION", "true").lower() == "true"

# Scanner defaults
S2_WHITELIST      = os.getenv("S2_WHITELIST", "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL")
S2_TF_DEFAULT     = os.getenv("S2_TF_DEFAULT", "60")
S2_MODE_DEFAULT   = os.getenv("S2_MODE_DEFAULT", "either")
S2_USE_RTH_DEFAULT= os.getenv("S2_USE_RTH_DEFAULT", "true")
S2_USE_VOL_DEFAULT= os.getenv("S2_USE_VOL_DEFAULT", "false")

HEADERS = {
    "APCA-API-KEY-ID": APCA_API_KEY_ID,
    "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
    "Content-Type": "application/json",
}

# ========= Small Utils =========
def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _canon_timeframe(tf: str) -> str:
    return {"30":"30Min", "60":"1Hour", "120":"2Hour", "day":"1Day"}.get(tf, "1Hour")

def _cid(prefix: str, system: str, symbol: str) -> str:
    return f"{prefix}-{system}-{symbol}-{uuid.uuid4().hex[:8]}"

def _side_is_long(side: str) -> bool:
    return side.lower() == "buy"
    
# ---- helpers (put near top, once) ----
def _scrub_nans(x):
    import math
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return 0.0
    if isinstance(x, list):
        return [_scrub_nans(v) for v in x]
    if isinstance(x, dict):
        return {k: _scrub_nans(v) for k, v in x.items()}
    return x

# ========= Market & Account =========
def is_market_open_now() -> bool:
    try:
        r = requests.get(f"{ALPACA_BASE}/clock", headers=HEADERS, timeout=6)
        r.raise_for_status()
        return bool(r.json().get("is_open"))
    except Exception as e:
        log.warning(f"clock check failed: {e}")
        return False

def bars(symbol: str, timeframe: str, limit: int = 200, start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str,Any]]:
    params = {"timeframe": timeframe, "limit": limit, "feed": DATA_FEED}
    if start: params["start"] = start
    if end:   params["end"] = end
    r = requests.get(f"{DATA_BASE}/stocks/{symbol}/bars", headers=HEADERS, params=params, timeout=8)
    r.raise_for_status()
    return r.json().get("bars", [])

def last_close(symbol: str) -> Optional[float]:
    try:
        b = bars(symbol, timeframe="1Min", limit=1)
        return float(b[-1]["c"]) if b else None
    except Exception:
        return None

def get_position(symbol: str) -> Optional[Dict[str,Any]]:
    try:
        r = requests.get(f"{ALPACA_BASE}/positions/{symbol}", headers=HEADERS, timeout=6)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def list_positions() -> List[Dict[str,Any]]:
    try:
        r = requests.get(f"{ALPACA_BASE}/positions", headers=HEADERS, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def list_open_orders(symbol: Optional[str] = None) -> List[Dict[str,Any]]:
    try:
        q = "status=open&nested=false&limit=200"
        r = requests.get(f"{ALPACA_BASE}/orders?{q}", headers=HEADERS, timeout=8)
        r.raise_for_status()
        orders = r.json()
        if symbol:
            orders = [o for o in orders if o.get("symbol")==symbol]
        return orders
    except Exception:
        return []

def cancel_open_orders(symbol: Optional[str] = None):
    orders = list_open_orders(symbol)
    for o in orders:
        oid = o.get("id")
        if not oid: continue
        try:
            requests.delete(f"{ALPACA_BASE}/orders/{oid}", headers=HEADERS, timeout=6)
        except Exception:
            pass

def can_open_new_position(symbol: str, system: str) -> Tuple[bool, str]:
    positions = list_positions()
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False, f"MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}"
    sym_count = sum(1 for p in positions if p.get("symbol")==symbol)
    if sym_count >= MAX_POSITIONS_PER_SYMBOL:
        return False, f"MAX_POSITIONS_PER_SYMBOL={MAX_POSITIONS_PER_SYMBOL}"
    # Strategy cap (best enforced with tags; here we lightly skip)
    return True, "ok"

# ========= Orders & OCO =========
def post_order(payload: Dict[str,Any]) -> Dict[str,Any]:
    r = requests.post(f"{ALPACA_BASE}/orders", headers=HEADERS, data=json.dumps(payload), timeout=10)
    if not r.ok:
        log.error(f"POST /orders {r.status_code} {r.text} payload={payload}")
    r.raise_for_status()
    return r.json()

def attach_oco_exit_if_position(symbol: str, qty_hint: Optional[int], take_profit: float, stop_loss: float, tif: str, system: str) -> Optional[Dict[str,Any]]:
    pos = get_position(symbol)
    if not pos:
        log.info(f"OCO skip: no position for {symbol}")
        return None
    qty = qty_hint or int(float(pos.get("qty","0")))
    if qty <= 0:
        log.info(f"OCO skip: qty<=0 for {symbol}")
        return None

    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": "sell",                 # exit a long
        "type": "limit",
        "time_in_force": tif if tif in ("day","gtc") else "gtc",
        "order_class": "oco",
        "take_profit": {"limit_price": f"{take_profit:.2f}"},
        "stop_loss":  {"stop_price": f"{stop_loss:.2f}"},
        "client_order_id": _cid("OCO", system, symbol),
    }
    try:
        return post_order(payload)
    except requests.HTTPError as e:
        log.error(f"OCO_POST_ERROR status={getattr(e.response,'status_code','?')} body={getattr(e.response,'text','')} sent={payload}")
        return None

# ========= Strategy Config =========
@dataclass
class StrategyConfig:
    qty: int = 1
    order_type: str = "bracket"  # 'bracket' for first entries, 'plain' for adds
    tif: str = "day"
    tp_pct: float = 0.010  # 1.0%
    sl_pct: float = 0.007  # 0.7%

def _envf(system: str, key: str, default: Optional[str]) -> str:
    return os.getenv(f"{system}_{key}", default if default is not None else "")

def load_strategy_config(system: str) -> StrategyConfig:
    return StrategyConfig(
        qty = int(_envf(system, "QTY", "1") or "1"),
        order_type = _envf(system, "ORDER_TYPE", "bracket") or "bracket",
        tif = _envf(system, "TIF", "day") or "day",
        tp_pct = float(_envf(system, "TP_PCT", "0.010") or "0.010"),
        sl_pct = float(_envf(system, "SL_PCT", "0.007") or "0.007"),
    )

# ========= Performance (fills → FIFO realized) =========
def fetch_trade_activities(start_iso: str, end_iso: str) -> List[Dict[str,Any]]:
    """
    Alpaca Activities supports single-date queries best.
    We iterate per-UTC-day from start..end and merge FILL activities.
    """
    acts: List[Dict[str,Any]] = []
    start = datetime.fromisoformat(start_iso.replace("Z","+00:00"))
    end   = datetime.fromisoformat(end_iso.replace("Z","+00:00"))
    day = start
    while day.date() <= end.date():
        date_str = day.strftime("%Y-%m-%d")
        try:
            url = f"{ALPACA_BASE}/account/activities"
            params = {"activity_types":"FILL", "date": date_str}
            r = requests.get(url, headers=HEADERS, params=params, timeout=8)
            if r.ok:
                data = r.json()
                if isinstance(data, dict) and "activities" in data:
                    data = data["activities"]
                if isinstance(data, list):
                    acts.extend(data)
        except Exception as e:
            log.warning(f"activities fetch failed for {date_str}: {e}")
        day += timedelta(days=1)
    # Filter by timestamp window just in case
    out = []
    for a in acts:
        ts = a.get("transaction_time") or a.get("date")
        if not ts: continue
        if ts < start_iso or ts > end_iso: 
            continue
        out.append(a)
    return out

def fetch_client_order_ids(order_ids: List[str]) -> Dict[str,str]:
    out: Dict[str,str] = {}
    for oid in order_ids:
        try:
            r = requests.get(f"{ALPACA_BASE}/orders/{oid}", headers=HEADERS, timeout=6)
            if r.ok:
                j = r.json()
                cid = j.get("client_order_id")
                if cid: out[oid] = cid
        except Exception:
            pass
    return out

def _load_symbol_system_map() -> Dict[str,str]:
    """
    Optional fallback mapping for strategy attribution when client IDs are missing.
    Supply env SYMBOL_SYSTEM_MAP as JSON: {"SPY":"SPY_VWAP_EMA20", "TSLA":"SMA10D_MACD"}
    """
    try:
        raw = os.getenv("SYMBOL_SYSTEM_MAP", "{}")
        m = json.loads(raw)
        return {k.upper(): v for k,v in m.items()}
    except Exception:
        return {}

def infer_system_from_cid(cid: str, sym_map: Dict[str,str], symbol: str) -> str:
    # Expect pattern like BRK-<SYSTEM>-<SYMBOL>-xxxx OR PLN-...
    parts = (cid or "").split("-")
    if len(parts) >= 3:
        return parts[1]
    return sym_map.get(symbol.upper(), "UNKNOWN")

def compute_performance(acts: List[Dict[str,Any]], cid_map: Dict[str,str], sym_map: Dict[str,str]) -> Dict[str,Any]:
    """
    FIFO realized P&L per symbol; aggregate per strategy; build equity series (realized).
    """
    # Normalize fills
    fills = []
    for a in acts:
        try:
            symbol = a.get("symbol") or a.get("symbol_id")
            side   = (a.get("side") or "").lower()  # "buy" or "sell"
            qty    = float(a.get("qty") or a.get("quantity") or 0)
            price  = float(a.get("price") or a.get("price_per_share") or 0)
            ts     = a.get("transaction_time") or a.get("date")
            oid    = a.get("order_id")
            cid    = cid_map.get(oid or "", "")
            fills.append({"symbol":symbol, "side":side, "qty":qty, "price":price, "time":ts, "order_id":oid, "client_order_id":cid})
        except Exception:
            continue

    buys:  Dict[str,deque] = defaultdict(deque)
    sells: Dict[str,List[Dict[str,Any]]] = defaultdict(list)

    for f in sorted(fills, key=lambda x: x["time"]):
        if f["side"] == "buy":
            buys[f["symbol"]].append({"qty":f["qty"], "price":f["price"], "time":f["time"], "cid":f["client_order_id"]})
        elif f["side"] == "sell":
            sells[f["symbol"]].append(f)

    trades: List[Dict[str,Any]] = []
    total_pnl = 0.0
    equity_series: List[Dict[str,Any]] = []
    equity_running = 0.0

    for symbol, s_list in sells.items():
        for sfill in s_list:
            qty_to_match = sfill["qty"]
            legs = []
            pnl_for_trade = 0.0
            while qty_to_match > 0 and buys[symbol]:
                b = buys[symbol][0]
                mqty = min(qty_to_match, b["qty"])
                leg_pnl = (sfill["price"] - b["price"]) * mqty
                pnl_for_trade += leg_pnl
                legs.append({"buy_px":b["price"], "sell_px":sfill["price"], "qty":mqty, "pnl":leg_pnl})
                b["qty"] -= mqty
                qty_to_match -= mqty
                if b["qty"] <= 1e-9:
                    buys[symbol].popleft()
            system = infer_system_from_cid(sfill.get("client_order_id",""), sym_map, symbol)
            trade = {
                "symbol": symbol,
                "system": system,
                "side": "long_exit",
                "entry_price": legs[0]["buy_px"] if legs else None,
                "exit_price": sfill["price"],
                "qty": sum(l["qty"] for l in legs) if legs else 0,
                "pnl": pnl_for_trade,
                "legs": legs,
                "exit_time": sfill["time"],
                "client_order_id": sfill.get("client_order_id"),
            }
            trades.append(trade)
            total_pnl += pnl_for_trade
            equity_running += pnl_for_trade
            equity_series.append({"time": sfill["time"], "equity": equity_running})

    # Aggregate by strategy with trades/winrate
    agg: Dict[str, Dict[str,Any]] = defaultdict(lambda: {"realized_pnl":0.0,"trades":0,"wins":0})
    for t in trades:
        s = t["system"]
        agg[s]["realized_pnl"] += t["pnl"]
        agg[s]["trades"] += 1
        if t["pnl"] > 0: agg[s]["wins"] += 1
    # format win_rate
    by_strategy = {}
    for k,v in agg.items():
        n = v["trades"]
        win_rate = round((v["wins"]/n*100) if n else 0.0, 1)
        by_strategy[k] = {"realized_pnl": v["realized_pnl"], "trades": n, "win_rate": win_rate}

    return {
        "total_pnl": total_pnl,
        "trades": trades,
        "by_strategy": by_strategy,
        "equity": equity_series
    }

def bucket_daily(trades: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    days: Dict[str, Dict[str,Any]] = {}
    for t in trades:
        day = str(t["exit_time"])[:10]
        d = days.setdefault(day, {"date": day, "pnl": 0.0, "by_strategy": defaultdict(float)})
        d["pnl"] += t["pnl"]
        d["by_strategy"][t["system"]] += t["pnl"]
    # normalize
    out = []
    for day, row in sorted(days.items()):
        out.append({"date": day, "pnl": row["pnl"], "by_strategy": dict(row["by_strategy"])})
    return out

# ========= Indicators =========
def sma(values: List[float], length: int) -> List[Optional[float]]:
    out = []
    q: List[float] = []
    s = 0.0
    for v in values:
        q.append(v); s += v
        if len(q) > length: s -= q.pop(0)
        out.append(s/length if len(q)==length else None)
    return out

def ema(values: List[float], length: int) -> List[float]:
    k = 2/(length+1.0)
    out = []
    e = None
    for v in values:
        e = v if e is None else v*k + e*(1-k)
        out.append(e)
    return out

def macd(values: List[float], fast=12, slow=26, signal=9) -> Tuple[List[float], List[float], List[float]]:
    ef = ema(values, fast)
    es = ema(values, slow)
    macd_line = [ (f - s) for f,s in zip(ef, es) ]
    signal_line = ema(macd_line, signal)
    hist = [m-s for m,s in zip(macd_line, signal_line)]
    return macd_line, signal_line, hist

# ========= Signal Processing (S1 webhooks + S2 triggers) =========
def _compute_exits_from_price(entry_px: float, side: str, tp_pct: float, sl_pct: float) -> Tuple[float,float]:
    if _side_is_long(side):
        tp = round(entry_px*(1+tp_pct), 2)
        sl = round(entry_px*(1-sl_pct), 2)
    else:
        tp = round(entry_px*(1-tp_pct), 2)
        sl = round(entry_px*(1+sl_pct), 2)
    return tp, sl

def _ref_price(symbol: str, given: Optional[float]) -> float:
    if given and given > 0: return float(given)
    lc = last_close(symbol)
    return float(lc) if lc else 0.0

def _first_entry_should_be_bracket(has_pos: bool, order_type: str) -> bool:
    # If no pos, prefer bracket; if configured 'bracket', use bracket; else allow plain + later OCO
    return (not has_pos) and (order_type.lower()=="bracket")

def process_signal(data: Dict[str,Any]) -> Tuple[int, Dict[str,Any]]:
    """
    Accepts: {"system","side","ticker","price","time"} (TradingView or scanner)
    Places either BRACKET (entry+exits) or PLAIN (entry) and, if configured, attaches OCO after fill.
    """
    system = (data.get("system") or "UNKNOWN").strip()
    symbol = (data.get("ticker") or "").upper().strip()
    side   = (data.get("side") or "buy").lower().strip()
    price  = _ref_price(symbol, data.get("price"))

    if not symbol or side not in ("buy","sell"):
        return 400, {"ok": False, "error": "invalid payload"}

    ok, reason = can_open_new_position(symbol, system)
    if not ok:
        return 200, {"ok": False, "blocked": reason}

    cfg = load_strategy_config(system)
    has_pos = get_position(symbol) is not None
    tif = cfg.tif

    # Optional: cancel open symbol orders before a plain add (wash-trade safety)
    if (cfg.order_type.lower()=="plain") and CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
        cancel_open_orders(symbol)

    tp, sl = _compute_exits_from_price(price, side, cfg.tp_pct, cfg.sl_pct)

    try:
        if _first_entry_should_be_bracket(has_pos, cfg.order_type):
            payload = {
                "symbol": symbol, "qty": str(cfg.qty), "side": side,
                "type": "market", "time_in_force": tif,
                "order_class": "bracket",
                "take_profit": {"limit_price": f"{tp:.2f}"},
                "stop_loss":   {"stop_price":  f"{sl:.2f}"},
                "client_order_id": _cid("BRK", system, symbol),
            }
            order = post_order(payload)
            out = {"ok": True, "placed": "bracket", "order": order}
        else:
            # Plain entry (initial or add)
            payload = {
                "symbol": symbol, "qty": str(cfg.qty), "side": side,
                "type": "market", "time_in_force": tif,
                "client_order_id": _cid("PLN", system, symbol),
            }
            order = post_order(payload)
            out = {"ok": True, "placed": "plain", "order": order}

            # Attach OCO only after position exists (exit-only rule)
            if ATTACH_OCO_ON_PLAIN:
                # Fast poll (<= 8s)
                filled = True
                if OCO_REQUIRE_POSITION:
                    filled = False
                    t0 = time.time()
                    while time.time()-t0 < 8.0:
                        pos = get_position(symbol)
                        if pos and float(pos.get("qty","0")) > 0:
                            filled = True
                            break
                        time.sleep(0.4)
                if filled:
                    attach_oco_exit_if_position(symbol, cfg.qty, tp, sl, OCO_TIF, system)
                else:
                    log.info(f"Skipped OCO attach for {symbol}: position not filled in time.")
        return 200, out
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text   = getattr(e.response, "text", "")
        return 200, {"ok": False, "status": status, "error": text}

# ========= Scanner (S2) =========
def scan_s2_symbols(symbols: List[str], tf: str, mode: str, use_rth: bool, use_vol: bool, dry_run: bool) -> Dict[str,Any]:
    tf_c = _canon_timeframe(tf)
    checked = []
    triggers = []

    for sym in symbols:
        s = sym.strip().upper()
        try:
            # Daily for trend filter
            d_bars = bars(s, "1Day", limit=60)
            if len(d_bars) < 12: 
                checked.append({"symbol": s, "skip": "not_enough_daily"})
                continue
            closes_d = [float(b["c"]) for b in d_bars]
            sma10 = sma(closes_d, 10)[-1]
            trend_ok = (closes_d[-1] > sma10) if sma10 is not None else False

            # Intraday/timeframe bars for MACD
            i_bars = bars(s, tf_c, limit=200)
            if len(i_bars) < 35:
                checked.append({"symbol": s, "skip": "not_enough_intraday"})
                continue
            closes = [float(b["c"]) for b in i_bars]
            vols   = [float(b["v"]) for b in i_bars]

            m_line, s_line, _ = macd(closes, 12, 26, 9)
            if len(m_line) < 2: 
                checked.append({"symbol": s, "skip": "macd_len"})
                continue
            prev_cross = m_line[-2] - s_line[-2]
            last_cross = m_line[-1] - s_line[-1]
            macd_up    = (prev_cross <= 0 and last_cross > 0)

            vol_ok = True
            if use_vol and len(vols) >= 20:
                v_sma20 = sma(vols, 20)[-1]
                vol_ok = v_sma20 is not None and vols[-1] > v_sma20

            should_long = macd_up and (trend_ok if mode=="strict" else True) and vol_ok

            checked.append({"symbol": s, "trend_ok": trend_ok, "macd_up": macd_up, "vol_ok": vol_ok})
            if should_long:
                ref = closes[-1]
                payload = {"system":"SMA10D_MACD","side":"buy","ticker":s,"price":ref,"time":_iso(datetime.now(timezone.utc))}
                if dry_run:
                    triggers.append(payload)
                else:
                    status, out = process_signal(payload)
                    log.info(f"S2 execute {s}: {status} {out}")
                    triggers.append(payload)
        except Exception as e:
            checked.append({"symbol": s, "error": str(e)})
    return {"checked": checked, "triggers": triggers, "timeframe": tf_c}

# ===== Routes =====
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "equities-webhook", "time": int(time.time())}), 200

@app.post("/webhook")
def webhook():
    """Accept external alerts (or tests) and pass to the shared processor."""
    data = request.get_json(force=True, silent=True) or {}
    status, out = process_signal(data)
    return jsonify(out), status

@app.get("/performance")
def performance():
    """Realized P&L from Alpaca fills (FIFO)."""
    try:
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

        acts = fetch_trade_activities(start_iso, end_iso)  # resilient
        order_ids = [a.get("order_id") for a in acts if a.get("order_id")]
        cid_map = fetch_client_order_ids(order_ids)        # resilient
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

        # guard against NaN/Inf sneaking into JSON (Flask will 500)
        return jsonify(_scrub_nans(out)), 200

    except Exception as e:
        app.logger.exception("performance route error")
        # Keep 200 so the dashboard JS doesn't throw
        return jsonify({"ok": False, "error": str(e)}), 200

@app.get("/performance/daily")
def performance_daily():
    """Daily buckets of realized P&L (UTC), with per-strategy splits."""
    try:
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

        return jsonify(_scrub_nans({"ok": True, "days": days, "daily": daily})), 200

    except Exception as e:
        app.logger.exception("performance/daily route error")
        return jsonify({"ok": False, "error": str(e)}), 200

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
            "oco_require_position": OCO_REQUIRE_POSITION,
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
    :root { --bg:#0b0f14; --panel:#0f172a; --border:#1f2937; --ink:#e2e8f0; --muted:#94a3b8; --grid:#334155; --pos-h:142; --neg-h:0; }
    html, body { background: var(--bg); color: var(--ink); }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin: 0 0 16px 0; } h2 { margin: 0 0 12px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.35); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid var(--border); }
    th { color: var(--muted); font-weight: 600; }
    .muted { color: var(--muted); font-size: 12px; }
    .cal-wrap { display: grid; gap: 12px; }
    .cal-head { display: flex; align-items: center; gap: 8px; justify-content: space-between; }
    .cal-title { font-weight: 700; font-size: 18px; }
    .cal-ctl button { padding: 6px 10px; border-radius: 8px; border: 1px solid var(--border); background: #0b1220; color: var(--ink); cursor: pointer; }
    .cal-grid { display: grid; grid-template-columns: repeat(7, 1fr); gap: 8px; }
    .dow { text-align:center; font-size:12px; color: var(--muted); margin-bottom: -6px; }
    .day { min-height: 82px; border: 1px solid var(--border); border-radius: 10px; padding: 6px 8px; position: relative; background: #0b1220; display:flex; flex-direction:column; justify-content:flex-end; }
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
    function fmt(n) { return (n>=0?'+':'') + Number(n).toFixed(2); }
    Chart.defaults.color = '#e2e8f0';
    Chart.defaults.borderColor = '#334155';

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
      DOW.forEach(d => { const el = document.createElement('div'); el.className = 'dow'; el.textContent = d; dowRow.appendChild(el); });
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
            <div class="pl">$${Number(pnl||0).toFixed(2)}</div>
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
      document.getElementById('summary').innerHTML = '<div>Total realized P&amp;L: <b>$' + Number(perf7.total_realized_pnl||0).toFixed(2) + '</b></div>';

      // By strategy
      const tbody = document.querySelector('#byStrategy tbody');
      tbody.innerHTML = '';
      Object.entries(perf7.by_strategy || {}).forEach(([k,v]) => {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>'+k+'</td><td>'+(v.trades||0)+'</td><td>'+(v.win_rate||0)+'%</td><td>'+Number(v.realized_pnl||0).toFixed(2)+'</td>';
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
      force=1                (optional) bypass market-open gate
    """
    try:
        # Auto-skip when market closed unless force=1
        if request.args.get("force", "0") != "1":
            if not is_market_open_now():
                return jsonify({"ok": True, "skipped": "market_closed"}), 200

        default_syms = S2_WHITELIST
        symbols = request.args.get("symbols", default_syms).split(",")
        tf      = request.args.get("tf", S2_TF_DEFAULT)
        mode    = request.args.get("mode", S2_MODE_DEFAULT)
        rth     = (request.args.get("rth", S2_USE_RTH_DEFAULT).lower() == "true")
        vol     = (request.args.get("vol", S2_USE_VOL_DEFAULT).lower() == "true")
        dry     = request.args.get("dry", "1") == "1"

        scan = scan_s2_symbols(symbols, tf=tf, mode=mode, use_rth=rth, use_vol=vol, dry_run=dry)

        res = {
            "ok": True,
            "timeframe": _canon_timeframe(tf),
            "mode": mode,
            "use_rth": rth,
            "use_vol": vol,
            "dry_run": dry,
            "checked": scan.get("checked", []),
            "triggers": [],
        }

        if dry:
            res["triggers"] = scan.get("triggers", [])
            return jsonify(res), 200

        # Live: execute triggers in-process (already done in scan when dry=False via process_signal)
        res["triggers"] = scan.get("triggers", [])
        return jsonify(res), 200

    except Exception as e:
        app.logger.exception(f"/scan/s2 route error: {e}")
        # Return 200 with an error payload so crons don't mark as failed
        return jsonify({"ok": False, "error": str(e)}), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
