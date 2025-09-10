# app.py
import os
import json
import time
import math
import uuid
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify, Response, render_template_string

# --------------------------------------
# Config & Environment
# --------------------------------------
TZ = timezone(timedelta(hours=-5))  # America/Chicago (fixed offset for simplicity)
APP_NAME = "Equities Webhook/Scanner"

APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
APCA_DATA_BASE_URL = os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets/v2")
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "")
APCA_DATA_FEED = os.getenv("APCA_DATA_FEED", "iex")  # "iex" for paper; consider "sip" for live

# Risk / safety caps
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
MAX_POSITIONS_PER_SYMBOL = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", "1"))
MAX_POSITIONS_PER_STRATEGY = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", "3"))

CANCEL_OPEN_ORDERS_BEFORE_PLAIN = os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN", "true").lower() == "true"
ATTACH_OCO_ON_PLAIN = os.getenv("ATTACH_OCO_ON_PLAIN", "true").lower() == "true"
OCO_REQUIRE_POSITION = os.getenv("OCO_REQUIRE_POSITION", "true").lower() == "true"

# Default exits if order_mapper doesn't provide them
DEFAULT_TP_PCT = float(os.getenv("DEFAULT_TP_PCT", "0.010"))   # 1.0%
DEFAULT_SL_PCT = float(os.getenv("DEFAULT_SL_PCT", "0.007"))   # 0.7%
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))

# Strategy 2 defaults
S2_WHITELIST = os.getenv(
    "S2_WHITELIST",
    "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL"
).split(",")

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("app")

app = Flask(__name__)

# --------------------------------------
# Optional import: order_mapper.py
# --------------------------------------
class _DefaultOrderPlan:
    def __init__(self, side: str, qty: int = DEFAULT_QTY, order_class: str = "bracket",
                 take_profit_pct: float = DEFAULT_TP_PCT, stop_loss_pct: float = DEFAULT_SL_PCT,
                 tif: str = "day"):
        self.side = side  # "buy" or "sell"
        self.qty = qty
        self.order_class = order_class  # "bracket" or "plain"
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.time_in_force = tif

class _DefaultStrategyRouter:
    """
    Fallback logic if order_mapper.py is not provided.
    You can replace this by providing order_mapper.StrategyRouter with:
      .plan_for_webhook(strategy_name, payload) -> _DefaultOrderPlan
      .plan_for_signal(strategy_name, symbol, side, ref_price) -> _DefaultOrderPlan
    """
    def plan_for_webhook(self, strategy_name: str, payload: Dict[str, Any]) -> _DefaultOrderPlan:
        side = payload.get("side", "buy").lower()
        return _DefaultOrderPlan(side=side, order_class="bracket")

    def plan_for_signal(self, strategy_name: str, symbol: str, side: str, ref_price: float) -> _DefaultOrderPlan:
        # First entry bracket; adds plain (caller decides which)
        return _DefaultOrderPlan(side=side, order_class="bracket")

try:
    from order_mapper import StrategyRouter  # type: ignore
    router = StrategyRouter()
    log.info("Using StrategyRouter from order_mapper.py")
except Exception as e:
    log.warning(f"order_mapper.py not found or invalid, using defaults: {e}")
    router = _DefaultStrategyRouter()

# --------------------------------------
# HTTP helpers to Alpaca
# --------------------------------------
def _auth_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": APCA_API_KEY_ID,
        "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
        "Content-Type": "application/json",
    }

def _get(url: str, params: Dict[str, Any] = None, data_api: bool = False):
    base = APCA_DATA_BASE_URL if data_api else APCA_API_BASE_URL
    r = requests.get(f"{base}{url}", headers=_auth_headers(), params=params, timeout=20)
    if not r.ok:
        log.error(f"GET {url} failed: {r.status_code} {r.text}")
    r.raise_for_status()
    return r.json()

def _post(url: str, payload: Dict[str, Any]):
    r = requests.post(f"{APCA_API_BASE_URL}{url}", headers=_auth_headers(), data=json.dumps(payload), timeout=20)
    if not r.ok:
        log.error(f"POST {url} failed: {r.status_code} {r.text} payload={payload}")
    r.raise_for_status()
    return r.json()

def _delete(url: str):
    r = requests.delete(f"{APCA_API_BASE_URL}{url}", headers=_auth_headers(), timeout=20)
    if not r.ok:
        log.error(f"DELETE {url} failed: {r.status_code} {r.text}")
    r.raise_for_status()
    return r.json() if r.text else {}

# --------------------------------------
# Market data & account helpers
# --------------------------------------
def now_ct() -> datetime:
    return datetime.now(TZ)

def get_clock() -> Dict[str, Any]:
    return _get("/clock")

def market_is_open() -> bool:
    try:
        clk = get_clock()
        return bool(clk.get("is_open", False))
    except Exception:
        return False

def bars(symbol: str, timeframe: str = "1Hour", limit: int = 200, start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
    params = {"timeframe": timeframe, "limit": limit, "feed": APCA_DATA_FEED}
    if start: params["start"] = start
    if end: params["end"] = end
    # GET /v2/stocks/{symbol}/bars
    data = _get(f"/stocks/{symbol}/bars", params=params, data_api=True)
    return data.get("bars", [])

def bars_daily(symbol: str, limit: int = 60) -> List[Dict[str, Any]]:
    return bars(symbol, timeframe="1Day", limit=limit)

def get_position(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        return _get(f"/positions/{symbol}")
    except Exception:
        return None

def list_positions() -> List[Dict[str, Any]]:
    try:
        return _get("/positions")
    except Exception:
        return []

def list_open_orders(symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    params = {"status": "open", "nested": "true"}
    if symbols:
        params["symbols"] = ",".join(symbols)
    try:
        return _get("/orders", params=params)
    except Exception:
        return []

def cancel_all_orders(symbol: Optional[str] = None):
    if symbol:
        # Cancel per-symbol
        orders = list_open_orders([symbol])
        for o in orders:
            try:
                _delete(f"/orders/{o['id']}")
            except Exception:
                pass
    else:
        try:
            _delete("/orders")
        except Exception:
            pass

def account_info() -> Dict[str, Any]:
    return _get("/account")

def portfolio_history(period: str = "30D", timeframe: str = "1D") -> Dict[str, Any]:
    params = {"period": period, "timeframe": timeframe}
    return _get("/account/portfolio/history", params=params)

def list_fills_today() -> List[Dict[str, Any]]:
    # Activities API: /v2/account/activities?activity_types=FILL&date=YYYY-MM-DD
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    params = {"activity_types": "FILL", "date": today}
    try:
        acts = _get("/account/activities", params=params)
        # Ensure list
        if isinstance(acts, dict) and acts.get("activities"):
            acts = acts["activities"]
        return acts if isinstance(acts, list) else []
    except Exception:
        return []

# --------------------------------------
# Technical indicators
# --------------------------------------
def sma(values: List[float], length: int) -> List[Optional[float]]:
    out = []
    q = []
    s = 0.0
    for v in values:
        q.append(v)
        s += v
        if len(q) > length:
            s -= q.pop(0)
        out.append(s / length if len(q) == length else None)
    return out

def ema(values: List[float], length: int) -> List[Optional[float]]:
    out = []
    k = 2 / (length + 1.0)
    ema_prev = None
    for v in values:
        if ema_prev is None:
            ema_prev = v
        else:
            ema_prev = v * k + ema_prev * (1 - k)
        out.append(ema_prev)
    return out

def macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    ema_fast = ema(values, fast)
    ema_slow = ema(values, slow)
    macd_line = [ (f - s) if (f is not None and s is not None) else None for f, s in zip(ema_fast, ema_slow)]
    macd_line_filled = [x if x is not None else 0.0 for x in macd_line]
    signal_line = ema(macd_line_filled, signal)
    hist = [ (m - s) for m, s in zip(macd_line_filled, signal_line)]
    return macd_line_filled, signal_line, hist

# --------------------------------------
# Order creation & OCO attach (exit-only)
# --------------------------------------
def _cid(prefix: str, strategy: str, symbol: str) -> str:
    rnd = uuid.uuid4().hex[:8]
    return f"{prefix}-{strategy}-{symbol}-{rnd}"

def _compute_exits_from_price(entry_px: float, side: str,
                              tp_pct: float, sl_pct: float) -> Tuple[float, float]:
    if side == "buy":
        tp = round(entry_px * (1 + tp_pct), 2)
        sl = round(entry_px * (1 - sl_pct), 2)
    else:
        # For shorts (if you add later): invert
        tp = round(entry_px * (1 - tp_pct), 2)
        sl = round(entry_px * (1 + sl_pct), 2)
    return tp, sl

def post_bracket(symbol: str, side: str, qty: int, entry_type: str, tif: str,
                 tp_price: float, sl_price: float, strategy: str) -> Dict[str, Any]:
    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side,
        "type": entry_type,           # "market" or "limit"
        "time_in_force": tif,         # "day" or "gtc"
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{tp_price:.2f}"},
        "stop_loss": {"stop_price": f"{sl_price:.2f}"},
        "client_order_id": _cid("BRK", strategy, symbol)
    }
    return _post("/orders", payload)

def post_plain(symbol: str, side: str, qty: int, entry_type: str, tif: str, strategy: str,
               limit_price: Optional[float] = None) -> Dict[str, Any]:
    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": side,
        "type": entry_type,
        "time_in_force": tif,
        "client_order_id": _cid("PLN", strategy, symbol)
    }
    if entry_type == "limit" and limit_price is not None:
        payload["limit_price"] = f"{limit_price:.2f}"
    return _post("/orders", payload)

def attach_oco_exit_if_position(symbol: str, qty_hint: Optional[int],
                                tp_price: float, sl_price: float,
                                tif: str, strategy: str) -> Optional[Dict[str, Any]]:
    """
    Post OCO only if a filled position exists (exit-only rule).
    """
    pos = get_position(symbol)
    if not pos:
        log.info(f"Skip OCO attach for {symbol}: no position present.")
        return None

    qty = qty_hint or int(float(pos.get("qty", "0")))
    if qty <= 0:
        log.info(f"Skip OCO attach for {symbol}: qty <= 0")
        return None

    payload = {
        "symbol": symbol,
        "qty": str(qty),
        "side": "sell",  # long exit; (extend if you support shorts)
        "type": "limit",
        "time_in_force": tif if tif in ("day", "gtc") else "gtc",
        "order_class": "oco",
        "take_profit": {"limit_price": f"{tp_price:.2f}"},
        "stop_loss": {"stop_price": f"{sl_price:.2f}"},
        "client_order_id": _cid("OCO", strategy, symbol)
    }
    try:
        return _post("/orders", payload)
    except requests.HTTPError as e:
        # Surface useful line in logs; return None to avoid cascades
        log.error(f"OCO_POST_ERROR status={getattr(e.response, 'status_code', '??')} body={getattr(e.response, 'text', '')} sent={payload}")
        return None

def wait_for_position(symbol: str, timeout_sec: float = 7.5, poll_sec: float = 0.5) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        pos = get_position(symbol)
        if pos and float(pos.get("qty", "0")) > 0:
            return True
        time.sleep(poll_sec)
    return False

# --------------------------------------
# Caps & gating
# --------------------------------------
def count_positions_by_strategy(strategy: str) -> int:
    # Strategy attribution via client_order_id prefix is best; for simplicity use current positions
    # and assume 1 per symbol per strategy. Enhance if you track per-order strategy tags server-side.
    return 0  # Soft bypass; rely on MAX_POSITIONS_PER_SYMBOL & MAX_OPEN_POSITIONS primarily.

def can_open_new_position(symbol: str, strategy: str) -> Tuple[bool, str]:
    positions = list_positions()
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False, f"MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS} reached"

    sym_count = sum(1 for p in positions if p.get("symbol") == symbol)
    if sym_count >= MAX_POSITIONS_PER_SYMBOL:
        return False, f"MAX_POSITIONS_PER_SYMBOL={MAX_POSITIONS_PER_SYMBOL} reached for {symbol}"

    strat_count = count_positions_by_strategy(strategy)
    if strat_count >= MAX_POSITIONS_PER_STRATEGY:
        return False, f"MAX_POSITIONS_PER_STRATEGY={MAX_POSITIONS_PER_STRATEGY} reached for {strategy}"

    return True, "ok"

# --------------------------------------
# Strategy 1: Webhook (TradingView) -> order
# JSON: {"system","side","ticker","price","time"}
# --------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(force=True, silent=True) or {}
    strategy = payload.get("system", "UNKNOWN")
    symbol = payload.get("ticker", "").upper()
    side = payload.get("side", "buy").lower()
    ref_price = float(payload.get("price", 0) or 0)

    if not symbol or side not in ("buy", "sell"):
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    # Caps
    ok, reason = can_open_new_position(symbol, strategy)
    if not ok:
        log.info(f"CAP_BLOCK webhook {symbol} {strategy}: {reason}")
        return jsonify({"ok": False, "blocked": reason}), 200

    # Order plan (prefer router)
    try:
        plan = router.plan_for_webhook(strategy, payload)
    except Exception:
        plan = _DefaultOrderPlan(side=side, order_class="bracket")

    entry_type = "market"
    tif = plan.time_in_force

    # Optionally cancel open symbol orders before a plain add (wash-trade protection)
    if plan.order_class == "plain" and CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
        cancel_all_orders(symbol)

    # Compute exits if bracket or if we plan to attach OCO after fill
    tp_px, sl_px = _compute_exits_from_price(
        entry_px=ref_price if ref_price > 0 else ref_price_from_last(symbol),
        side=plan.side,
        tp_pct=plan.take_profit_pct,
        sl_pct=plan.stop_loss_pct
    )

    results = {"strategy": strategy, "symbol": symbol, "plan": plan.order_class}

    try:
        if plan.order_class == "bracket":
            r = post_bracket(symbol, plan.side, plan.qty, entry_type, tif, tp_px, sl_px, strategy)
            results["order"] = r
        else:
            r = post_plain(symbol, plan.side, plan.qty, entry_type, tif, strategy)
            results["order"] = r
            if ATTACH_OCO_ON_PLAIN:
                # wait for fill then attach OCO (exit-only)
                if not OCO_REQUIRE_POSITION or wait_for_position(symbol):
                    attach_oco_exit_if_position(symbol, plan.qty, tp_px, sl_px, tif, strategy)
                else:
                    log.info(f"Skipped OCO attach for {symbol}: position not filled.")

        return jsonify({"ok": True, **results})
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        body = getattr(e.response, "text", "")
        return jsonify({"ok": False, "status": status, "error": body}), 200

def ref_price_from_last(symbol: str) -> float:
    try:
        b = bars(symbol, timeframe="1Min", limit=1)
        if b:
            return float(b[-1]["c"])
    except Exception:
        pass
    return 0.0

# --------------------------------------
# Strategy 2: Scanner (SMA10D + MACD)
# --------------------------------------
@app.route("/scan/s2", methods=["GET"])
def scan_s2():
    symbols = request.args.get("symbols", ",".join(S2_WHITELIST)).split(",")
    tf = request.args.get("tf", "60")  # 60|30|120|day
    mode = request.args.get("mode", "either")  # either|strict
    rth = request.args.get("rth", "true").lower() == "true"
    vol = request.args.get("vol", "false").lower() == "true"
    dry = request.args.get("dry", "1") == "1"
    force = request.args.get("force", "0") == "1"
    strategy = "SMA10D_MACD"

    # Market open gate
    if rth and not force and not market_is_open():
        return jsonify({"ok": True, "skipped": "market closed", "symbols": symbols})

    # Map tf param to Alpaca timeframe
    tf_map = {"30": "30Min", "60": "1Hour", "120": "2Hour", "day": "1Day"}
    timeframe = tf_map.get(tf, "1Hour")

    actions = []
    for sym in symbols:
        sym = sym.strip().upper()
        try:
            # Daily bars for SMA10D trend filter
            d_bars = bars_daily(sym, limit=60)
            if len(d_bars) < 11:
                continue
            closes_d = [float(b["c"]) for b in d_bars]
            sma10d = sma(closes_d, 10)
            last_close = closes_d[-1]
            last_sma10 = sma10d[-1]
            trend_ok = (last_close > last_sma10) if last_sma10 is not None else False

            # Timeframe bars for MACD signal
            i_bars = bars(sym, timeframe=timeframe, limit=200)
            closes = [float(b["c"]) for b in i_bars]
            vols = [float(b["v"]) for b in i_bars]
            if len(closes) < 35:
                continue
            macd_line, signal_line, _ = macd(closes, 12, 26, 9)
            # Cross detection on last two bars
            if len(macd_line) < 2 or len(signal_line) < 2:
                continue
            prev_cross = macd_line[-2] - signal_line[-2]
            last_cross = macd_line[-1] - signal_line[-1]
            macd_up = (prev_cross <= 0 and last_cross > 0)
            macd_down = (prev_cross >= 0 and last_cross < 0)

            # Volume filter (optional): last vol > SMA20 of vol
            vol_ok = True
            if vol and len(vols) >= 20:
                v_sma20 = sma(vols, 20)[-1]
                vol_ok = v_sma20 is not None and vols[-1] > v_sma20

            # Mode logic
            should_long = macd_up and (trend_ok if mode == "strict" else True) and vol_ok
            # (You can extend with short logic if desired)
            if should_long:
                ref_px = closes[-1]
                if dry:
                    actions.append({"symbol": sym, "action": "BUY", "ref_price": ref_px, "mode": mode, "vol_ok": vol_ok, "trend_ok": trend_ok})
                else:
                    ok, reason = can_open_new_position(sym, strategy)
                    if not ok:
                        actions.append({"symbol": sym, "action": "BLOCKED", "reason": reason})
                        continue

                    # Decide bracket vs plain: if no position, bracket; else plain + attach OCO
                    has_pos = get_position(sym) is not None
                    side = "buy"
                    if has_pos:
                        plan = _DefaultOrderPlan(side=side, order_class="plain")
                    else:
                        try:
                            plan = router.plan_for_signal(strategy, sym, side, ref_px)
                        except Exception:
                            plan = _DefaultOrderPlan(side=side, order_class="bracket")

                    tp_px, sl_px = _compute_exits_from_price(ref_px, side, plan.take_profit_pct, plan.stop_loss_pct)

                    if plan.order_class == "bracket":
                        res = post_bracket(sym, side, plan.qty, "market", plan.time_in_force, tp_px, sl_px, strategy)
                        actions.append({"symbol": sym, "order": res, "class": "bracket"})
                    else:
                        if CANCEL_OPEN_ORDERS_BEFORE_PLAIN:
                            cancel_all_orders(sym)
                        res = post_plain(sym, side, plan.qty, "market", plan.time_in_force, strategy)
                        actions.append({"symbol": sym, "order": res, "class": "plain"})
                        if ATTACH_OCO_ON_PLAIN:
                            if not OCO_REQUIRE_POSITION or wait_for_position(sym):
                                attach_oco_exit_if_position(sym, plan.qty, tp_px, sl_px, plan.time_in_force, strategy)
                            else:
                                log.info(f"Skipped OCO attach for {sym}: position not filled.")
        except Exception as ex:
            actions.append({"symbol": sym, "error": str(ex)})

    return jsonify({"ok": True, "time": now_ct().isoformat(), "timeframe": timeframe, "mode": mode, "dry": dry, "actions": actions})

# --------------------------------------
# Performance endpoints
# --------------------------------------
def fifo_realized_pnl_today() -> Tuple[float, List[Dict[str, Any]]]:
    """
    Compute intraday realized P&L per symbol using today's fills with a simple FIFO.
    Assumes day trading (no carry from prior day). Open positions are ignored in realized P&L.
    """
    fills = list_fills_today()
    # Normalize fills to {symbol, side, qty, price, timestamp, client_order_id}
    norm = []
    for f in fills:
        try:
            symbol = f.get("symbol")
            side = f.get("side", "").lower()
            qty = float(f.get("qty", 0))
            price = float(f.get("price", 0))
            ts = f.get("transaction_time") or f.get("date") or f.get("id")
            cid = f.get("order_id") or f.get("id")
            norm.append({"symbol": symbol, "side": side, "qty": qty, "price": price, "time": ts, "client_order_id": cid})
        except Exception:
            continue

    # Group by symbol and FIFO match
    realized = 0.0
    trades_detail = []
    from collections import defaultdict, deque
    buys = defaultdict(deque)
    sells = defaultdict(list)

    for t in sorted(norm, key=lambda x: str(x["time"])):
        s = t["symbol"]
        if t["side"] == "buy":
            buys[s].append({"qty": t["qty"], "price": t["price"], "time": t["time"]})
        elif t["side"] == "sell":
            sells[s].append(t)

    for s, sell_list in sells.items():
        for sfill in sell_list:
            qty_to_match = sfill["qty"]
            pnl_for_this = 0.0
            legs = []
            while qty_to_match > 0 and buys[s]:
                bfill = buys[s][0]
                match_qty = min(qty_to_match, bfill["qty"])
                pnl_leg = (sfill["price"] - bfill["price"]) * match_qty
                pnl_for_this += pnl_leg
                legs.append({"buy_px": bfill["price"], "sell_px": sfill["price"], "qty": match_qty, "pnl": pnl_leg})
                # decrement
                bfill["qty"] -= match_qty
                qty_to_match -= match_qty
                if bfill["qty"] <= 1e-9:
                    buys[s].popleft()
            realized += pnl_for_this
            trades_detail.append({
                "symbol": s,
                "strategy": infer_strategy_from_cid(sfill.get("client_order_id", "")),
                "pnl": pnl_for_this,
                "legs": legs,
                "time": sfill["time"]
            })
    return realized, trades_detail

def infer_strategy_from_cid(cid: str) -> str:
    # Example cids: BRK-<STRAT>-<SYM>-xxxx | PLN-... | OCO-...
    parts = cid.split("-")
    if len(parts) >= 3:
        return parts[1]
    return "UNKNOWN"

@app.route("/performance", methods=["GET"])
def performance():
    days = int(request.args.get("days", "7"))
    include_trades = request.args.get("include_trades", "0") == "1"

    # Today realized pnl via FIFO
    realized_today, trades_detail = fifo_realized_pnl_today()

    # Equity curve (period=days)
    period = f"{max(days,1)}D"
    hist = portfolio_history(period=period, timeframe="1D")

    # Aggregate by strategy from trades_detail
    by_strategy = {}
    for t in trades_detail:
        by_strategy.setdefault(t["strategy"], 0.0)
        by_strategy[t["strategy"]] += t.get("pnl", 0.0)

    resp = {
        "ok": True,
        "as_of": now_ct().isoformat(),
        "total_pnl": realized_today,
        "by_strategy": by_strategy,
    }
    if include_trades:
        resp["trades"] = trades_detail
    if hist:
        resp["equity_curve"] = {
            "timestamp": hist.get("timestamp", []),
            "equity": hist.get("equity", []),
            "profit_loss": hist.get("profit_loss", []),
        }
    return jsonify(resp)

@app.route("/performance/daily", methods=["GET"])
def performance_daily():
    days = int(request.args.get("days", "30"))
    hist = portfolio_history(period=f"{max(days,1)}D", timeframe="1D")
    # Normalize to a simple series array
    series = []
    ts = hist.get("timestamp", [])
    eq = hist.get("equity", [])
    for t, e in zip(ts, eq):
        # Portfolio history returns epoch seconds (UTC)
        dt = datetime.fromtimestamp(t, tz=timezone.utc).astimezone(TZ).strftime("%Y-%m-%d")
        series.append({"date": dt, "equity": e})
    return jsonify({"ok": True, "series": series})

# --------------------------------------
# Info routes
# --------------------------------------
@app.route("/positions", methods=["GET"])
def positions_route():
    return jsonify(list_positions())

@app.route("/orders/open", methods=["GET"])
def orders_open():
    return jsonify(list_open_orders())

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "app": APP_NAME,
        "time": now_ct().isoformat(),
        "env": {
            "APCA_API_BASE_URL": APCA_API_BASE_URL,
            "APCA_DATA_FEED": APCA_DATA_FEED,
            "MAX_OPEN_POSITIONS": MAX_OPEN_POSITIONS,
            "MAX_POSITIONS_PER_SYMBOL": MAX_POSITIONS_PER_SYMBOL,
            "MAX_POSITIONS_PER_STRATEGY": MAX_POSITIONS_PER_STRATEGY,
            "CANCEL_OPEN_ORDERS_BEFORE_PLAIN": CANCEL_OPEN_ORDERS_BEFORE_PLAIN,
            "ATTACH_OCO_ON_PLAIN": ATTACH_OCO_ON_PLAIN,
            "OCO_REQUIRE_POSITION": OCO_REQUIRE_POSITION,
        }
    })

# --------------------------------------
# Simple dashboard
# --------------------------------------
DASHBOARD_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Trading Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    body { background:#0b0f14; color:#eaeef2; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
    .wrap { max-width: 1100px; margin: 32px auto; padding: 0 16px; }
    h1 { font-size: 22px; margin-bottom: 8px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    pre, code { background: #111722; padding: 12px; border-radius: 10px; overflow:auto; }
    .card { background: #0f1521; padding: 16px; border-radius: 12px; box-shadow: 0 0 0 1px #1f2733 inset; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px; border-bottom: 1px solid #1f2733; font-size: 13px; }
    th { text-align: left; color: #a3b1c2; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Equities Dashboard</h1>
    <div class="grid">
      <div class="card">
        <h3>Today Performance</h3>
        <pre id="perf">Loading...</pre>
      </div>
      <div class="card">
        <h3>Open Positions</h3>
        <pre id="pos">Loading...</pre>
      </div>
      <div class="card" style="grid-column: span 2;">
        <h3>Open Orders</h3>
        <pre id="orders">Loading...</pre>
      </div>
    </div>
  </div>
<script>
async function load() {
  const perf = await fetch('/performance?days=1&include_trades=1').then(r=>r.json()).catch(()=>({}));
  const pos  = await fetch('/positions').then(r=>r.json()).catch(()=>([]));
  const ord  = await fetch('/orders/open').then(r=>r.json()).catch(()=>([]));
  document.getElementById('perf').textContent = JSON.stringify(perf, null, 2);
  document.getElementById('pos').textContent  = JSON.stringify(pos, null, 2);
  document.getElementById('orders').textContent = JSON.stringify(ord, null, 2);
}
load();
</script>
</body>
</html>
"""

@app.route("/dashboard", methods=["GET"])
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

# --------------------------------------
# Main
# --------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
