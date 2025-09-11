import os, time, json
from collections import defaultdict, deque
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone

from .config import (ALPACA_BASE, SESSION, HEADERS, MAX_ACTIVITY_DAYS,
                     MAX_ORDER_LOOKUPS, LOOKUP_BUDGET_SEC, log)
from .utils import _iso

def fetch_trade_activities(start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    acts: List[Dict[str, Any]] = []
    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    days_requested = (end.date() - start.date()).days + 1
    if days_requested > MAX_ACTIVITY_DAYS:
        end = start + timedelta(days=MAX_ACTIVITY_DAYS-1)
    day = start
    while day.date() <= end.date():
        date_str = day.strftime("%Y-%m-%d")
        try:
            url = f"{ALPACA_BASE}/account/activities"
            params = {"activity_types":"FILL", "date": date_str}
            r = SESSION.get(url, headers=HEADERS, params=params, timeout=4)
            if r.ok:
                data = r.json()
                if isinstance(data, dict) and "activities" in data:
                    data = data["activities"]
                if isinstance(data, list):
                    acts.extend(data)
        except Exception as e:
            log.warning(f"activities fetch failed for {date_str}: {e}")
        day += timedelta(days=1)

    out = []
    for a in acts:
        ts = a.get("transaction_time") or a.get("date")
        if not ts: continue
        if start_iso <= ts <= _iso(end):
            out.append(a)
    return out

def fetch_client_order_ids(order_ids: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    t0 = time.time()
    for i, oid in enumerate(order_ids):
        if i >= MAX_ORDER_LOOKUPS: break
        if time.time() - t0 > LOOKUP_BUDGET_SEC: break
        try:
            r = SESSION.get(f"{ALPACA_BASE}/orders/{oid}", headers=HEADERS, timeout=3)
            if r.ok:
                j = r.json()
                cid = j.get("client_order_id")
                if cid: out[oid] = cid
        except Exception:
            continue
    return out

def _load_symbol_system_map() -> Dict[str, str]:
    try:
        raw = os.getenv("SYMBOL_SYSTEM_MAP", "{}")
        m = json.loads(raw)
        return {k.upper(): v for k, v in m.items()}
    except Exception:
        return {}

def infer_system_from_cid(cid: str, sym_map: Dict[str, str], symbol: str) -> str:
    parts = (cid or "").split("-")
    if len(parts) >= 3:
        return parts[1]
    return sym_map.get(symbol.upper(), "UNKNOWN")

def compute_performance(acts: List[Dict[str, Any]], cid_map: Dict[str, str], sym_map: Dict[str, str]) -> Dict[str, Any]:
    fills = []
    for a in acts:
        try:
            symbol = a.get("symbol") or a.get("symbol_id")
            side = (a.get("side") or "").lower()
            qty = float(a.get("qty") or a.get("quantity") or 0)
            price = float(a.get("price") or a.get("price_per_share") or 0)
            ts = a.get("transaction_time") or a.get("date")
            oid = a.get("order_id")
            cid = cid_map.get(oid or "", "")
            fills.append({"symbol": symbol, "side": side, "qty": qty, "price": price, "time": ts, "order_id": oid, "client_order_id": cid})
        except Exception:
            continue

    buys: Dict[str, deque] = defaultdict(deque)
    sells: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for f in sorted(fills, key=lambda x: x["time"]):
        if f["side"] == "buy":
            buys[f["symbol"]].append({"qty": f["qty"], "price": f["price"], "time": f["time"], "cid": f["client_order_id"]})
        elif f["side"] == "sell":
            sells[f["symbol"]].append(f)

    trades: List[Dict[str, Any]] = []
    total_pnl = 0.0
    equity_series: List[Dict[str, Any]] = []
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
                legs.append({"buy_px": b["price"], "sell_px": sfill["price"], "qty": mqty, "pnl": leg_pnl})
                b["qty"] -= mqty
                qty_to_match -= mqty
                if b["qty"] <= 1e-9:
                    buys[symbol].popleft()
            system = infer_system_from_cid(sfill.get("client_order_id",""), sym_map, symbol)
            trade = {
                "symbol": symbol, "system": system, "side": "long_exit",
                "entry_price": legs[0]["buy_px"] if legs else None,
                "exit_price": sfill["price"],
                "qty": sum(l["qty"] for l in legs) if legs else 0,
                "pnl": pnl_for_trade, "legs": legs,
                "exit_time": sfill["time"], "client_order_id": sfill.get("client_order_id"),
            }
            trades.append(trade)
            total_pnl += pnl_for_trade
            equity_running += pnl_for_trade
            equity_series.append({"time": sfill["time"], "equity": equity_running})

    agg = defaultdict(lambda: {"realized_pnl": 0.0, "trades": 0, "wins": 0})
    for t in trades:
        s = t["system"]
        agg[s]["realized_pnl"] += t["pnl"]
        agg[s]["trades"] += 1
        if t["pnl"] > 0:
            agg[s]["wins"] += 1
    by_strategy = {}
    for k, v in agg.items():
        n = v["trades"]
        win_rate = round((v["wins"]/n*100) if n else 0.0, 1)
        by_strategy[k] = {"realized_pnl": v["realized_pnl"], "trades": n, "win_rate": win_rate}

    return {"total_pnl": total_pnl, "trades": trades, "by_strategy": by_strategy, "equity": equity_series}

def bucket_daily(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    days: Dict[str, Dict[str, Any]] = {}
    for t in trades:
        day = str(t["exit_time"])[:10]
        d = days.setdefault(day, {"date": day, "pnl": 0.0, "by_strategy": defaultdict(float)})
        d["pnl"] += t["pnl"]
        d["by_strategy"][t["system"]] += t["pnl"]
    out = []
    for day, row in sorted(days.items()):
        out.append({"date": day, "pnl": row["pnl"], "by_strategy": dict(row["by_strategy"])})
    return out