"""
equities-system-api (app.py)
----------------------------
Alpaca-only equities trading webhook + scheduler.

Key routes:
  GET  /                  -> basic status
  GET  /health            -> heartbeat
  GET  /routes            -> list routes
  GET  /dashboard         -> HTML dashboard UI
  GET  /config            -> config snapshot (safe)
  GET  /debug/env         -> safe env dump (no secrets)

  POST /scheduler/v2/run  -> run one scheduler pass (scan -> intents -> optional orders)
  GET  /scheduler/v2/status -> scheduler thread status

  GET  /orders            -> broker open orders
  GET  /positions         -> broker open positions
  GET  /fills             -> best-effort recent fills (broker history)
  GET  /price/{symbol}   -> last traded price
"""

from __future__ import annotations

import logging
import os
import threading
import time
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import br_router as br

# Core engine modules
from scheduler_core import SchedulerConfig, SchedulerResult, run_scheduler_once
from risk_engine import RiskEngine
from position_manager import Position as PMPosition
from strategy_api import PositionSnapshot


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _last_price_safe(symbol: str) -> Optional[float]:
    """Best-effort last price fetch from the active broker."""
    try:
        return float(br.last_price(symbol))
    except Exception:
        return None


def _load_open_positions_for_equities(default_strat: str = "e1") -> Dict[tuple, PMPosition]:
    """
    Scheduler-core expects positions keyed by (symbol, strategy).

    In an equities-only, single-strategy system, broker positions do not carry
    a strategy id. We therefore attribute all open broker positions to the
    default strategy (e.g. 'e1').
    """
    out: Dict[tuple, PMPosition] = {}
    try:
        pos_list = br.positions()  # expected list[dict] from broker
    except Exception:
        pos_list = []

    if not isinstance(pos_list, list):
        return out

    for p in pos_list:
        if not isinstance(p, dict):
            continue
        sym = str(p.get("symbol", "")).upper().strip()
        if not sym:
            continue
        try:
            qty = float(p.get("qty") or p.get("quantity") or p.get("qty_available") or 0.0)
        except Exception:
            qty = 0.0
        if qty == 0:
            continue
        avg = p.get("avg_entry_price") or p.get("avg_price") or p.get("avgEntryPrice")
        try:
            avg_f = float(avg) if avg is not None else None
        except Exception:
            avg_f = None

        out[(sym, default_strat)] = PMPosition(symbol=sym, strategy=default_strat, qty=qty, avg_price=avg_f)

    return out

APP_VERSION = os.getenv("APP_VERSION", "unknown")
TZ = os.getenv("TZ", "America/Chicago")

log = logging.getLogger("equities-system-api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(title="Equities Trading Webhook", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Basic routes
# -----------------------

@app.get("/")
def root():
    return {
        "ok": True,
        "app": "equities-system-api",
        "version": APP_VERSION,
        "broker": os.getenv("BROKER", "alpaca"),
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/routes")
def routes():
    return sorted([{"path": r.path, "methods": sorted(list(r.methods or []))} for r in app.router.routes], key=lambda x: x["path"])

@app.get("/config")
def config():
    # safe config snapshot (no secrets)
    return {
        "ok": True,
        "version": APP_VERSION,
        "broker": os.getenv("BROKER", "alpaca"),
        "tz": TZ,
        "scheduler": {
            "enabled": os.getenv("SCHED_ENABLED", "0"),
            "sleep": float(os.getenv("SCHED_SLEEP", "30") or 30),
            "timeframe": os.getenv("SCHED_TIMEFRAME", "5Min"),
            "limit": int(os.getenv("SCHED_LIMIT", "800") or 800),
            "notional": float(os.getenv("SCHED_NOTIONAL", "40") or 40),
            "strats": os.getenv("SCHED_STRATS", "e1"),
            "symbols": os.getenv("SYMBOLS", ""),
            "trading_enabled": os.getenv("TRADING_ENABLED", "0"),
        },
    }

@app.get("/debug/env")
def debug_env():
    allow_prefixes = ("APP_", "BROKER", "TZ", "SCHED_", "TRADING_", "APCA_", "E1_", "LOG_")
    safe = {}
    for k, v in os.environ.items():
        if not k.startswith(allow_prefixes):
            continue
        if "KEY" in k or "SECRET" in k or "TOKEN" in k or "PASS" in k:
            safe[k] = "redacted"
        else:
            safe[k] = v
    return {"ok": True, "env": safe}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    p = Path(__file__).with_name("dashboard.html")
    if not p.exists():
        return HTMLResponse("<h3>dashboard.html not found</h3>", status_code=404)
    return HTMLResponse(p.read_text(errors="ignore"))

@app.get("/orders")
def get_orders():
    try:
        return {"ok": True, "orders": br.orders()}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/positions")
def get_positions():
    try:
        return {"ok": True, "positions": br.positions()}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/fills")
def fills(count: int = 50):
    try:
        return {"ok": True, "fills": br.trades_history(count=int(count))}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/price/{symbol}")
def price(symbol: str):
    sym = symbol.strip().upper()
    try:
        return {"ok": True, "symbol": sym, "price": br.last_price(sym)}
    except Exception as e:
        return JSONResponse({"ok": False, "symbol": sym, "error": str(e)}, status_code=500)

# -----------------------
# Scheduler v2
# -----------------------
@app.post("/scheduler/v2/run")
def scheduler_run_v2(payload: Dict[str, Any] = Body(default=None)):
    """
    Scheduler v2:

    - Uses scheduler_core + RiskEngine to produce explicit OrderIntents
      (entries + exits + per-strategy + global exits).
    - Applies per-symbol caps + loss-zone no-rebuy for ENTRY/SCALE intents.
    - Routes surviving intents through br_router.market_notional when dry=False.

    This does NOT remove the legacy /scheduler/run endpoint. Use this side-by-side
    for testing until you're happy to flip over.
    """
    actions: List[Dict[str, Any]] = []
    telemetry: List[Dict[str, Any]] = []

    payload = payload or {}

    # ------------------------------------------------------------------
    # Helper: env bool
    # ------------------------------------------------------------------
    def _env_bool(key: str, default: bool) -> bool:
        v = os.getenv(key)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes", "on")

    # Dry-run flag: payload.dry overrides SCHED_DRY (default True)
    dry = payload.get("dry", None)
    if dry is None:
        dry = _env_bool("SCHED_DRY", True)
    dry = bool(dry)

    # ------------------------------------------------------------------
    # Minimum order notional (USD) to avoid dust orders
    # ------------------------------------------------------------------
    MIN_NOTIONAL_USD = float(os.getenv("MIN_ORDER_NOTIONAL_USD", "5.0") or 5.0)

    # ------------------------------------------------------------------
    # Resolve basic scheduler config from payload + env
    # ------------------------------------------------------------------
    tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
    strats_csv = str(payload.get("strats", os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6")))
    strats = [s.strip().lower() for s in strats_csv.split(",") if s.strip()]

    symbols_csv = str(payload.get("symbols", os.getenv("SYMBOLS", "SPY,SPY")))
    syms = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
    notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25.0)))

    config_snapshot = {
        "tf": tf,
        "strats_raw": strats_csv,
        "strats": strats,
        "symbols_raw": symbols_csv,
        "symbols": syms,
        "limit": limit,
        "notional": notional,
        "dry": bool(dry),
    }

    log.info(
        "Scheduler v2: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
        ",".join(strats),
        tf,
        limit,
        notional,
        dry,
        symbols_csv,
    )

    # ------------------------------------------------------------------
    # Imports that can fail without killing the API entirely
    # ------------------------------------------------------------------
    try:
        import br_router as br  # type: ignore[import]
    except Exception as e:
        log.error("scheduler_v2: failed to import br_router: %s", e)
        if not dry:
            return {"ok": False, "error": f"failed to import br_router: {e}", "config": config_snapshot}
        br = None  # dry mode can still show intents

    try:
        from policy.guard import guard_allows  # type: ignore[import]
    except Exception:
        guard_allows = None  # optional; strategies already use guard internally

    # ------------------------------------------------------------------
    # Load positions & risk config
    # ------------------------------------------------------------------
    # Equities-only: infer positions from broker and attribute to the
    # default strategy. (Broker positions do not include a strategy id.)
    positions = _load_open_positions_for_equities(default_strat=(strats[0] if strats else "e1"))
    risk_engine = RiskEngine()

    # ------------------------------------------------------------------
    # Preload bar contexts once (similar to legacy scheduler_run)
    # ------------------------------------------------------------------
    contexts: Dict[str, Any] = {}

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    for sym in syms:
        try:
            # For now, hard-code 1m + tf (e.g. 5Min) like your existing path
            one = br.get_bars(sym, timeframe="1Min", limit=limit) if br is not None else []
            multi = br.get_bars(sym, timeframe=tf, limit=limit) if br is not None else []

            if not one or not multi:
                contexts[sym] = None
                telemetry.append(
                    {
                        "symbol": sym,
                        "stage": "preload_bars",
                        "ok": False,
                        "reason": "no_bars",
                    }
                )
                continue

            contexts[sym] = {
                "one": {
                    "open": _safe_series(one, "open"),
                    "high": _safe_series(one, "high"),
                    "low": _safe_series(one, "low"),
                    "close": _safe_series(one, "close"),
                    "volume": _safe_series(one, "volume"),
                    "ts": _safe_series(one, "ts"),
                },
                "five": {  # we keep the key name 'five' even if tf != 5Min
                    "open": _safe_series(multi, "open"),
                    "high": _safe_series(multi, "high"),
                    "low": _safe_series(multi, "low"),
                    "close": _safe_series(multi, "close"),
                    "volume": _safe_series(multi, "volume"),
                    "ts": _safe_series(multi, "ts"),
                },
            }
        except Exception as e:
            contexts[sym] = None
            telemetry.append(
                {
                    "symbol": sym,
                    "stage": "preload_bars",
                    "ok": False,
                    "error": f"{e.__class__.__name__}: {e}",
                }
            )

    # ------------------------------------------------------------------
    # Run scheduler_core (strategies + per-strat + global exits)
    # ------------------------------------------------------------------
    now = dt.datetime.utcnow()
    cfg = SchedulerConfig(
        now=now,
        timeframe=tf,
        limit=limit,
        symbols=syms,
        strats=strats,
        notional=notional,
        positions=positions,
        contexts=contexts,
    )

    result: SchedulerResult = run_scheduler_once(cfg, last_price_fn=_last_price_safe)
    telemetry.extend(result.telemetry)

    # ------------------------------------------------------------------
    # Deduplicate intents: exits take precedence over entries on same (sym,strat)
    # ------------------------------------------------------------------
    exit_kinds = {"exit", "take_profit", "stop_loss"}
    entry_kinds = {"entry", "scale"}

    # Priority for exits: stop_loss > take_profit > exit
    def _exit_priority(kind: str) -> int:
        if kind == "stop_loss":
            return 3
        if kind == "take_profit":
            return 2
        return 1  # generic exit

    best_exit: Dict[tuple, Any] = {}
    entries: List[Any] = []

    for intent in result.intents:
        key = (intent.symbol, intent.strategy)

        if intent.kind in exit_kinds:
            prev = best_exit.get(key)
            if prev is None or _exit_priority(intent.kind) > _exit_priority(prev.kind):
                best_exit[key] = intent
        elif intent.kind in entry_kinds:
            entries.append(intent)
        else:
            # unknown kind: just pass through as a generic action
            entries.append(intent)

    # Keep entries only if there is *no* exit for that (symbol,strategy)
    final_intents: List[Any] = list(best_exit.values())
    for intent in entries:
        key = (intent.symbol, intent.strategy)
        if key in best_exit:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": "dropped_entry_due_to_exit_same_pass",
                    "source": "scheduler_v2",
                }
            )
            continue
        final_intents.append(intent)

    # ------------------------------------------------------------------
    # Apply guard + per-symbol caps + loss-zone no-rebuy; then route
    # ------------------------------------------------------------------
    for intent in final_intents:
        key = (intent.symbol, intent.strategy)
        pm_pos = positions.get(key)

        snap = PositionSnapshot(
            symbol=intent.symbol,   
            strategy=intent.strategy,
            qty=float(getattr(pm_pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pm_pos, "avg_price", None),
            unrealized_pct=None,
        )

        # Fill unrealized_pct here so loss-zone + any extra logic
        # see the exact same P&L % that scheduler_core used.
        try:
            snap.unrealized_pct = risk_engine.compute_unrealized_pct(
                snap,
                last_price_fn=_last_price_safe,
            )
        except Exception:
            snap.unrealized_pct = None

        guard_allowed = True
        guard_reason = "ok"
        if guard_allows is not None and intent.kind in entry_kinds:
            try:
                guard_allowed, guard_reason = guard_allows(intent.strategy, intent.symbol, now=now)
            except Exception as e:
                guard_allowed = False
                guard_reason = f"guard_exception:{e}"

        if not guard_allowed:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": guard_reason,
                    "source": "guard_allows",
                }
            )
            continue

        # Decide final notional to send
        final_notional: float = 0.0
        side = intent.side

        # ----- ENTRY / SCALE: enforce caps & loss-zone --------------------------------
        if intent.kind in entry_kinds:
            if intent.notional is None or intent.notional <= 0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "entry_without_notional",
                        "source": "scheduler_v2",
                    }
                )
                continue

            allowed_cap, adjusted_notional, cap_reason = risk_engine.enforce_symbol_cap(
                symbol=intent.symbol,
                strat=intent.strategy,
                pos=snap,
                notional_value=float(intent.notional),
                last_price_fn=_last_price_safe,
                now=now,
            )
            if not allowed_cap or adjusted_notional <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": cap_reason or "blocked_by_symbol_cap",
                        "source": "risk_engine.enforce_symbol_cap",
                    }
                )
                continue

            final_notional = float(adjusted_notional)

            # Loss-zone no-rebuy below threshold (if we have unrealized_pct wired)
            if risk_engine.is_loss_zone_norebuy_block(
                unrealized_pct=snap.unrealized_pct,
                is_entry_side=True,
            ):
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "loss_zone_no_rebuy_below",
                        "source": "risk_engine.is_loss_zone_norebuy_block",
                    }
                )
                continue

        # ----- EXIT / TP / SL: compute notional from position if missing -------------
        else:
            qty_here = float(getattr(pm_pos, "qty", 0.0) or 0.0)
            if abs(qty_here) < 1e-10:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "no_position_to_exit",
                        "source": "scheduler_v2",
                    }
                )
                continue

            px = _last_price_safe(intent.symbol)
            if px <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "no_price_for_exit",
                        "source": "scheduler_v2",
                    }
                )
                continue

            if intent.notional is not None and intent.notional > 0:
                final_notional = float(intent.notional)
            else:
                final_notional = abs(qty_here) * px  # flatten full position

        # If we reached here, we have a valid final_notional
        if final_notional <= 0:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": "non_positive_final_notional",
                    "source": "scheduler_v2",
                }
            )
            continue

                # Ignore dust for entries + TP/SL; still allow generic exits (e.g. daily_flatten)
        if final_notional < MIN_NOTIONAL_USD and intent.kind in {"entry", "scale", "take_profit", "stop_loss"}:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": f"below_min_notional:{final_notional:.4f}<{MIN_NOTIONAL_USD}",
                    "source": "scheduler_v2",
                }
            )
            continue

        action_record: Dict[str, Any] = {
            "symbol": intent.symbol,
            "strategy": intent.strategy,
            "side": side,
            "kind": intent.kind,
            "notional": final_notional,
            "reason": intent.reason,
            "dry": bool(dry),
        }

        if dry or br is None:
            action_record["status"] = "skipped_dry_run"
            actions.append(action_record)
            continue

        # ------------------------------------------------------------------
        # Send to broker via br_router.market_notional
        # ------------------------------------------------------------------
        try:
            resp = br.market_notional(
                symbol=intent.symbol,
                side=side,
                notional=final_notional,
                strategy=intent.strategy,
            )
            action_record["status"] = "sent"
            action_record["response"] = resp
        except Exception as e:
            action_record["status"] = "error"
            action_record["error"] = f"{e.__class__.__name__}: {e}"

        actions.append(action_record)

    # ------------------------------------------------------------------
    # FINAL RETURN — avoid null/None responses
    # ------------------------------------------------------------------
    return {
        "ok": True,
        "dry": bool(dry),
        "config": config_snapshot,
        "actions": actions,
        "telemetry": telemetry,
    }

# ---- New core debug endpoint) ------------------------------------------------------------


# -----------------------
# Background scheduler thread
# -----------------------

_sched_state = {
    "enabled": False,
    "sleep": float(os.getenv("SCHED_SLEEP", "30") or 30),
    "last_run_ts": None,
    "last_ok": None,
    "last_error": None,
    "ticks": 0,
}

def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")

def _scheduler_loop():
    while _sched_state["enabled"]:
        try:
            _sched_state["ticks"] += 1
            resp = scheduler_run_v2(payload={"dry": False})
            _sched_state["last_ok"] = bool(resp.get("ok"))
            _sched_state["last_error"] = None if resp.get("ok") else resp.get("error")
            _sched_state["last_run_ts"] = time.time()
            log.info("scheduler v2 tick #%s ok", _sched_state["ticks"])
        except Exception as e:
            _sched_state["last_ok"] = False
            _sched_state["last_error"] = str(e)
            _sched_state["last_run_ts"] = time.time()
            log.exception("scheduler v2 tick #%s failed: %s", _sched_state["ticks"], e)
        time.sleep(_sched_state["sleep"])

@app.on_event("startup")
def on_startup():
    enabled = _env_bool("SCHED_ENABLED", False)
    _sched_state["enabled"] = enabled
    _sched_state["sleep"] = float(os.getenv("SCHED_SLEEP", "30") or 30)
    if enabled:
        t = threading.Thread(target=_scheduler_loop, daemon=True)
        t.start()
        log.info("Scheduler thread started: sleep=%s enabled=True", _sched_state["sleep"])
    else:
        log.info("Scheduler thread disabled (SCHED_ENABLED=0)")

@app.get("/scheduler/v2/status")
def scheduler_status():
    return {"ok": True, **_sched_state}
