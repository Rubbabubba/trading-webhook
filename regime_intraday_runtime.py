"""Broker-aware orchestration for the isolated, paper-only intraday system."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from intraday_market_data import fetch_minute_bars, fetch_recent_minute_bars
from market_clock import is_regular_market_time, now_ny, parse_hhmm
from regime_intraday import REGIME_INTRADAY_VERSION, RegimeIntradayConfig, evaluate_regime_intraday
from regime_intraday_candidates import failed_breakout_fade_candidate, relative_strength_divergence_candidate, trend_pullback_candidate
from regime_intraday_email import send_exit_email, send_signal_email
from regime_intraday_executor import cancel_order, get_order, get_order_by_client_id, paper_client_order_id, submit_mleg_close_order, submit_mleg_limit_order
from regime_intraday_ledger import load_ledger, paper_submission_decision, pending_candidate, record_broker_order, record_pending_candidate, save_ledger, update_ledger
from regime_intraday_options import fetch_option_chain, select_debit_spread, spread_exit_decision, value_debit_spread
from regime_intraday_readiness import readiness_snapshot
from regime_intraday_replay import chronological_holdout, cost_adjusted_report, mean_reversion_walk_forward, replay_sessions, threshold_sensitivity, walk_forward
from regime_intraday_validation import paper_fill_reconciliation, validation_lab


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or default).strip()


def _bool(name: str, default: bool = False) -> bool:
    return _env(name, str(default)).lower() in {"1", "true", "yes", "y", "on"}


def _int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default))))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


class RegimeIntradayRuntime:
    """Owns volatile scan state and the durable paper lifecycle."""

    def __init__(self) -> None:
        self.last_scan: dict[str, Any] = {}
        self.ledger_path = _env("REGIME_INTRADAY_LEDGER_PATH", "/var/data/regime_intraday_ledger.json")

    def config(self) -> RegimeIntradayConfig:
        trade_symbols = tuple(value.strip().upper() for value in _env("REGIME_INTRADAY_TRADE_SYMBOLS", "SPY").split(",") if value.strip())
        return RegimeIntradayConfig(
            trade_symbols=trade_symbols or ("SPY",), momentum_enabled=_bool("REGIME_INTRADAY_MOMENTUM_ENABLED"),
            mean_reversion_enabled=_bool("REGIME_INTRADAY_MEAN_REVERSION_ENABLED", True),
            opening_range_minutes=max(5, _int("REGIME_INTRADAY_OPENING_RANGE_MINUTES", 30)), min_bars=max(10, _int("REGIME_INTRADAY_MIN_BARS", 40)),
            momentum_volume_ratio=_float("REGIME_INTRADAY_MOMENTUM_VOLUME_RATIO", 1.20), momentum_break_buffer_pct=_float("REGIME_INTRADAY_BREAK_BUFFER_PCT", 0.0003),
            momentum_max_vwap_extension_pct=_float("REGIME_INTRADAY_MAX_VWAP_EXTENSION_PCT", 0.008), trend_efficiency_min=_float("REGIME_INTRADAY_TREND_EFFICIENCY_MIN", 0.34),
            range_efficiency_max=_float("REGIME_INTRADAY_RANGE_EFFICIENCY_MAX", 0.24), mean_reversion_min_vwap_atr=_float("REGIME_INTRADAY_MR_MIN_VWAP_ATR", 1.0),
            mean_reversion_max_vwap_atr=_float("REGIME_INTRADAY_MR_MAX_VWAP_ATR", 2.75), stop_atr=_float("REGIME_INTRADAY_STOP_ATR", 0.75),
            target_r=_float("REGIME_INTRADAY_TARGET_R", 2.0), option_min_dte=max(1, _int("REGIME_INTRADAY_OPTION_MIN_DTE", 7)),
            option_max_dte=max(1, _int("REGIME_INTRADAY_OPTION_MAX_DTE", 21)), option_target_delta_low=_float("REGIME_INTRADAY_OPTION_DELTA_LOW", 0.55),
            option_target_delta_high=_float("REGIME_INTRADAY_OPTION_DELTA_HIGH", 0.70), option_max_spread_pct=_float("REGIME_INTRADAY_OPTION_MAX_SPREAD_PCT", 0.08),
        )

    def dia_config(self) -> RegimeIntradayConfig:
        return replace(self.config(), symbols=("SPY", "DIA"), trade_symbols=("DIA",), momentum_enabled=False, mean_reversion_enabled=True)

    @staticmethod
    def _paper_credentials() -> tuple[str, str]:
        return _env("ALPACA_PAPER_API_KEY_ID"), _env("ALPACA_PAPER_API_SECRET_KEY")

    @staticmethod
    def _worker_authorize(body: dict) -> None:
        expected = _env("WORKER_SECRET")
        if not expected or str(body.get("worker_secret") or "").strip() != expected:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    def _readiness(self) -> dict:
        ledger = load_ledger(self.ledger_path)
        key, secret = self._paper_credentials()
        snapshot = readiness_snapshot(
            config={"paper_submit_enabled": _bool("REGIME_INTRADAY_PAPER_SUBMIT_ENABLED"), "live_enabled": False,
                    "option_feed": _env("REGIME_INTRADAY_OPTION_FEED", "indicative"), "max_scan_age_sec": _int("REGIME_INTRADAY_MAX_SCAN_AGE_SEC", 600),
                    "min_shadow_closed": _int("REGIME_INTRADAY_MIN_SHADOW_CLOSED_FOR_LIVE", 10)},
            ledger=ledger, last_scan=self.last_scan, paper_credentials_present=bool(key and secret),
        )
        snapshot["notifications"] = {"email_enabled": _bool("REGIME_INTRADAY_ALERT_EMAIL_ENABLED", True),
                                     "email_configured": bool(_env("RESEND_API_KEY") and _env("REGIME_INTRADAY_ALERT_EMAIL_TO")), "provider": "resend"}
        return snapshot

    def scan(self) -> dict:
        timestamp = datetime.now(timezone.utc).isoformat()
        cfg = self.config()
        dia_enabled = _bool("REGIME_INTRADAY_DIA_PAPER_ENABLED", True)
        dia_cfg = self.dia_config()
        if _bool("ONLY_MARKET_HOURS", True) and not is_regular_market_time():
            self.last_scan = {"ok": True, "version": REGIME_INTRADAY_VERSION, "status": "skipped_outside_market_hours", "ts_utc": timestamp,
                              "paper_only": True, "live_submission": False, "symbols": list(cfg.symbols)}
            return self.last_scan
        requested_symbols = list(dict.fromkeys([*cfg.symbols, *(dia_cfg.symbols if dia_enabled else ())]))
        bars, fetch = fetch_recent_minute_bars(requested_symbols)
        today = now_ny().date()
        regular = {symbol: [row for row in bars.get(symbol, []) if row["ts_ny"].date() == today and is_regular_market_time(row["ts_ny"])] for symbol in requested_symbols}
        if fetch.get("error"):
            raise HTTPException(status_code=502, detail={"message": "market data unavailable", "fetch": fetch})
        payload = evaluate_regime_intraday(regular, cfg)
        primary = payload
        dia_scan = evaluate_regime_intraday(regular, dia_cfg) if dia_enabled else {"signals": [], "features": {}, "regime": {"name": "disabled"}}
        payload = {
            **primary,
            "signals": [*list(primary.get("signals") or []), *list(dia_scan.get("signals") or [])],
            "signal_count": len(list(primary.get("signals") or [])) + len(list(dia_scan.get("signals") or [])),
            "features": {**dict(primary.get("features") or {}), "DIA": dict(dict(dia_scan.get("features") or {}).get("DIA") or {})},
            "sleeves": {
                "spy_mean_reversion": {"execution": "paper", "regime": primary.get("regime"), "signal_count": len(list(primary.get("signals") or []))},
                "dia_mean_reversion": {"execution": "paper" if dia_enabled else "disabled", "regime": dia_scan.get("regime"), "signal_count": len(list(dia_scan.get("signals") or []))},
            },
        }
        payload.update({"status": "completed", "ts_utc": timestamp, "paper_only": True, "live_submission": False, "market_data": fetch})
        plans = []
        key = _env("APCA_API_KEY_ID") or self._paper_credentials()[0]
        secret = _env("APCA_API_SECRET_KEY") or self._paper_credentials()[1]
        if _bool("REGIME_INTRADAY_OPTION_CHAIN_ENABLED", True):
            chains: dict[tuple, dict] = {}
            for signal in payload.get("signals", []):
                symbol = str(signal.get("symbol") or "").upper()
                try:
                    intent = dict(signal.get("option_intent") or {})
                    chain_key = (symbol, intent.get("option_type"), intent.get("min_dte"), intent.get("max_dte"))
                    if chain_key not in chains:
                        chains[chain_key] = fetch_option_chain(key, secret, symbol, feed=_env("REGIME_INTRADAY_OPTION_FEED", "indicative"), timeout=max(5, _int("REGIME_INTRADAY_OPTION_CHAIN_TIMEOUT_SEC", 20)), intent=intent)
                    plan = select_debit_spread(chains[chain_key], intent, max_loss_dollars=_float("REGIME_INTRADAY_MAX_TRADE_LOSS_DOLLARS", 100), width=_float("REGIME_INTRADAY_SPREAD_WIDTH", 1))
                except Exception as exc:
                    plan = {"status": "chain_error", "detail": str(exc)[:300], "live_submission": False}
                plans.append({"signal": {"signal_id": signal.get("signal_id"), "symbol": symbol, "strategy": signal.get("strategy")}, "plan": plan})
        payload["option_plans"] = plans
        payload["execution_gate"] = {"paper_enabled": True, "paper_submit_enabled": _bool("REGIME_INTRADAY_PAPER_SUBMIT_ENABLED"),
                                     "live_enabled": False, "live_submission": False, "max_trade_loss_dollars": _float("REGIME_INTRADAY_MAX_TRADE_LOSS_DOLLARS", 100),
                                     "max_daily_loss_dollars": _float("REGIME_INTRADAY_MAX_DAILY_LOSS_DOLLARS", 200)}
        ledger = update_ledger(load_ledger(self.ledger_path), payload, max_open_positions=max(1, _int("REGIME_INTRADAY_MAX_OPEN_POSITIONS", 1)),
                               max_daily_loss_r=_float("REGIME_INTRADAY_MAX_DAILY_LOSS_R", 2), ts_utc=timestamp)
        expires = (datetime.fromisoformat(timestamp) + timedelta(seconds=max(60, _int("REGIME_INTRADAY_CANDIDATE_TTL_SEC", 600)))).isoformat()
        signals_by_id = {str(row.get("signal_id")): row for row in payload.get("signals", [])}
        for row in plans:
            signal_id = str(row["signal"].get("signal_id") or "")
            row["approval_expires_at"] = expires
            record_pending_candidate(ledger, dict(signals_by_id.get(signal_id) or {}), dict(row.get("plan") or {}), ts_utc=timestamp, expires_at=expires)
        sent = {str(event.get("signal_id") or "") for event in ledger.get("events", []) if event.get("event") == "signal_email_sent"}
        alerts = []
        for row in plans:
            signal_id = str(row["signal"].get("signal_id") or "")
            plan = dict(row.get("plan") or {})
            if plan.get("status") != "selected" or not signal_id or signal_id in sent:
                continue
            signal = dict(signals_by_id.get(signal_id) or {})
            signal["approval_expires_at"] = expires
            try:
                result = send_signal_email(api_key=_env("RESEND_API_KEY") if _bool("REGIME_INTRADAY_ALERT_EMAIL_ENABLED", True) else "", to_email=_env("REGIME_INTRADAY_ALERT_EMAIL_TO"),
                                           from_email=_env("REGIME_INTRADAY_ALERT_EMAIL_FROM", "Trading System <onboarding@resend.dev>"), signal=signal, plan=plan)
            except Exception as exc:
                result = {"sent": False, "reason": "provider_error", "detail": str(exc)[:200]}
            alerts.append({"signal_id": signal_id, **result})
            if result.get("sent"):
                ledger.setdefault("events", []).append({"event": "signal_email_sent", "signal_id": signal_id, "message_id": result.get("message_id"), "ts_utc": timestamp})
        payload["email_alerts"] = alerts
        payload["paper_ledger"] = dict(ledger.get("summary") or {})
        save_ledger(self.ledger_path, ledger)
        self.last_scan = payload
        return payload

    def scan_worker(self, body: dict) -> dict:
        self._worker_authorize(body)
        payload = self.scan()
        auto_submission = None
        if payload.get("status") == "completed" and _bool("REGIME_INTRADAY_PAPER_AUTO_SUBMIT", True):
            for row in list(payload.get("option_plans") or []):
                plan = dict(row.get("plan") or {})
                signal_id = str(dict(row.get("signal") or {}).get("signal_id") or "")
                if plan.get("status") != "selected" or not signal_id:
                    continue
                try:
                    auto_submission = self.paper_roundtrip({**body, "signal_id": signal_id})
                except HTTPException as exc:
                    auto_submission = {"ok": False, "status_code": exc.status_code, "detail": exc.detail, "signal_id": signal_id}
                break
        payload["paper_auto_submit_enabled"] = _bool("REGIME_INTRADAY_PAPER_AUTO_SUBMIT", True)
        payload["auto_submission"] = auto_submission
        return payload

    def ledger_payload(self) -> dict:
        ledger = load_ledger(self.ledger_path)
        return {"ok": True, "summary": dict(ledger.get("summary") or {}), "open": dict(ledger.get("open") or {}), "closed": list(ledger.get("closed") or [])[-50:],
                "events": list(ledger.get("events") or [])[-100:], "orders": dict(ledger.get("orders") or {}), "pending_candidates": dict(ledger.get("pending_candidates") or {}),
                "execution_quality": paper_fill_reconciliation(ledger), "last_scan": dict(self.last_scan), "live_submission": False}

    def readiness_payload(self) -> dict:
        return {"ok": True, **self._readiness(), "hard_controls": {"max_open_positions": _int("REGIME_INTRADAY_MAX_OPEN_POSITIONS", 1),
                "max_trades_per_day": _int("REGIME_INTRADAY_MAX_TRADES_PER_DAY", 2), "max_consecutive_losses": _int("REGIME_INTRADAY_MAX_CONSECUTIVE_LOSSES", 2),
                "max_trade_loss_dollars": _float("REGIME_INTRADAY_MAX_TRADE_LOSS_DOLLARS", 100), "max_daily_loss_dollars": _float("REGIME_INTRADAY_MAX_DAILY_LOSS_DOLLARS", 200),
                "latest_entry_time_ny": _env("REGIME_INTRADAY_LATEST_ENTRY_TIME_NY", "15:30"), "forced_exit_time_ny": _env("REGIME_INTRADAY_FORCED_EXIT_TIME_NY", "15:45")}}

    def dashboard_payload(self) -> dict:
        ledger = load_ledger(self.ledger_path)
        return {"scan": dict(self.last_scan), "ledger": ledger, "readiness": self._readiness(),
                "scanner": {"last_event": "regime_intraday_scan", "last_status": self.last_scan.get("status"), "last_success_utc": self.last_scan.get("ts_utc"), "consecutive_failures": 0}}

    def replay(self, body: dict) -> dict:
        days = max(7, min(60, int(body.get("calendar_days") or 28)))
        end = datetime.now(timezone.utc)
        bars, fetch = fetch_minute_bars(["SPY", "QQQ"], start=end - timedelta(days=days), end=end)
        regular = {symbol: [row for row in rows if is_regular_market_time(row["ts_ny"])] for symbol, rows in bars.items()}
        if fetch.get("error") or not all(regular.get(symbol) for symbol in ("SPY", "QQQ")):
            raise HTTPException(status_code=502, detail={"message": "historical bars unavailable", "fetch": fetch})
        cfg = self.config()
        result = {"ok": True, "mode": "historical_underlying_no_order_transport", "fetch": fetch,
                  "baseline": replay_sessions(regular, cfg, max_trades_per_day=max(1, _int("REGIME_INTRADAY_MAX_TRADES_PER_DAY", 2)))}
        if body.get("sensitivity"):
            result["sensitivity"] = threshold_sensitivity(regular, cfg)[:20]
        if body.get("walk_forward"):
            result["walk_forward"] = walk_forward(regular, cfg)
        return result

    def after_hours_replay(self, body: dict) -> dict:
        self._worker_authorize(body)
        days = max(30, min(252, int(body.get("calendar_days") or 180)))
        end = datetime.now(timezone.utc)
        research_symbols = ["SPY", "QQQ", "IWM", "DIA"]
        bars, fetch = fetch_minute_bars(research_symbols, start=end - timedelta(days=days), end=end, max_pages=40)
        regular = {symbol: [row for row in rows if is_regular_market_time(row["ts_ny"])] for symbol, rows in bars.items()}
        if fetch.get("error") or not all(regular.get(symbol) for symbol in ("SPY", "QQQ")):
            raise HTTPException(status_code=502, detail={"message": "historical bars unavailable", "fetch": fetch})
        cfg = self.config()
        variants = {
            "configured": replay_sessions(regular, cfg),
            "regime_routed_both": replay_sessions(regular, replace(cfg, momentum_enabled=True, mean_reversion_enabled=True)),
            "opening_range_momentum_only": replay_sessions(regular, replace(cfg, momentum_enabled=True, mean_reversion_enabled=False)),
            "vwap_mean_reversion_only": replay_sessions(regular, replace(cfg, momentum_enabled=False, mean_reversion_enabled=True)),
        }
        risk = _float("REGIME_INTRADAY_MAX_TRADE_LOSS_DOLLARS", 100)
        cost_r = _float("REGIME_INTRADAY_REPLAY_ROUND_TRIP_COST_R", 0.12)
        summaries = {name: {key: value for key, value in report.items() if key != "trades"} | {"cost_adjusted": cost_adjusted_report(report, risk_dollars=risk, round_trip_cost_r=cost_r)} for name, report in variants.items()}
        ranking = sorted(summaries, key=lambda name: float(dict(summaries[name].get("cost_adjusted") or {}).get("net_average_r") or -999), reverse=True)
        walk = mean_reversion_walk_forward(regular, cfg, risk_dollars=risk, round_trip_cost_r=cost_r)
        instruments = {
            "spy_only": replay_sessions(regular, replace(cfg, trade_symbols=("SPY",), momentum_enabled=False, mean_reversion_enabled=True)),
            "qqq_only": replay_sessions(regular, replace(cfg, trade_symbols=("QQQ",), momentum_enabled=False, mean_reversion_enabled=True)),
            "spy_qqq_shared_limits": replay_sessions(regular, replace(cfg, trade_symbols=("SPY", "QQQ"), momentum_enabled=False, mean_reversion_enabled=True)),
            "iwm_with_spy_confirmation": replay_sessions(regular, replace(cfg, symbols=("SPY", "IWM"), trade_symbols=("IWM",), momentum_enabled=False, mean_reversion_enabled=True)),
            "dia_with_spy_confirmation": replay_sessions(regular, replace(cfg, symbols=("SPY", "DIA"), trade_symbols=("DIA",), momentum_enabled=False, mean_reversion_enabled=True)),
        }
        candidate_reports = {
            "trend_pullback": replay_sessions(regular, replace(cfg, trade_symbols=("SPY",), momentum_enabled=False, mean_reversion_enabled=False), evaluator=trend_pullback_candidate),
            "failed_breakout_fade": replay_sessions(regular, replace(cfg, trade_symbols=("SPY",), momentum_enabled=False, mean_reversion_enabled=False), evaluator=failed_breakout_fade_candidate),
            "relative_strength_divergence": replay_sessions(regular, replace(cfg, trade_symbols=("SPY",), momentum_enabled=False, mean_reversion_enabled=False), evaluator=relative_strength_divergence_candidate),
        }
        dia_cfg = replace(cfg, symbols=("SPY", "DIA"), trade_symbols=("DIA",), momentum_enabled=False, mean_reversion_enabled=True)
        dia_holdout = chronological_holdout(regular, dia_cfg, risk_dollars=risk)
        output = {"ok": True, "generated_utc": datetime.now(timezone.utc).isoformat(), "calendar_days": days, "paper_only": True, "live_submission": False,
                  "cost_model": {"risk_dollars": risk, "round_trip_cost_r": cost_r}, "ranking": ranking, "variants": summaries,
                  "mean_reversion_walk_forward": walk,
                  "validation_lab": validation_lab(baseline=variants["configured"], walk_forward=walk, instrument_reports=instruments, candidate_reports=candidate_reports, risk_dollars=risk) | {"dia_fixed_holdout": dia_holdout}}
        save_ledger(_env("REGIME_INTRADAY_AFTER_HOURS_REPORT_PATH", "/var/data/regime_intraday_after_hours_report.json"), output)
        return output

    def paper_roundtrip(self, body: dict) -> dict:
        self._worker_authorize(body)
        if not _bool("REGIME_INTRADAY_PAPER_SUBMIT_ENABLED"):
            raise HTTPException(status_code=409, detail="paper submission gate is closed")
        if now_ny().time() >= parse_hhmm(_env("REGIME_INTRADAY_LATEST_ENTRY_TIME_NY", "15:30")):
            raise HTTPException(status_code=409, detail="paper entry window is closed for the session")
        ledger = load_ledger(self.ledger_path)
        signal_id = str(body.get("signal_id") or "").strip()
        durable = pending_candidate(ledger, signal_id, now_utc=datetime.now(timezone.utc).isoformat())
        if not durable:
            raise HTTPException(status_code=409, detail="no fresh selected option spread is available")
        decision = paper_submission_decision(ledger, signal_id, session=now_ny().date().isoformat(), max_trades_per_day=max(1, _int("REGIME_INTRADAY_MAX_TRADES_PER_DAY", 2)),
                                             max_consecutive_losses=max(1, _int("REGIME_INTRADAY_MAX_CONSECUTIVE_LOSSES", 2)),
                                             max_daily_loss_dollars=_float("REGIME_INTRADAY_MAX_DAILY_LOSS_DOLLARS", 200))
        if not decision.get("allowed"):
            raise HTTPException(status_code=409, detail=decision)
        key, secret = self._paper_credentials()
        client_order_id = paper_client_order_id(signal_id)
        try:
            result = submit_mleg_limit_order(key, secret, dict(durable.get("plan") or {}), paper=True, live_enabled=False, client_order_id=client_order_id)
        except Exception as exc:
            try:
                recovered = get_order_by_client_id(key, secret, client_order_id, paper=True)
                if not recovered.get("id"):
                    raise ValueError("accepted order not found")
                result = {"submitted": True, "recovered_after_transport_error": True, "paper": True, "order_id": recovered.get("id"), "client_order_id": client_order_id, "status": recovered.get("status"), "order_class": recovered.get("order_class")}
            except Exception:
                raise HTTPException(status_code=502, detail=f"paper order rejected or unrecoverable: {str(exc)[:300]}") from exc
        record_broker_order(ledger, signal_id, {**result, "plan": dict(durable.get("plan") or {})})
        ledger["pending_candidates"][signal_id]["status"] = "paper_order_submitted"
        save_ledger(self.ledger_path, ledger)
        return {"ok": True, "mode": "alpaca_paper_roundtrip", "risk_decision": decision, "result": result, "signal_id": signal_id, "live_submission": False}

    def paper_reconcile(self, body: dict) -> dict:
        self._worker_authorize(body)
        key, secret = self._paper_credentials()
        ledger = load_ledger(self.ledger_path)
        alerted = {str(event.get("signal_id") or "") for event in ledger.get("events", []) if event.get("event") == "paper_exit_email_sent"}
        refreshed = []
        for signal_id, record in dict(ledger.get("orders") or {}).items():
            order_id = str(record.get("order_id") or "")
            if not order_id:
                continue
            try:
                close_id = str(dict(record.get("close_order") or {}).get("order_id") or "")
                broker = get_order(key, secret, close_id or order_id, paper=True)
                status = str(broker.get("status") or "").lower()
                if close_id:
                    record.setdefault("close_order", {}).update({"status": status, "broker": broker, "reconciled_at": datetime.now(timezone.utc).isoformat()})
                    record["status"] = "filled_closed" if status == "filled" else "close_submitted"
                    if status == "partially_filled":
                        record["status"] = "close_requires_attention"
                    if status in {"rejected", "canceled", "expired"}:
                        # Never replace a partially filled or unverified close with a full spread.
                        if str(broker.get("filled_qty")) not in {"0", "0.0"} or int(record.get("close_attempt", 1)) >= 3:
                            record["status"] = "close_requires_attention"
                        else:
                            record.setdefault("close_history", []).append(record.pop("close_order"))
                            record["close_retry_required"] = True
                            record["status"] = "close_retry_pending"
                        refreshed.append({"signal_id": signal_id, "status": record["status"]})
                        continue
                    if status in {"new", "accepted", "pending_new"}:
                        submitted = datetime.fromisoformat(str(broker.get("submitted_at") or record["close_order"].get("requested_at") or "").replace("Z", "+00:00"))
                        if (datetime.now(timezone.utc) - submitted).total_seconds() >= 120:
                            cancel_order(key, secret, close_id, paper=True)
                            record["status"] = "close_cancel_requested"
                            # Wait for broker-confirmed cancellation before repricing next cycle.
                    if status == "filled":
                        record["closed_at"] = broker.get("filled_at") or datetime.now(timezone.utc).isoformat()
                        entry_fill = abs(float(dict(record.get("broker") or {}).get("filled_avg_price") or dict(record.get("plan") or {}).get("limit_debit") or 0))
                        close_fill = abs(float(broker.get("filled_avg_price") or dict(record.get("valuation") or {}).get("liquidation_credit") or 0))
                        actual_realized = round((close_fill - entry_fill) * 100, 2) if entry_fill and close_fill else record.get("estimated_realized_dollars")
                        record["actual_realized_dollars"] = actual_realized
                        closed = list(ledger.get("closed") or [])
                        if not any(str(row.get("paper_signal_id") or "") == signal_id for row in closed):
                            closed.append({
                                "paper_signal_id": signal_id,
                                "session": record.get("session"),
                                "exit_ts_utc": record["closed_at"],
                                "exit_reason": dict(record.get("close_order") or {}).get("reason"),
                                "realized_dollars": actual_realized,
                                "status": "filled_closed",
                            })
                            ledger["closed"] = closed[-500:]
                            ledger.setdefault("events", []).append({"event": "paper_roundtrip_closed", "signal_id": signal_id, "order_id": close_id, "realized_dollars": actual_realized, "ts_utc": record["closed_at"]})
                    refreshed.append({"signal_id": signal_id, "order_id": close_id, "status": record["status"], "action": "close"})
                    continue
                record.update({"status": status, "broker": broker, "reconciled_at": datetime.now(timezone.utc).isoformat()})
                if status in {"new", "accepted", "pending_new", "partially_filled"} and _bool("REGIME_INTRADAY_PAPER_AUTO_CANCEL_STALE"):
                    submitted = datetime.fromisoformat(str(broker.get("submitted_at") or broker.get("created_at") or "").replace("Z", "+00:00"))
                    if (datetime.now(timezone.utc) - submitted).total_seconds() >= max(30, _int("REGIME_INTRADAY_STALE_ENTRY_SEC", 120)):
                        cancel_order(key, secret, order_id, paper=True)
                        record["status"] = "cancel_requested"
                if status == "filled" and isinstance(record.get("plan"), dict):
                    plan = dict(record["plan"])
                    chain = fetch_option_chain(key, secret, str(plan.get("underlying") or ""), feed=_env("REGIME_INTRADAY_OPTION_FEED", "indicative"), expiration=plan.get("expiration"))
                    valuation = value_debit_spread(chain, plan)
                    current = now_ny()
                    decision = spread_exit_decision(plan, valuation, minutes_to_close=max(0, 960 - current.hour * 60 - current.minute),
                                                    take_profit_fraction=_float("REGIME_INTRADAY_TAKE_PROFIT_FRACTION", .5), stop_loss_fraction=_float("REGIME_INTRADAY_STOP_LOSS_FRACTION", .5))
                    if record.get("close_retry_required"):
                        decision = {"exit": True, "reason": "close_retry"}
                    record.update({"valuation": valuation, "exit_decision": decision})
                    if valuation.get("status") != "valued" and (current.hour * 60 + current.minute >= 945 or record.get("close_retry_required")):
                        record["status"] = "close_requires_attention"
                        record["exit_decision"] = {"exit": True, "reason": "exit_quote_unavailable"}
                        refreshed.append({"signal_id": signal_id, "status": "close_requires_attention"})
                        continue
                    if decision.get("exit") and signal_id not in alerted:
                        try:
                            result = send_exit_email(api_key=_env("RESEND_API_KEY") if _bool("REGIME_INTRADAY_ALERT_EMAIL_ENABLED", True) else "", to_email=_env("REGIME_INTRADAY_ALERT_EMAIL_TO"),
                                                     from_email=_env("REGIME_INTRADAY_ALERT_EMAIL_FROM", "Trading System <onboarding@resend.dev>"), signal_id=signal_id, record=record)
                        except Exception:
                            result = {"sent": False}
                            ledger.setdefault("events", []).append({"event": "paper_exit_email_failed", "signal_id": signal_id, "ts_utc": datetime.now(timezone.utc).isoformat()})
                        if result.get("sent"):
                            ledger.setdefault("events", []).append({"event": "paper_exit_email_sent", "signal_id": signal_id, "message_id": result.get("message_id"), "ts_utc": datetime.now(timezone.utc).isoformat()})
                    if decision.get("exit") and _bool("REGIME_INTRADAY_PAPER_AUTO_EXIT", True) and not record.get("close_order"):
                        credit = float(valuation.get("liquidation_credit") or 0)
                        if credit > 0:
                            attempt = len(record.get("close_history") or []) + 1
                            close_client_id = paper_client_order_id(f"close:{signal_id}" if attempt == 1 else f"close:{signal_id}:retry:{attempt}")
                            try:
                                close = submit_mleg_close_order(key, secret, plan, credit, paper=True, live_enabled=False, client_order_id=close_client_id)
                            except Exception as close_error:
                                recovered = get_order_by_client_id(key, secret, close_client_id, paper=True)
                                if not recovered.get("id"):
                                    raise close_error
                                close = {"submitted": True, "recovered_after_transport_error": True, "paper": True, "order_id": recovered.get("id"), "client_order_id": close_client_id, "status": recovered.get("status"), "order_class": recovered.get("order_class"), "action": "close"}
                            record["close_order"] = {**close, "reason": decision.get("reason"), "requested_at": datetime.now(timezone.utc).isoformat()}
                            record["close_attempt"] = attempt
                            record["close_retry_required"] = False
                            record["estimated_realized_dollars"] = round((credit - float(plan.get("limit_debit") or 0)) * 100, 2)
                            record["status"] = "close_submitted"
                            ledger.setdefault("events", []).append({"event": "paper_auto_close_recorded", "signal_id": signal_id, "order_id": close.get("order_id"), "reason": decision.get("reason"), "ts_utc": datetime.now(timezone.utc).isoformat()})
                refreshed.append({"signal_id": signal_id, "order_id": order_id, "status": record.get("status")})
            except Exception as exc:
                refreshed.append({"signal_id": signal_id, "order_id": order_id, "status": "reconcile_error", "detail": str(exc)[:200]})
        save_ledger(self.ledger_path, ledger)
        return {"ok": True, "refreshed": refreshed, "live_submission": False, "automatic_exit_submission": _bool("REGIME_INTRADAY_PAPER_AUTO_EXIT", True)}

    def paper_close(self, body: dict) -> dict:
        self._worker_authorize(body)
        if str(body.get("confirm") or "") != "SUBMIT_PAPER_CLOSE":
            raise HTTPException(status_code=409, detail="explicit paper-close confirmation is required")
        signal_id = str(body.get("signal_id") or "").strip()
        ledger = load_ledger(self.ledger_path)
        record = dict(dict(ledger.get("orders") or {}).get(signal_id) or {})
        if str(record.get("status") or "").lower() != "filled" or not isinstance(record.get("plan"), dict):
            raise HTTPException(status_code=409, detail="a filled recorded paper spread is required")
        if not dict(record.get("exit_decision") or {}).get("exit") or record.get("close_order"):
            raise HTTPException(status_code=409, detail="a fresh unsubmitted exit decision is required")
        credit = float(dict(record.get("valuation") or {}).get("liquidation_credit") or 0)
        if credit <= 0:
            raise HTTPException(status_code=409, detail="no positive executable closing credit is available")
        key, secret = self._paper_credentials()
        try:
            close_client_id = paper_client_order_id(f"close:{signal_id}")
            close = submit_mleg_close_order(key, secret, dict(record["plan"]), credit, paper=True, live_enabled=False, client_order_id=close_client_id)
        except Exception as exc:
            try:
                recovered = get_order_by_client_id(key, secret, close_client_id, paper=True)
                if not recovered.get("id"):
                    raise ValueError("accepted close not found")
                close = {"submitted": True, "recovered_after_transport_error": True, "paper": True, "order_id": recovered.get("id"), "client_order_id": close_client_id, "status": recovered.get("status"), "order_class": recovered.get("order_class"), "action": "close"}
            except Exception:
                raise HTTPException(status_code=502, detail=f"paper close rejected or unrecoverable: {str(exc)[:300]}") from exc
        record["close_order"] = {**close, "reason": record["exit_decision"].get("reason"), "requested_at": datetime.now(timezone.utc).isoformat()}
        record["status"] = "close_submitted"
        ledger.setdefault("orders", {})[signal_id] = record
        ledger.setdefault("events", []).append({"event": "paper_close_recorded", "signal_id": signal_id, "order_id": close.get("order_id"), "ts_utc": datetime.now(timezone.utc).isoformat()})
        save_ledger(self.ledger_path, ledger)
        return {"ok": True, "mode": "alpaca_paper_close", "signal_id": signal_id, "result": close, "live_submission": False}
