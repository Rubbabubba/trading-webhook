import os
from datetime import datetime, timezone

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _row(symbol, ts, pnl, reason, source, **extra):
    out = {
        "symbol": symbol,
        "strategy_name": extra.pop("strategy_name", "daily_breakout"),
        "ts_utc": ts,
        "gross_pnl": pnl,
        "reason": reason,
        "exit_reason": reason,
        "source": source,
        "qty": extra.pop("qty", 1),
        "entry_price": extra.pop("entry_price", 100),
        "exit_price": extra.pop("exit_price", 90),
    }
    out.update(extra)
    return out


def test_patch_228_loss_cluster_report_dedupes_worker_and_broker_fill(monkeypatch):
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "_session_boundary_snapshot", lambda: {"today_ny": "2026-07-02"})
    perf_state = {
        "closed_trades": [
            _row("INTC", "2026-07-02T13:31:21.377703+00:00", -117.3488, "stall_exit", "worker_exit", qty=8.55),
            _row("INTC", "2026-07-02T13:31:21.259Z", -120.9398, "broker_exit_fill", "alpaca_orders_reconciled", qty=8.55, exit_order_id="exit-intc"),
            _row("AMAT", "2026-07-02T13:33:13.093491+00:00", -120.2648, "stall_exit", "worker_exit", qty=1.66),
            _row("AMAT", "2026-07-02T13:33:12.984Z", -133.9554, "broker_exit_fill", "alpaca_orders_reconciled", qty=1.66, exit_order_id="exit-amat"),
        ],
        "by_strategy": {},
        "kill_switch": {},
    }
    pnl = {
        "account_daily_pnl": -42.0,
        "account_daily_pnl_source": "alpaca_account_equity",
        "broker_orders_today": {
            "ok": True,
            "rows": [
                {"symbol": "INTC", "ts_utc": "2026-07-02T13:31:21.259Z", "qty": 8.55, "entry_price": 140.395, "exit_price": 126.25, "gross_pnl": -120.9398, "calc_ok": True, "exit_order_id": "exit-intc", "entry_order_id": "entry-intc", "source": "alpaca_orders"},
                {"symbol": "AMAT", "ts_utc": "2026-07-02T13:33:12.984Z", "qty": 1.66, "entry_price": 723.2087, "exit_price": 642.5127, "gross_pnl": -133.9554, "calc_ok": True, "exit_order_id": "exit-amat", "entry_order_id": "entry-amat", "source": "alpaca_orders"},
            ],
        },
    }
    halt = {
        "daily_halt_active": False,
        "daily_halt_reason": "none",
        "configured_daily_stop_dollars": 150,
        "configured_daily_loss_limit": 200,
        "account_daily_pnl": -42.0,
    }

    out = app._loss_cluster_report(perf_state=perf_state, pnl_truth=pnl, halt_truth=halt)

    assert out["patch_version"] == "patch-228-loss-cluster-forensics"
    assert out["summary"]["strategy_state_closed_rows"] == 4
    assert out["summary"]["duplicate_pair_count"] == 2
    assert out["summary"]["strategy_state_realized_pnl"] == -492.5088
    assert out["summary"]["duplicate_adjusted_realized_pnl"] == -254.8952
    assert out["summary"]["trust_assessment"] == "strategy_state_duplicate_accounting_suspect"
    assert "dedupe_strategy_state_realized_exit_reporting" in out["recommended_actions"]


def test_patch_228_loss_cluster_report_uses_estimate_for_missing_broker_basis(monkeypatch):
    monkeypatch.setattr(app, "_session_boundary_snapshot", lambda: {"today_ny": "2026-07-02"})
    perf_state = {
        "closed_trades": [
            _row("UBER", "2026-07-02T13:35:40.482280+00:00", -57.6576, "stall_exit", "worker_exit", strategy_name="recovered", qty=15.56, recovered=True),
            _row("UBER", "2026-07-02T13:35:40.372Z", -63.0164, "broker_exit_fill", "alpaca_orders_reconciled", qty=15.56, exit_order_id="exit-uber"),
        ],
        "by_strategy": {},
        "kill_switch": {},
    }
    pnl = {
        "account_daily_pnl": -45.0,
        "broker_orders_today": {
            "ok": True,
            "rows": [
                {"symbol": "UBER", "ts_utc": "2026-07-02T13:35:40.372Z", "qty": 15.56, "entry_price": None, "exit_price": 73.0656, "gross_pnl": 0, "calc_ok": False, "exit_order_id": "exit-uber", "entry_order_id": "", "source": "alpaca_orders"},
            ],
        },
    }
    halt = {
        "daily_halt_active": False,
        "daily_halt_reason": "none",
        "configured_daily_stop_dollars": 150,
        "configured_daily_loss_limit": 200,
        "account_daily_pnl": -45.0,
    }

    out = app._loss_cluster_report(perf_state=perf_state, pnl_truth=pnl, halt_truth=halt)

    assert out["summary"]["missing_broker_entry_basis_count"] == 1
    assert out["summary"]["duplicate_adjusted_realized_pnl"] == -63.0164
    assert out["duplicate_adjusted_economic_exits"][0]["preferred_basis"] == "broker_reconciled_strategy_estimate"
    assert "backfill_missing_broker_entry_basis" in out["recommended_actions"]


def test_patch_228_endpoint_exists(monkeypatch):
    monkeypatch.setattr(app, "require_admin_if_configured", lambda request: None)
    monkeypatch.setattr(app, "_loss_cluster_report", lambda: {"ok": True, "patch_version": app.PATCH_VERSION})

    req = app.Request({"type": "http", "headers": [], "query_string": b"", "method": "GET", "path": "/diagnostics/loss_cluster_report"})
    out = app.diagnostics_loss_cluster_report(req)

    assert out["ok"] is True
    assert out["patch_version"] == "patch-228-loss-cluster-forensics"