import os

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


def _broker_pnl_payload(pnl=-524.1349):
    return {
        "ok": True,
        "account_daily_pnl": -35.0,
        "account_daily_pnl_source": "alpaca_account_equity",
        "broker_orders_today": {
            "ok": True,
            "broker_exit_fills_today": 5,
            "rows": [
                {
                    "symbol": "INTC",
                    "ts_utc": "2026-07-02T13:31:21.259Z",
                    "qty": 1,
                    "entry_price": 100,
                    "exit_price": 100 + pnl,
                    "gross_pnl": pnl,
                    "calc_ok": True,
                    "exit_order_id": "exit-1",
                    "entry_order_id": "entry-1",
                    "source": "alpaca_orders",
                }
            ],
        },
    }


def test_patch_229_realized_closed_trade_halt_blocks_entries(monkeypatch):
    monkeypatch.setattr(app, "_configured_daily_stop_dollars_safe", lambda: 150.0)
    monkeypatch.setattr(app, "_configured_daily_loss_limit_safe", lambda: 200.0)
    monkeypatch.setattr(app, "today_pnl_truth_snapshot", lambda: _broker_pnl_payload())

    out = app._p229_realized_closed_trade_loss_halt()

    assert out["ok"] is True
    assert out["active"] is True
    assert out["new_entries_blocked"] is True
    assert out["exits_still_permitted"] is True
    assert out["bulk_flatten_allowed"] is False
    assert out["realized_closed_trade_pnl"] == -524.1349


def test_patch_229_live_trading_permitted_blocks_worker_scan_only(monkeypatch):
    monkeypatch.setattr(app, "DRY_RUN", False)
    monkeypatch.setattr(app, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(app, "NEW_ENTRIES_ENABLED", True)
    monkeypatch.setattr(app, "SCANNER_ALLOW_LIVE", True)
    monkeypatch.setattr(app, "RELEASE_GATE_ENFORCED", False)
    monkeypatch.setattr(app, "realized_closed_trade_loss_halt_active", lambda pnl_truth=None: True)

    assert app.is_live_trading_permitted("worker_scan") is False
    assert app.is_live_exit_permitted("worker_exit", reason="realized_closed_trade_loss_halt") is True


def test_patch_229_loss_cluster_report_carries_worker_attribution(monkeypatch):
    monkeypatch.setattr(app, "_session_boundary_snapshot", lambda: {"today_ny": "2026-07-02"})
    monkeypatch.setattr(app, "_configured_daily_stop_dollars_safe", lambda: 150.0)
    monkeypatch.setattr(app, "_configured_daily_loss_limit_safe", lambda: 200.0)

    perf_state = {
        "closed_trades": [
            {
                "symbol": "INTC",
                "strategy_name": "daily_breakout",
                "signal": "daily_breakout",
                "ts_utc": "2026-07-02T13:31:21.377703+00:00",
                "qty": 8.55,
                "entry_price": 140.395,
                "exit_price": 126.67,
                "gross_pnl": -117.3488,
                "pnl_r": -3.9158,
                "reason": "stall_exit",
                "exit_reason": "stall_exit",
                "source": "worker_exit",
                "holding_days": 1.7347,
                "recovered": False,
            },
            {
                "symbol": "INTC",
                "strategy_name": "daily_breakout",
                "signal": "daily_breakout",
                "ts_utc": "2026-07-02T13:31:21.259Z",
                "qty": 8.55,
                "entry_price": 140.395,
                "exit_price": 126.25,
                "gross_pnl": -120.9398,
                "reason": "broker_exit_fill",
                "exit_reason": "broker_exit_fill",
                "source": "alpaca_orders_reconciled",
                "exit_order_id": "exit-intc",
                "entry_order_id": "entry-intc",
            },
        ],
        "by_strategy": {},
        "kill_switch": {},
    }
    pnl = {
        "account_daily_pnl": -35.0,
        "broker_orders_today": {
            "ok": True,
            "broker_exit_fills_today": 1,
            "rows": [
                {
                    "symbol": "INTC",
                    "ts_utc": "2026-07-02T13:31:21.259Z",
                    "qty": 8.55,
                    "entry_price": 140.395,
                    "exit_price": 126.25,
                    "gross_pnl": -120.9398,
                    "calc_ok": True,
                    "exit_order_id": "exit-intc",
                    "entry_order_id": "entry-intc",
                    "source": "alpaca_orders",
                }
            ],
        },
    }
    halt = {
        "daily_halt_active": False,
        "daily_halt_reason": "none",
        "configured_daily_stop_dollars": 150,
        "configured_daily_loss_limit": 200,
        "account_daily_pnl": -35.0,
    }

    out = app._loss_cluster_report(perf_state=perf_state, pnl_truth=pnl, halt_truth=halt)
    econ = out["duplicate_adjusted_economic_exits"][0]

    assert econ["gross_pnl"] == -120.9398
    assert econ["preferred_basis"] == "broker_order_calc"
    assert econ["strategy_name"] == "daily_breakout"
    assert econ["reason"] == "stall_exit"
    assert econ["holding_period"] == "1 day"
    assert out["losses_by_strategy"][0]["name"] == "daily_breakout"
    assert out["summary"]["realized_closed_trade_loss_halt_active"] is False


def test_patch_229_daily_halt_truth_reports_realized_closed_trade_halt(monkeypatch):
    pnl = _broker_pnl_payload()
    monkeypatch.setattr(app, "today_pnl_truth_snapshot", lambda: pnl)
    monkeypatch.setattr(app, "_account_daily_pnl_snapshot", lambda: {"ok": True, "account_daily_pnl": -35.0, "source": "unit"})
    monkeypatch.setattr(app, "daily_pnl", lambda: -35.0)
    monkeypatch.setattr(app, "daily_halt_active", lambda: False)
    monkeypatch.setattr(app, "_configured_daily_stop_dollars_safe", lambda: 150.0)
    monkeypatch.setattr(app, "_configured_daily_loss_limit_safe", lambda: 200.0)

    out = app.daily_halt_truth_snapshot()

    assert out["daily_halt_active"] is True
    assert out["daily_halt_reason"] in {
        "realized_closed_trade_daily_stop_breached",
        "realized_closed_trade_daily_loss_breached",
    }
    assert out["realized_closed_trade_loss_halt"]["new_entries_blocked"] is True
    assert out["realized_closed_trade_loss_halt"]["exits_still_permitted"] is True