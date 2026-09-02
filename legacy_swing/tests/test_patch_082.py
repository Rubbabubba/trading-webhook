import os
import sys
import types
import unittest
from unittest import mock


def _install_alpaca_stubs():
    alpaca = types.ModuleType("alpaca")
    trading = types.ModuleType("alpaca.trading")
    trading_client = types.ModuleType("alpaca.trading.client")
    trading_requests = types.ModuleType("alpaca.trading.requests")
    trading_enums = types.ModuleType("alpaca.trading.enums")
    data = types.ModuleType("alpaca.data")
    data_historical = types.ModuleType("alpaca.data.historical")
    data_requests = types.ModuleType("alpaca.data.requests")
    data_timeframe = types.ModuleType("alpaca.data.timeframe")
    data_enums = types.ModuleType("alpaca.data.enums")

    class _Dummy:
        def __init__(self, *args, **kwargs):
            pass

    class _EnumLike:
        IEX = "iex"
        SIP = "sip"
        RAW = "raw"
        SPLIT = "split"
        DIVIDEND = "dividend"
        ALL = "all"

    trading_client.TradingClient = _Dummy
    trading_requests.MarketOrderRequest = _Dummy
    trading_enums.OrderSide = _EnumLike
    trading_enums.TimeInForce = _EnumLike
    data_historical.StockHistoricalDataClient = _Dummy
    data_requests.StockLatestTradeRequest = _Dummy
    data_requests.StockBarsRequest = _Dummy
    data_timeframe.TimeFrame = _Dummy
    data_enums.DataFeed = _EnumLike
    data_enums.Adjustment = _EnumLike

    sys.modules.setdefault("alpaca", alpaca)
    sys.modules.setdefault("alpaca.trading", trading)
    sys.modules.setdefault("alpaca.trading.client", trading_client)
    sys.modules.setdefault("alpaca.trading.requests", trading_requests)
    sys.modules.setdefault("alpaca.trading.enums", trading_enums)
    sys.modules.setdefault("alpaca.data", data)
    sys.modules.setdefault("alpaca.data.historical", data_historical)
    sys.modules.setdefault("alpaca.data.requests", data_requests)
    sys.modules.setdefault("alpaca.data.timeframe", data_timeframe)
    sys.modules.setdefault("alpaca.data.enums", data_enums)


_install_alpaca_stubs()
os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")

import app


class TestPatch082(unittest.TestCase):
    def test_execution_visibility_joins_lifecycle_state(self):
        proof = {
            "truth_source": "current_runtime_preview",
            "runtime_symbols": ["CRM"],
            "selected_symbols": ["CRM"],
            "selected_count": 1,
            "planned_count": 1,
            "submitted_count": 0,
            "filled_count": 0,
            "exit_armed_count": 0,
            "items": [
                {
                    "symbol": "CRM",
                    "selected": True,
                    "plan_created": True,
                    "order_submitted": False,
                    "fill_observed": False,
                    "exit_armed": False,
                    "current_stage": "planned",
                }
            ],
        }
        lifecycle = {
            "issue_counts": {"error": 0, "warn": 1},
            "items": [
                {
                    "symbol": "CRM",
                    "execution_state": "planned",
                    "derived_state": "planned",
                    "order_status": None,
                    "position_qty": 0,
                    "execution_updated_utc": "2026-03-22T00:00:00+00:00",
                    "submitted_at": "2026-03-22T00:00:00+00:00",
                    "history_tail": [{"to_state": "planned"}],
                    "issues": [{"code": "example", "severity": "warn"}],
                    "persisted_issue_count": 0,
                }
            ],
        }
        with mock.patch.object(app, "_execution_proof_snapshot", return_value=proof), \
             mock.patch.object(app, "execution_lifecycle_snapshot", return_value=lifecycle):
            snap = app.diagnostics_execution_visibility(limit=5)
        self.assertEqual(snap["selected_count"], 1)
        self.assertEqual(snap["items"][0]["symbol"], "CRM")
        self.assertEqual(snap["items"][0]["derived_state"], "planned")
        self.assertEqual(snap["execution_lifecycle_issue_counts"]["warn"], 1)

    def test_live_readiness_gate_surfaces_env_and_release_blockers(self):
        readiness = {
            "component_ready": True,
            "trade_path_proven": False,
            "same_session_proven": True,
        }
        release = {
            "go_live_eligible": False,
            "live_orders_permitted": False,
            "effective_release_stage": "paper",
            "configured_release_stage": "paper",
            "unmet_conditions": ["recent_market_scan_missing"],
            "worker_status": {"scanner_running": True, "exit_worker_running": True},
            "release_workflow": {
                "approval_armed": False,
                "live_activation_armed": False,
                "promotion_targets": {
                    "guarded_live_eligible": {"ready": False},
                    "live_guarded": {"ready": False},
                },
            },
        }
        proof = {"selected_count": 1, "planned_count": 1, "submitted_count": 0, "filled_count": 0, "exit_armed_count": 0, "selected_symbols": ["CRM"]}
        with mock.patch.object(app, "diagnostics_readiness", return_value=readiness), \
             mock.patch.object(app, "release_gate_status", return_value=release), \
             mock.patch.object(app, "_execution_proof_snapshot", return_value=proof), \
             mock.patch.object(app, "DRY_RUN", True), \
             mock.patch.object(app, "LIVE_TRADING_ENABLED", False), \
             mock.patch.object(app, "SCANNER_ALLOW_LIVE", False), \
             mock.patch.object(app, "RELEASE_GATE_ENFORCED", True):
            snap = app.diagnostics_live_readiness_gate(limit=5)
        self.assertIn("trade_path_not_proven", snap["blockers"])
        self.assertIn("dry_run_enabled", snap["blockers"])
        self.assertIn("recent_market_scan_missing", snap["blockers"])
        self.assertFalse(snap["live_orders_permitted"])


if __name__ == "__main__":
    unittest.main()
