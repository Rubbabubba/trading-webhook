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


class TestPatch083(unittest.TestCase):
    def test_proof_capture_plan_surfaces_gaps_and_next_step(self):
        preview = {
            "preview_source": "current_runtime_env",
            "runtime_symbols": ["CRM", "NET"],
            "selected_symbols": ["CRM"],
            "top_candidates": [
                {"symbol": "CRM", "eligible": True, "rejection_reasons": [], "selection_blockers": [], "rank_score": 55.0},
                {"symbol": "NET", "eligible": False, "rejection_reasons": ["too_far_below_breakout"], "selection_blockers": [], "rank_score": 77.0},
            ],
        }
        visibility = {
            "truth_source": "current_runtime_preview",
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
                    "preview_only": True,
                    "current_stage": "planned",
                    "issues": [{"code": "plan_without_order", "severity": "info"}],
                }
            ],
        }
        live_gate = {
            "component_ready": True,
            "trade_path_proven": False,
            "same_session_proven": False,
            "go_live_eligible": False,
            "effective_release_stage": "paper",
            "blockers": ["dry_run_enabled", "regime_not_favorable"],
            "env": {"dry_run": True, "live_trading_enabled": False, "scanner_allow_live": False},
            "worker_status": {"scanner_running": True, "exit_worker_running": True},
        }
        readiness = {"market_open": True}
        with mock.patch.object(app, "_current_runtime_preview_snapshot", return_value=preview), \
             mock.patch.object(app, "_execution_visibility_snapshot", return_value=visibility), \
             mock.patch.object(app, "_live_readiness_gate_snapshot", return_value=live_gate), \
             mock.patch.object(app, "diagnostics_readiness", return_value=readiness):
            snap = app.diagnostics_proof_capture_plan(limit=5)
        self.assertFalse(snap["proof_capture_possible_now"])
        self.assertIn("dry_run_disabled", snap["arming_gaps"])
        self.assertEqual(snap["items"][0]["next_step"], "disable_dry_run_and_submit_in_paper_session")
        self.assertEqual(snap["top_runtime_candidates"][0]["symbol"], "NET")

    def test_proof_capture_plan_can_show_ready_now(self):
        preview = {"preview_source": "current_runtime_env", "runtime_symbols": ["CRM"], "selected_symbols": ["CRM"], "top_candidates": []}
        visibility = {
            "truth_source": "current_runtime_preview",
            "selected_symbols": ["CRM"],
            "selected_count": 1,
            "planned_count": 0,
            "submitted_count": 1,
            "filled_count": 0,
            "exit_armed_count": 0,
            "items": [{"symbol": "CRM", "selected": True, "plan_created": True, "order_submitted": True, "fill_observed": False, "exit_armed": False, "preview_only": False, "current_stage": "submitted", "issues": []}],
        }
        live_gate = {
            "component_ready": True,
            "trade_path_proven": True,
            "same_session_proven": True,
            "go_live_eligible": False,
            "effective_release_stage": "paper",
            "blockers": [],
            "env": {"dry_run": False, "live_trading_enabled": True, "scanner_allow_live": True},
            "worker_status": {"scanner_running": True, "exit_worker_running": True},
        }
        readiness = {"market_open": True}
        with mock.patch.object(app, "_current_runtime_preview_snapshot", return_value=preview), \
             mock.patch.object(app, "_execution_visibility_snapshot", return_value=visibility), \
             mock.patch.object(app, "_live_readiness_gate_snapshot", return_value=live_gate), \
             mock.patch.object(app, "diagnostics_readiness", return_value=readiness):
            snap = app.diagnostics_proof_capture_plan(limit=5)
        self.assertTrue(snap["proof_capture_possible_now"])
        self.assertEqual(snap["items"][0]["next_step"], "observe_entry_fill")
        self.assertEqual(snap["arming_gaps"], [])


if __name__ == "__main__":
    unittest.main()
