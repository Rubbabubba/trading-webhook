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
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


class TestPatch079(unittest.TestCase):
    def test_current_runtime_truth_snapshot_emits_preview_plan_for_selected_symbol(self):
        preview = {
            "ts_utc": "2026-03-22T00:00:00+00:00",
            "runtime_symbols": ["CRM", "NET"],
            "selected_symbols": ["CRM"],
            "top_candidates": [
                {
                    "symbol": "CRM",
                    "strategy": "daily_breakout",
                    "signal": "daily_breakout",
                    "side": "buy",
                    "eligible": True,
                    "estimated_qty": 5.5,
                    "close": 200.0,
                    "rank_score": 73.8,
                    "breakout_level": 210.0,
                    "stop_price": 195.0,
                    "target_price": 215.0,
                    "risk_per_share": 5.0,
                    "rejection_reasons": [],
                }
            ],
            "regime": {"favorable": False},
            "global_block_reasons": [],
            "remaining_new_entries_today": 1,
            "max_new_entries_effective": 1,
            "regime_mode": "defensive",
            "mode_thresholds": {"allow_entries_when_regime_unfavorable": True},
            "eligible_but_not_selected": [],
        }
        with mock.patch.object(app, "_current_runtime_preview_snapshot", return_value=preview):
            snap = app._current_runtime_truth_snapshot(limit=10)
        self.assertEqual(snap["_scan_source"], "current_runtime_preview")
        self.assertIn("CRM", snap["preview_plans"])
        self.assertTrue(snap["preview_plans"]["CRM"]["preview_only"])
        self.assertEqual(snap["summary"]["selected_symbols"], ["CRM"])

    def test_paper_execution_proof_uses_preview_plan_for_selected_symbol(self):
        active_scan = {
            "_scan_source": "current_runtime_preview",
            "summary": {
                "symbols": ["CRM"],
                "selected_symbols": ["CRM"],
                "top_candidates": [
                    {"symbol": "CRM", "eligible": True, "rank_score": 73.8, "selection_blockers": [], "rejection_reasons": []}
                ],
                "preview_plans": {
                    "CRM": {
                        "symbol": "CRM",
                        "preview_only": True,
                        "active": False,
                        "order_id": "",
                    }
                },
            },
            "preview_plans": {
                "CRM": {
                    "symbol": "CRM",
                    "preview_only": True,
                    "active": False,
                    "order_id": "",
                }
            },
        }
        with mock.patch.object(app, "_active_truth_scan", return_value=active_scan), \
             mock.patch.object(app, "get_position", return_value=(0.0, "flat")), \
             mock.patch.object(app, "_derive_execution_lifecycle_state", return_value={"state": None, "order_status": None, "position_qty": 0, "issues": []}):
            proof = app._paper_execution_proof_snapshot(limit=10)
        rows = {row["symbol"]: row for row in proof["rows"]}
        self.assertIn("CRM", rows)
        self.assertTrue(rows["CRM"]["selected"])
        self.assertTrue(rows["CRM"]["plan_created"])
        self.assertIn("CRM", proof["planned_symbols"])


if __name__ == "__main__":
    unittest.main()
