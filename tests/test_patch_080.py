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


class TestPatch080(unittest.TestCase):
    def test_trade_path_snapshot_uses_proof_before_coverage(self):
        active_scan = {
            "ts_utc": "2026-03-21T00:00:00+00:00",
            "_scan_source": "current_runtime_preview",
            "summary": {
                "symbols": ["CRM"],
                "top_candidates": [{"symbol": "CRM"}],
                "candidates_total": 1,
                "eligible_total": 1,
                "selected_total": 1,
                "global_block_reasons": [],
            },
        }
        proof = {
            "selected_symbols": ["CRM"],
            "planned_symbols": ["CRM"],
            "stage_failures": [],
            "rows": [{"symbol": "CRM", "selected": True, "plan_created": True}],
        }
        with mock.patch.object(app, "_active_truth_scan", return_value=active_scan), \
             mock.patch.object(app, "_paper_execution_proof_snapshot", return_value=proof), \
             mock.patch.object(app, "_paper_lifecycle_counts", return_value={"candidate_selected": 0, "entry_events": 0, "exit_events": 0}), \
             mock.patch.object(app, "build_reconcile_snapshot", return_value={"trading_blocked": False}):
            snap = app._trade_path_snapshot(limit=10)
        self.assertTrue(snap["coverage"]["selected_candidates_present"])
        self.assertTrue(snap["coverage"]["entry_events_present"])
        self.assertEqual(snap["stage_failures"], [])
        self.assertEqual(snap["lifecycle_rows"][0]["symbol"], "CRM")

    def test_runtime_preview_alias_matches_current_runtime_preview(self):
        preview = {"runtime_symbols": ["CRM"], "selected_symbols": ["CRM"], "healthy": True}
        with mock.patch.object(app, "_current_runtime_preview_snapshot", return_value=preview):
            direct = app.diagnostics_current_runtime_preview(limit=5)
            alias = app.diagnostics_runtime_preview(limit=5)
        self.assertEqual(direct, alias)
        self.assertEqual(alias["selected_symbols"], ["CRM"])


if __name__ == "__main__":
    unittest.main()
