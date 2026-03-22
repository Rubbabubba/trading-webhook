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


class TestPatch081(unittest.TestCase):
    def test_execution_proof_endpoint_focuses_selected_symbols(self):
        active_scan = {
            "_scan_source": "current_runtime_preview",
            "summary": {"selected_symbols": ["CRM"], "symbols": ["CRM", "NET"]},
        }
        proof = {
            "truth_source": "current_runtime_preview",
            "runtime_symbols": ["CRM", "NET"],
            "selected_symbols": ["CRM"],
            "rows": [
                {
                    "symbol": "CRM",
                    "selected": True,
                    "plan_created": True,
                    "order_submitted": False,
                    "fill_observed": False,
                    "exit_armed": False,
                    "current_stage": "planned",
                    "latest_decision": {"action": "dry_run_plan_created"},
                    "decisions": [
                        {"event": "SCAN", "action": "candidate_selected"},
                        {"event": "ENTRY", "action": "dry_run_plan_created"},
                    ],
                    "lifecycle_events": [],
                }
            ],
        }
        with mock.patch.object(app, "_active_truth_scan", return_value=active_scan), \
             mock.patch.object(app, "_paper_execution_proof_snapshot", return_value=proof):
            snap = app.diagnostics_execution_proof(limit=5)
        self.assertEqual(snap["selected_symbols"], ["CRM"])
        self.assertEqual(snap["planned_count"], 1)
        self.assertEqual(snap["items"][0]["symbol"], "CRM")
        self.assertEqual(snap["items"][0]["selected_event"].get("action"), "candidate_selected")
        self.assertEqual(snap["items"][0]["plan_event"].get("action"), "dry_run_plan_created")

    def test_scan_candidate_selected_is_journal_persisted(self):
        self.assertTrue(app._journal_should_persist("SCAN", "candidate_selected"))


if __name__ == "__main__":
    unittest.main()
