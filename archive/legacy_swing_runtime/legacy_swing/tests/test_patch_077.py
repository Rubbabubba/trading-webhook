import os
import sys
import types
import unittest


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


class TestPatch077(unittest.TestCase):
    def test_stale_fill_is_marked_historical_not_current(self):
        old_ts = (app.datetime.now(app.timezone.utc) - app.timedelta(days=10)).isoformat()
        row = {
            "symbol": "SPY",
            "fill_observed": True,
            "active_position": False,
            "plan_created": False,
            "order_submitted": False,
            "exit_armed": False,
            "entry_event": {"ts_utc": old_ts, "status": "filled"},
            "exit_event": {},
            "latest_decision": {},
            "lifecycle_events": [{"ts_utc": old_ts, "stage": "entry", "status": "filled"}],
            "decisions": [],
        }
        flags = app._proof_row_fill_flags(row)
        self.assertFalse(flags["fill_observed"])
        self.assertTrue(flags["historical_fill_observed"])

    def test_pipeline_guardrails_do_not_flag_stale_fill_without_exit_arm(self):
        old_ts = (app.datetime.now(app.timezone.utc) - app.timedelta(days=10)).isoformat()
        row = {
            "symbol": "SPY",
            "selected": False,
            "plan_created": False,
            "order_submitted": False,
            "fill_observed": True,
            "exit_armed": False,
            "exit_event": {},
            "active_position": False,
            "entry_event": {"ts_utc": old_ts, "status": "filled"},
            "lifecycle_events": [{"ts_utc": old_ts, "stage": "entry", "status": "filled"}],
            "decisions": [],
            "latest_decision": {},
        }
        out = app._pipeline_guardrail_rows([row], truth_source="current_runtime_preview")
        self.assertEqual(out[0]["issues"], [])
        self.assertTrue(out[0]["historical_fill_observed"])
        self.assertFalse(out[0]["fill_observed"])

    def test_recent_fill_still_counts_as_current_fill(self):
        recent_ts = app.datetime.now(app.timezone.utc).isoformat()
        row = {
            "symbol": "SPY",
            "fill_observed": True,
            "active_position": False,
            "plan_created": False,
            "order_submitted": False,
            "exit_armed": False,
            "entry_event": {"ts_utc": recent_ts, "status": "filled"},
            "exit_event": {},
            "latest_decision": {},
            "lifecycle_events": [{"ts_utc": recent_ts, "stage": "entry", "status": "filled"}],
            "decisions": [],
        }
        flags = app._proof_row_fill_flags(row)
        self.assertTrue(flags["fill_observed"])
        self.assertFalse(flags["historical_fill_observed"])


if __name__ == "__main__":
    unittest.main()
