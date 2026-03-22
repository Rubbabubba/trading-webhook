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


class TestPatch076(unittest.TestCase):
    def test_latest_regular_session_bars_use_most_recent_market_day(self):
        fri1 = app.datetime(2026, 3, 20, 15, 55, tzinfo=app.timezone.utc)  # 11:55 NY
        fri2 = app.datetime(2026, 3, 20, 15, 56, tzinfo=app.timezone.utc)
        sat = app.datetime(2026, 3, 21, 15, 55, tzinfo=app.timezone.utc)
        bars = [
            {"ts_utc": fri1, "ts_ny": fri1.astimezone(app.NY_TZ), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "vwap": 1},
            {"ts_utc": fri2, "ts_ny": fri2.astimezone(app.NY_TZ), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "vwap": 1},
            {"ts_utc": sat, "ts_ny": sat.astimezone(app.NY_TZ), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "vwap": 1},
        ]
        latest, session_date = app._bars_for_latest_regular_session(bars)
        self.assertEqual(session_date, "2026-03-20")
        self.assertEqual(len(latest), 2)

    def test_stale_reconcile_recovered_not_flagged(self):
        old_ts = (app.datetime.now(app.timezone.utc) - app.timedelta(days=10)).isoformat()
        row = {
            "symbol": "AMZN",
            "selected": False,
            "plan_created": False,
            "order_submitted": False,
            "fill_observed": False,
            "exit_armed": False,
            "exit_event": {},
            "active_position": False,
            "decisions": [{"ts_utc": old_ts, "action": "recovered_plan"}],
            "lifecycle_events": [],
            "latest_decision": {"ts_utc": old_ts, "action": "recovered_plan"},
        }
        out = app._pipeline_guardrail_rows([row], truth_source="current_runtime_preview")
        self.assertEqual(out[0]["issues"], [])

    def test_recent_fill_without_exit_arm_still_flagged(self):
        recent_ts = app.datetime.now(app.timezone.utc).isoformat()
        row = {
            "symbol": "SPY",
            "selected": False,
            "plan_created": False,
            "order_submitted": False,
            "fill_observed": True,
            "exit_armed": False,
            "exit_event": {},
            "active_position": False,
            "entry_event": {"ts_utc": recent_ts, "status": "filled"},
            "lifecycle_events": [{"ts_utc": recent_ts, "stage": "entry", "status": "filled"}],
            "decisions": [],
            "latest_decision": {},
        }
        out = app._pipeline_guardrail_rows([row], truth_source="authoritative_scan")
        self.assertEqual(out[0]["issues"][0]["code"], "fill_without_exit_arm")


if __name__ == "__main__":
    unittest.main()
