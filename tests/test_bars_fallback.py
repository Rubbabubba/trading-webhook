import os
import unittest
from unittest.mock import patch

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


class TestFetch1mBarsMultiFallback(unittest.TestCase):
    def test_fallback_used_when_sdk_raises(self):
        expected = {
            "AAPL": [
                {
                    "ts_utc": app.datetime.now(app.timezone.utc),
                    "ts_ny": app.now_ny(),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                    "vwap": 1.0,
                }
            ]
        }

        with patch("app.data_client") as dc, patch("app._fetch_bars_via_rest", return_value=(expected, {"ok": True})) as rest:
            dc.get_stock_bars.side_effect = RuntimeError("boom")
            out = app.fetch_1m_bars_multi(["AAPL"], lookback_days=1, limit_per_symbol=5)

        self.assertEqual(out, expected)
        rest.assert_called_once()

    def test_fallback_used_when_sdk_empty(self):
        class _Resp:
            df = []

        expected = {"MSFT": []}
        with patch("app.data_client") as dc, patch("app._fetch_bars_via_rest", return_value=(expected, {"ok": True})) as rest:
            dc.get_stock_bars.return_value = _Resp()
            out = app.fetch_1m_bars_multi(["MSFT"], lookback_days=1)

        self.assertEqual(out, expected)
        rest.assert_called_once()


if __name__ == "__main__":
    unittest.main()
