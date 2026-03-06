import os
import unittest

os.environ.setdefault("APCA_API_KEY_ID", "test-key")
os.environ.setdefault("APCA_API_SECRET_KEY", "test-secret")
os.environ.setdefault("TRADES_TODAY_ENABLE", "false")
os.environ.setdefault("SCANNER_VOL_RANK_ENABLE", "false")

import app


class TestSystemImprovements(unittest.TestCase):
    def test_env_bool_accepts_bool_default(self):
        os.environ.pop("UNITTEST_BOOL_MISSING", None)
        self.assertFalse(app.env_bool("UNITTEST_BOOL_MISSING", False))
        self.assertTrue(app.env_bool("UNITTEST_BOOL_MISSING", True))

    def test_resample_5m_keeps_datetime_ts_utc(self):
        ts = app.datetime.now(app.timezone.utc).replace(second=0, microsecond=0)
        bars = [
            {
                "ts_utc": ts,
                "ts_ny": ts.astimezone(app.NY_TZ),
                "open": 1.0,
                "high": 1.2,
                "low": 0.9,
                "close": 1.1,
                "volume": 100,
                "vwap": 1.05,
            }
        ]
        out = app.resample_5m(bars)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["ts_utc"], ts)

    def test_hf_signal_reads_long_ohlc_keys(self):
        ts = app.datetime.now(app.timezone.utc).replace(second=0, microsecond=0)
        b0 = {
            "ts_utc": ts,
            "ts_ny": ts.astimezone(app.NY_TZ).isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 10,
            "vwap": 100.2,
            "ema_fast": 100.3,
        }
        b1 = {
            **b0,
            "close": 101.2,
            "high": 101.4,
            "low": 100.8,
            "vwap": 100.8,
            "ema_fast": 100.9,
        }
        sig, dbg = app.eval_hf_signal_with_debug([b0, b1], [b0, b1])
        self.assertIn("components", dbg)
        self.assertAlmostEqual(dbg["components"]["price"], 101.2)
        self.assertIn(sig[1] if sig else "", ("buy", ""))


if __name__ == "__main__":
    unittest.main()
