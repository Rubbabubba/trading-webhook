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

    def test_defensive_unlock_lab_finds_narrow_breakout_unlock(self):
        rows = [
            {
                "symbol": "CRM",
                "rank_score": 55.8,
                "close": 195.37,
                "fast_ma": 195.0,
                "slow_ma": 194.0,
                "return_20d_pct": 5.491,
                "close_to_high_pct": 99.877,
                "breakout_distance_pct": -4.6,
                "rejection_reasons": ["too_far_below_breakout"],
            },
            {
                "symbol": "NET",
                "rank_score": 77.5,
                "close": 215.34,
                "fast_ma": 212.79,
                "slow_ma": 195.19,
                "return_20d_pct": 21.613,
                "close_to_high_pct": 95.771,
                "breakout_distance_pct": -6.002,
                "rejection_reasons": ["close_not_near_high", "too_far_below_breakout"],
            },
        ]
        out = app._defensive_unlock_lab_from_rows(
            rows=rows,
            scan_symbols=["CRM", "NET"],
            ts_utc="2026-03-20T00:00:00+00:00",
            scan_source="unit_test",
            regime_mode="defensive",
            current_thresholds={
                "breakout_max_distance_pct": 1.0,
                "close_to_high_min_pct": 98.5,
                "return_20d_min_pct": 0.0,
            },
            limit=10,
        )
        self.assertEqual(out["regime_mode"], "defensive")
        self.assertTrue(out["ladder"])
        self.assertEqual(out["narrowest_unlock_step"]["type"], "breakout_only")
        self.assertEqual(out["narrowest_unlock_step"]["breakout_max_distance_pct"], 5.0)
        self.assertIn("CRM", out["narrowest_unlock_step"]["symbols"])

    def test_defensive_unlock_lab_is_noop_outside_defensive_mode(self):
        out = app._defensive_unlock_lab_from_rows(
            rows=[],
            scan_symbols=[],
            ts_utc="2026-03-20T00:00:00+00:00",
            scan_source="unit_test",
            regime_mode="trend",
            current_thresholds={},
            limit=5,
        )
        self.assertEqual(out["note"], "defensive_unlock_lab_only_applies_in_defensive_mode")

    def test_defensive_mode_defaults_to_seven_percent_breakout_distance(self):
        thresholds = app._regime_mode_thresholds("defensive")
        self.assertEqual(thresholds["mode"], "defensive")
        self.assertAlmostEqual(thresholds["breakout_max_distance_pct"], 0.07)
        self.assertAlmostEqual(thresholds["close_to_high_min_pct"], 0.985)
        self.assertAlmostEqual(thresholds["return_20d_min_pct"], 0.0)
        self.assertFalse(thresholds["require_trend"])
        self.assertFalse(thresholds["require_index_alignment"])

    def test_defensive_policy_snapshot_shows_unlock_vs_prior_policy(self):
        preview = {
            "ts_utc": "2026-03-20T21:21:16+00:00",
            "preview_source": "unit_test",
            "runtime_symbols": ["NET", "CRM"],
            "regime_mode": "defensive",
            "mode_thresholds": {
                "breakout_max_distance_pct": 0.07,
                "close_to_high_min_pct": 0.985,
                "return_20d_min_pct": 0.0,
                "require_trend": False,
                "require_index_alignment": False,
            },
            "top_candidates": [
                {
                    "symbol": "NET",
                    "rank_score": 77.5,
                    "close": 215.34,
                    "fast_ma": 212.79,
                    "slow_ma": 195.19,
                    "return_20d_pct": 21.613,
                    "close_to_high_pct": 98.8,
                    "breakout_distance_pct": -6.002,
                    "rejection_reasons": ["too_far_below_breakout"],
                },
                {
                    "symbol": "CRM",
                    "rank_score": 55.8,
                    "close": 195.37,
                    "fast_ma": 195.8,
                    "slow_ma": 194.66,
                    "return_20d_pct": 5.491,
                    "close_to_high_pct": 99.877,
                    "breakout_distance_pct": -4.6,
                    "rejection_reasons": [],
                },
            ],
        }
        original = app._current_runtime_preview_snapshot
        app._current_runtime_preview_snapshot = lambda limit=25: preview
        try:
            out = app._defensive_policy_snapshot(limit=10)
        finally:
            app._current_runtime_preview_snapshot = original
        self.assertEqual(out["eligible_count_previous_policy"], 0)
        self.assertEqual(out["eligible_count_current_policy"], 2)
        self.assertIn("NET", out["newly_unlocked_symbols"])


class TestFilterPressureTruthAlignment(unittest.TestCase):
    def test_filter_pressure_uses_mode_thresholds_and_separates_selection_blockers(self):
        rows = [
            {
                "symbol": "CRM",
                "rank_score": 55.8,
                "close": 195.37,
                "fast_ma": 195.8,
                "slow_ma": 194.66,
                "return_20d_pct": 5.491,
                "close_to_high_pct": 99.877,
                "breakout_distance_pct": -4.6,
                "rejection_reasons": [],
                "eligible": True,
                "selection_blockers": [],
            },
            {
                "symbol": "UBER",
                "rank_score": 44.4,
                "close": 73.885,
                "fast_ma": 74.5845,
                "slow_ma": 74.5255,
                "return_20d_pct": 0.007,
                "close_to_high_pct": 98.711,
                "breakout_distance_pct": -6.734,
                "rejection_reasons": ["symbol_exposure_limit"],
                "eligible": False,
                "selection_blockers": ["symbol_exposure_limit"],
            },
            {
                "symbol": "NET",
                "rank_score": 77.5,
                "close": 215.34,
                "fast_ma": 212.796,
                "slow_ma": 195.1987,
                "return_20d_pct": 21.613,
                "close_to_high_pct": 95.771,
                "breakout_distance_pct": -6.002,
                "rejection_reasons": ["close_not_near_high", "too_far_below_breakout"],
                "eligible": False,
                "selection_blockers": [],
            },
        ]
        out = app._filter_pressure_payload_from_rows(
            rows=rows,
            scan_symbols=["CRM", "UBER", "NET"],
            ts_utc="2026-03-20T00:00:00+00:00",
            scan_source="unit_test",
            global_block_reasons=[],
            limit=10,
            eligible_total=1,
            selected_total=1,
            mode_thresholds={
                "breakout_max_distance_pct": 7.0,
                "close_to_high_min_pct": 98.5,
                "return_20d_min_pct": 0.0,
                "require_trend": False,
                "require_index_alignment": False,
            },
        )
        self.assertEqual(out["baseline"]["eligible_count"], 1)
        self.assertEqual(out["baseline"]["filter_eligible_count"], 2)
        self.assertEqual(out["baseline"]["selection_blocked_count"], 1)
        self.assertIn("UBER", out["baseline"]["selection_blocked_symbols"])
        self.assertEqual(out["mode_thresholds"]["breakout_max_distance_pct"], 7.0)

    def test_current_runtime_truth_snapshot_keeps_full_preview_rows(self):
        preview = {
            "ts_utc": "2026-03-20T21:21:16+00:00",
            "preview_source": "unit_test",
            "runtime_symbols": ["CRM", "UBER", "NET"],
            "regime": {},
            "global_block_reasons": [],
            "selected_symbols": ["CRM"],
            "remaining_new_entries_today": 1,
            "max_new_entries_effective": 1,
            "regime_mode": "defensive",
            "mode_thresholds": {
                "breakout_max_distance_pct": 0.07,
                "close_to_high_min_pct": 0.985,
                "return_20d_min_pct": 0.0,
                "require_trend": False,
                "require_index_alignment": False,
            },
            "eligible_but_not_selected": [],
            "top_candidates": [
                {"symbol": "CRM", "eligible": True, "rejection_reasons": [], "rank_score": 55.8},
                {"symbol": "UBER", "eligible": False, "rejection_reasons": ["symbol_exposure_limit"], "selection_blockers": ["symbol_exposure_limit"], "rank_score": 44.4},
                {"symbol": "NET", "eligible": False, "rejection_reasons": ["close_not_near_high"], "rank_score": 77.5},
            ],
        }
        original = app._current_runtime_preview_snapshot
        app._current_runtime_preview_snapshot = lambda limit=25: preview
        try:
            out = app._current_runtime_truth_snapshot(limit=10)
        finally:
            app._current_runtime_preview_snapshot = original
        self.assertEqual(len(out["summary"]["top_candidates"]), 3)
        self.assertEqual(out["summary"]["selected_total"], 1)


if __name__ == "__main__":
    unittest.main()
