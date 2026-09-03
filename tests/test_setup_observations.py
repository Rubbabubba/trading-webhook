from regime_intraday_ledger import empty_ledger, record_setup_observations, setup_observation_summary


def _scan(bar_ts="2026-09-03T12:00:00-04:00", *, stretch=False, reversal=True):
    proximity = {"symbol": "SPY", "strategy": "vwap_mean_reversion", "data_ready": True,
                 "regime_ready": True, "stretch_ready": stretch, "reversal_ready": reversal,
                 "underlying_signal_ready": stretch and reversal,
                 "next_gate": "underlying signal ready" if stretch and reversal else "needs more VWAP stretch",
                 "vwap_distance_atr": -0.8, "distance_to_nearest_band_edge_atr": 0.2}
    return {"ts_utc": "2026-09-03T16:00:30+00:00", "features": {"SPY": {"last_ts": bar_ts}},
            "sleeves": {"spy_mean_reversion": {"setup_proximity": [proximity]}}}


def test_setup_observations_are_deduplicated_by_completed_bar():
    ledger = empty_ledger()
    record_setup_observations(ledger, _scan())
    record_setup_observations(ledger, _scan())
    assert len(ledger["setup_observations"]) == 1


def test_setup_observation_summary_counts_gates_and_closest_miss():
    ledger = empty_ledger()
    record_setup_observations(ledger, _scan())
    record_setup_observations(ledger, _scan("2026-09-03T12:01:00-04:00", stretch=True))
    result = setup_observation_summary(ledger, session="2026-09-03")
    spy = result["by_symbol"]["SPY"]
    assert spy["observations"] == 2
    assert spy["signal_ready"] == 1
    assert spy["next_gate_counts"]["needs more VWAP stretch"] == 1
    assert spy["closest_miss"]["gates_passed"] == 3
