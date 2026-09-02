from pathlib import Path


def test_patch_097_marker_present():
    text=(Path(__file__).resolve().parents[1]/"app.py").read_text(encoding="utf-8")
    assert "patch-097-regime-b-mean-reversion" in text
    assert "evaluate_daily_mean_reversion_candidate" in text
    assert "/diagnostics/regime_b" in text
