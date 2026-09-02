from pathlib import Path

def test_patch_100_marker_present():
    app_text = Path(__file__).resolve().parents[1].joinpath("app.py").read_text(encoding="utf-8")
    assert "patch-100-execution-liquidity-override" in app_text
    assert "ENTRY_IEX_LIQUIDITY_OVERRIDE_ENABLED" in app_text
    assert "_entry_spread_override_decision" in app_text
