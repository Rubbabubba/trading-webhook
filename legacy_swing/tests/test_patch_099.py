from pathlib import Path


def test_patch_099_marker_and_override_constants_present():
    text = Path(__file__).resolve().parents[1].joinpath("app.py").read_text()
    assert "patch-099-early-entry-override" in text
    assert "SWING_EARLY_ENTRY_OVERRIDE_ENABLED" in text
    assert "EARLY_ENTRY_OVERRIDE_SOURCE" in text
