import os

import intraday_defaults


def test_intraday_defaults_contain_no_swing_keys():
    assert not any("SWING" in key for key in intraday_defaults.INTRADAY_DEFAULTS)


def test_intraday_defaults_do_not_override_render(monkeypatch):
    monkeypatch.setenv("DATA_FEED", "sip")
    intraday_defaults.apply_intraday_defaults()
    assert os.environ["DATA_FEED"] == "sip"
