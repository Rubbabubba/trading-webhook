from datetime import datetime,timezone
from opportunity_lab.sports_live_v34 import scoreboard_dates

def test_soccer_range_retains_previous_day_after_utc_midnight():
    now=datetime(2026,9,13,0,1,tzinfo=timezone.utc)
    assert scoreboard_dates(now,'mls')=='20260912-20260914'
    assert scoreboard_dates(now,'epl')=='20260912-20260914'
    assert scoreboard_dates(now,'atp')=='20260913'
