from datetime import datetime, timezone

from tools.list_demo_sports_events import catalog_state


NOW = datetime(2026, 9, 28, tzinfo=timezone.utc)


def test_current_year_event_enters_mapping_queue():
    assert catalog_state('KXNFLGAME-26OCT01ABCCDE', NOW) == 'awaiting_schedule_mapping_and_pregame_anchor'


def test_old_and_copied_demo_events_are_rejected():
    assert catalog_state('KXNFLGAME-25SEP04DALPHI-DKCOPY', NOW) == 'rejected_stale_or_copied_ticker'
    assert catalog_state('KXMLSGAME-25JUL05ATXLAFC', NOW) == 'rejected_stale_or_copied_ticker'
