import unittest
from datetime import datetime, timezone
from opportunity_lab.sports_capture_expansion import in_scope, book_due, validate
from tools.assess_novig_public import summarize


class ExpansionTests(unittest.TestCase):
    def test_tennis_other_tournament_excluded_and_final_discoverable(self):
        config = {'first_date': '2026-09-10', 'last_date': '2026-09-17'}
        market = {'event_ticker': 'KXATPMATCH-26SEP13AB', 'rules_primary': 'US Open Final'}
        self.assertTrue(in_scope(market, config, 'KXATPMATCH'))
        market['rules_primary'] = 'Other tournament'
        self.assertFalse(in_scope(market, config, 'KXATPMATCH'))

    def test_soccer_draw_included_and_dates_bounded(self):
        config = {'first_date': '2026-09-10', 'last_date': '2026-09-17'}
        self.assertTrue(in_scope({'event_ticker': 'KXEPLGAME-26SEP13MUNMCI', 'ticker': 'DRAW'}, config, 'KXEPLGAME'))
        self.assertFalse(in_scope({'event_ticker': 'KXEPLGAME-26SEP20AB'}, config, 'KXEPLGAME'))
        self.assertFalse(in_scope({'event_ticker': 'INVALID'}, config, 'KXEPLGAME'))

    def test_overnight_capture_window(self):
        market = {'event_ticker': 'KXMLSGAME-26SEP12AB'}
        self.assertTrue(book_due(market, datetime(2026, 9, 13, 5, tzinfo=timezone.utc)))
        self.assertFalse(book_due(market, datetime(2026, 9, 13, 12, tzinfo=timezone.utc)))

    def test_execution_cannot_enable(self):
        with self.assertRaises(ValueError):
            validate({'version': 'expansion_capture_1.0', 'execution_enabled': True})

    def test_novig_does_not_double_count_maker_or_combo(self):
        raw = b'side,tradeType,league,qty,cost,marketId\nTAKER,STRAIGHT,EPL,100,45,m1\nMAKER,STRAIGHT,EPL,100,55,m1\nTAKER,COMBO,,100,50,m2\n'
        self.assertEqual(summarize(raw), {'EPL': {'trades': 1, 'contracts': '100', 'taker_stake': '45', 'markets': 1}})


if __name__ == '__main__':
    unittest.main()
