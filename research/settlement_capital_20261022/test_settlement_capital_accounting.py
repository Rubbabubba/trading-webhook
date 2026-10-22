import unittest
from settlement_capital_accounting import review


class AccountingTests(unittest.TestCase):
    def entry(self, key='a', start=10, debit='.62'):
        return dict(id=key,event='event',filled_at=start-1,observed_at=start,debit=debit)

    def credit(self, **changes):
        return dict(dict(id='cash1',entry_id='a',resolved_at=20,credited_at=30,observed_at=32,credit='.99'),**changes)

    def run_review(self, entries=None, credits=None, cutoff=40, cash=1):
        return review(entries or [self.entry()],credits or [],cutoff=cutoff,initial_cash=cash)

    def test_actual_credit_receipt_unlocks_cash_with_fees(self):
        r=self.run_review(credits=[self.credit()])
        self.assertEqual(r['cash'],'1.37')
        self.assertEqual(r['locked_dollar_seconds'],'13.64')
        self.assertEqual(r['paths'][0]['net_dollars'],'0.37')
        self.assertEqual(r['median_known_credit_delay_seconds'],'10')

    def test_resolved_and_credited_but_not_received_stays_locked(self):
        r=self.run_review(credits=[self.credit()],cutoff=31)
        self.assertEqual(r['cash'],'0.38')
        self.assertEqual(r['open_locked_dollars'],'0.62')
        self.assertIsNone(r['paths'][0]['net_dollars'])
        self.assertEqual(r['known_delay_samples'],0)

    def test_future_entry_not_available(self):
        self.assertEqual(self.run_review(cutoff=9)['paths'],[])

    def test_loss_zero_credit_releases_capital_not_principal(self):
        r=self.run_review(credits=[self.credit(credit=0)])
        self.assertEqual(r['cash'],'0.38')
        self.assertEqual(r['open_locked_dollars'],'0')
        self.assertEqual(r['paths'][0]['net_dollars'],'-0.62')

    def test_overlap_cannot_spend_future_winnings(self):
        with self.assertRaisesRegex(ValueError,'cash_capacity'):
            self.run_review(entries=[self.entry(),self.entry('b',25)],credits=[self.credit()])

    def test_duplicates_unowned_and_impossible_credit_fail_closed(self):
        for credits in ([self.credit(),self.credit(id='cash2')], [self.credit(entry_id='foreign')], [self.credit(credited_at=19)], [self.credit(credit='NaN')]):
            with self.subTest(credits=credits), self.assertRaises(ValueError):
                self.run_review(credits=credits)

    def test_independent_event_identity_and_order_invariance(self):
        entries=[self.entry('a',10,'.3'),self.entry('b',11,'.2')]
        a=self.run_review(entries=entries,credits=[self.credit()],cash=1)
        b=self.run_review(entries=list(reversed(entries)),credits=[self.credit()],cash=1)
        self.assertEqual(a['cash'],b['cash'])
        self.assertEqual(a['locked_dollar_seconds'],b['locked_dollar_seconds'])
        self.assertFalse(a['profitability_verified'])


if __name__ == '__main__':
    unittest.main()
