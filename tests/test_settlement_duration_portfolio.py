import unittest
from opportunity_lab.settlement_duration_portfolio import rank

class SettlementDurationTests(unittest.TestCase):
    def setUp(self):
        self.training = [dict(event_id='past', resolved_at=10, credited_at=20, observed_at=21)]
        self.row = dict(event_id='future', quote_at=99, quote_expires_at=101, forecast_at=98, expected_resolution_at=200, ask='.4', expected_payout='.6', fee='.01', available_quantity=1, quantity=1, capital_budget=1)
    def run_rank(self, rows=None, training=None):
        return rank(rows if rows is not None else [self.row], self.training if training is None else training, decision_at=100, fee_stress='.01')
    def test_delay_and_cost(self):
        row = self.run_rank()[0]
        self.assertEqual(row['expected_locked_seconds'], '110')
        self.assertEqual(row['expected_net'], '0.18')
        self.assertFalse(row['execution_enabled'])
    def test_no_future_delay_leak(self):
        self.assertEqual(self.run_rank(training=[dict(event_id='later', resolved_at=101, credited_at=102, observed_at=103)]), [])
    def test_missing_training(self):
        self.assertEqual(self.run_rank(training=[]), [])
    def test_partial_fill_rejected(self):
        self.row['available_quantity'] = '.5'
        with self.assertRaises(ValueError): self.run_rank()
    def test_stale_quote(self):
        self.row['quote_expires_at'] = 99
        with self.assertRaises(ValueError): self.run_rank()
    def test_future_forecast(self):
        self.row['forecast_at'] = 100
        with self.assertRaises(ValueError): self.run_rank()
    def test_event_independence(self):
        with self.assertRaises(ValueError): self.run_rank([self.row, self.row])
    def test_budget(self):
        self.row['capital_budget'] = '.4'
        with self.assertRaises(ValueError): self.run_rank()
    def test_nonfinite(self):
        self.row['ask'] = 'NaN'
        with self.assertRaises(ValueError): self.run_rank()
    def test_shorter_duration_ranks_first(self):
        other = dict(self.row, event_id='short', expected_resolution_at=150)
        self.assertEqual(self.run_rank([self.row, other])[0]['event_id'], 'short')
