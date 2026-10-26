import unittest
from queue_evidence import assess


def plan(**changes):
    p = dict(environment='demo', ticker='X', epoch='E', side='yes', price='.5',
             queue_ahead='2', quantity='1', arrival_at=10, cancel_requested_at=20,
             cancel_ack_at=22, close_at=30, clock_margin_seconds=1)
    p.update({k: True for k in ('metadata_current', 'rules_verified', 'fees_verified',
        'activation_book_verified', 'clock_bound_verified', 'side_mapping_verified')})
    return dict(p, **changes)


def trade(**changes):
    return dict(dict(id='a', ticker='X', epoch='E', resting_side='yes', occurred_at=15,
                     received_at=15.1, price='.5', quantity='3'), **changes)


class Admission(unittest.TestCase):
    def run_case(self, rows, **p):
        return assess(plan(**p), rows, tape_complete=True)
    def test_full_depletion_is_never_owned_fill(self):
        r = self.run_case([trade()])
        self.assertEqual(r['state'], 'conditional_queue_depletion')
        self.assertEqual(r['owned_fills'], 0)
        self.assertFalse(r['execution_enabled'] or r['promotion_ready'])
    def test_queue_volume_is_not_fill(self):
        self.assertEqual(self.run_case([trade(quantity='2')])['state'], 'no_evidence')
    def test_duplicate_not_double_counted(self):
        t = trade(quantity='1.5')
        self.assertEqual(self.run_case([t, t])['state'], 'no_evidence')
    def test_conflicting_duplicate_blocks(self):
        self.assertEqual(self.run_case([trade(), trade(quantity='4')])['state'], 'blocked')
    def test_partial_remains_uncertain(self):
        self.assertEqual(self.run_case([trade(quantity='2.5')])['state'], 'uncertain')
    def test_opposite_direction_not_volume(self):
        self.assertEqual(self.run_case([trade(resting_side='no')])['state'], 'no_evidence')
    def test_other_price_not_inferred_fill(self):
        self.assertEqual(self.run_case([trade(price='.49')])['state'], 'no_evidence')
    def test_cancel_race_remains_uncertain(self):
        self.assertEqual(self.run_case([trade(occurred_at=21, received_at=21.1)])['state'], 'uncertain')
    def test_activation_clock_band(self):
        self.assertEqual(self.run_case([trade(occurred_at=10, received_at=10.1)])['state'], 'uncertain')
    def test_incomplete_tape_not_zero_fill(self):
        self.assertEqual(assess(plan(), [], tape_complete=False)['state'], 'uncertain')
    def test_epoch_gap_blocks(self):
        self.assertEqual(self.run_case([trade(epoch='other')])['state'], 'blocked')
    def test_missing_fee_proof_blocks(self):
        self.assertEqual(self.run_case([trade()], fees_verified=False)['state'], 'blocked')
    def test_receipt_regression_blocks(self):
        self.assertEqual(self.run_case([trade(), trade(id='b', received_at=14)])['state'], 'blocked')
    def test_nonfinite_rejected(self):
        with self.assertRaises(ValueError): self.run_case([trade(quantity='NaN')])
    def test_bad_lifecycle_rejected(self):
        with self.assertRaises(ValueError): self.run_case([], cancel_ack_at=31)
    def test_live_rejected(self):
        with self.assertRaises(ValueError): self.run_case([], environment='live')


if __name__ == '__main__': unittest.main()
