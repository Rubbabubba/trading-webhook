import copy
import hashlib
import unittest
from receipt_validation import validate, canonical


def fixture(win=True):
    base=dict(environment='demo',account='synthetic-only',entry_id='e',event='event',ticker='ticker')
    rows=dict(entry=dict(base,kind='entry',receipt_id='fill',side='yes',quantity='1',price_dollars='.40',fee_dollars='.02',filled_at=10,observed_at=11),resolution=dict(base,kind='resolution',receipt_id='result',result='yes' if win else 'no',resolved_at=20,observed_at=21),cash_credit=dict(base,kind='cash_credit',receipt_id='credit',credited_at=22,observed_at=23,gross_dollars='1' if win else '0',fee_dollars='0',net_dollars='1' if win else '0',complete=True))
    return {k:dict(payload=v,sha256=hashlib.sha256(canonical(v)).hexdigest()) for k,v in rows.items()}


def change(b,kind,key,value):
    b[kind]['payload'][key]=value
    b[kind]['sha256']=hashlib.sha256(canonical(b[kind]['payload'])).hexdigest()
    return b


class Receipts(unittest.TestCase):
    def test_complete_winner(self):
        r=validate([fixture()]);self.assertEqual(r['entries'][0]['debit'],'0.42');self.assertFalse(r['authenticity_verified']);self.assertFalse(r['execution_enabled'])
    def test_explicit_zero_loss(self):
        self.assertEqual(validate([fixture(False)])['credits'][0]['credit'],'0')
    def test_hash_tamper(self):
        b=fixture();b['entry']['payload']['price_dollars']='.01'
        with self.assertRaisesRegex(ValueError,'hash'):validate([b])
    def test_identity(self):
        with self.assertRaisesRegex(ValueError,'unmatched'):validate([change(fixture(),'cash_credit','entry_id','other')])
    def test_environment(self):
        with self.assertRaisesRegex(ValueError,'environment'):validate([change(fixture(),'entry','environment','live')])
    def test_duplicate(self):
        b=fixture()
        with self.assertRaisesRegex(ValueError,'duplicate'):validate([b,b])
    def test_negative_locked_time(self):
        with self.assertRaisesRegex(ValueError,'chronology'):validate([change(fixture(),'entry','observed_at',24)])
    def test_unreceived_resolution(self):
        with self.assertRaisesRegex(ValueError,'chronology'):validate([change(fixture(),'resolution','observed_at',24)])
    def test_future_economic_credit(self):
        with self.assertRaisesRegex(ValueError,'chronology'):validate([change(fixture(),'cash_credit','credited_at',24)])
    def test_wrong_settlement_amount(self):
        with self.assertRaisesRegex(ValueError,'credit'):validate([change(fixture(False),'cash_credit','gross_dollars','1')])
    def test_partial_credit(self):
        with self.assertRaisesRegex(ValueError,'credit'):validate([change(fixture(),'cash_credit','complete',False)])
    def test_fee_mismatch(self):
        with self.assertRaisesRegex(ValueError,'credit'):validate([change(fixture(),'cash_credit','fee_dollars','.02')])
    def test_fractional_quantity(self):
        with self.assertRaisesRegex(ValueError,'quantity'):validate([change(fixture(),'entry','quantity','.5')])
    def test_nonfinite(self):
        with self.assertRaisesRegex(ValueError,'nonfinite'):validate([change(fixture(),'entry','fee_dollars','NaN')])
    def test_cash_not_reconciled_by_hash(self):
        self.assertFalse(validate([fixture()])['cash_balance_reconciliation_verified'])
    def test_missing_loss_credit(self):
        b=fixture(False);del b['cash_credit']
        with self.assertRaises(KeyError):validate([b])
    def test_no_outcomes_does_not_certify(self):
        r=validate([]);self.assertEqual(r['entries'],[]);self.assertFalse(r['profitability_verified'])

if __name__=='__main__':unittest.main()
