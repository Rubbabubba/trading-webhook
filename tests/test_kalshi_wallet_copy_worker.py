from datetime import datetime, timezone
import json
import sqlite3
import time

import pytest

from opportunity_lab import kalshi_wallet_copy_worker as copy
from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
from opportunity_lab.kalshi_demo_test_inventory import DemoTestInventory
from test_kalshi_binary_journal import Exchange, quote, snapshot
from opportunity_lab.kalshi_binary_journal import BinaryJournal


@pytest.mark.parametrize("outcome,side,source,expected", [
    ("yes", "BUY", ".45", 50),
    ("yes", "SELL", ".49", 44),
    ("no", "BUY", ".51", 56),
    ("no", "SELL", ".55", 50),
])
def test_five_cent_band_uses_executable_outcome_price(outcome, side, source, expected):
    quote = {"orderbook_fp": {"yes_dollars": [[".44", "2"]], "no_dollars": [[".50", "2"]]}}
    assert copy.executable({"side": side, "price": source}, {"side": outcome}, quote) == expected
    with pytest.raises(ValueError, match="outside_five_cent_band"):
        copy.executable({"side": side, "price": ".20"}, {"side": outcome}, quote)


def test_uncertain_wallet_entry_requires_two_flat_broker_reads_and_never_resends(tmp_path):
    now = int(time.time())
    clock = lambda: now
    journal = BinaryJournal(tmp_path / 'journal.db', clock=lambda: datetime.fromtimestamp(clock(), timezone.utc))
    class FlatExchange(Exchange):
        def pages(self, path, field, **params):
            if field == 'fills':
                return []
            return super().pages(path, field, **params)
    exchange = FlatExchange(journal)
    exchange.timeout = True
    broker = BinaryDemoBroker(journal, exchange)
    with sqlite3.connect(':memory:', isolation_level=None) as db:
        worker = copy.WalletCopy(tmp_path, db, broker, clock=clock)
        key = 'a' * 64
        cid = 'wallet-copy-' + key
        db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?)',
                   (key, now, 'BUY', '{}', 'submission_unresolved', '{"ticker":"GAME"}'))
        journal.reserve(cid, 'GAME', 1, 30, 2, outcome='yes', action='buy', account_snapshot=snapshot(journal))
        with pytest.raises(TimeoutError):
            broker.submit(cid, quote_snapshot=quote(journal, cid))
        now += 301
        with pytest.raises(ValueError, match='submission_unresolved'):
            worker.recover()
        assert journal.get(cid)['state'] == 'uncertain'
        now += 61
        worker.recover()
        assert journal.records() == []
        assert db.execute('SELECT state FROM signals').fetchone()[0] == 'quarantined_zero_fill'
        proof = json.loads(journal.db.execute('SELECT evidence FROM uncertain_quarantine').fetchone()[0])
        assert len(proof['negative_observations']) == 2
        assert exchange.posts == 1
    journal.close()


@pytest.mark.parametrize('outcome',['yes','no'])
@pytest.mark.parametrize('buy_budget_exhausted',[False,True])
def test_ranked_copy_recovers_and_only_sells_matching_owned_contract(tmp_path, monkeypatch,outcome,buy_budget_exhausted):
    wallets = ["0x" + str(index) * 40 for index in range(1, 5)]
    now = int(time.time())
    state = tmp_path / copy.DIRECTORY
    state.mkdir()
    (tmp_path / "status.json").write_text(json.dumps({
        "at": datetime.fromtimestamp(now, timezone.utc).isoformat(), "environment": "demo",
        "production_execution_enabled": False, "new_submissions_enabled": False,
        "wallet_copy_inventory_ledger": True, "phase": "running",
    }))
    (tmp_path / "separate-demo-test-inventory-v1.json").write_text('{"positions":[]}')
    db = sqlite3.connect(state / "signals.sqlite3", isolation_level=None)
    journal = copy.open_journal(tmp_path)
    exchange = Exchange(journal)
    broker = BinaryDemoBroker(journal, exchange)
    signals = []

    def leaderboard(url, params):
        assert url == copy.LEADERBOARD
        assert params["category"] in {"OVERALL", "SPORTS"}
        assert {key: params[key] for key in ("time_period", "order_by", "limit")} == {
            "time_period": "WEEK", "order_by": "PNL", "limit": 4}
        return ({"data": [{"rank": index, "user_id": wallet}
                          for index, wallet in enumerate(wallets, 1)]},)

    def activity(wallet, since, read):
        return [trade for trade in signals if trade["proxy_wallet"] == wallet]

    def quote(mapping, base, read):
        at = time.time() - .01
        return dict(environment="demo", ticker=mapping["ticker"], started_at=at, observed_at=at,
                    orderbook_fp={"yes_dollars": [[".44", "2"]], "no_dollars": [[".50", "2"]]})

    def trade(wallet, number, side, token="123"):
        return dict(proxy_wallet=wallet, transaction_hash=f"0x{number:064x}", type="TRADE",
                    condition_id="0x" + "a" * 64, token_id=token,
                    timestamp=now + number, side=side, price=".47" if outcome=='yes' else '.53', size="1")

    monkeypatch.setattr(copy, "read_trades", activity)
    monkeypatch.setattr(copy, "discover", lambda *_: {"ticker": "GAME", "side": outcome})
    monkeypatch.setattr(copy, "read_quote", quote)
    monkeypatch.setattr(copy, "read_fee", lambda *_: {"fee_multiplier": "1"})
    worker = copy.WalletCopy(tmp_path, db, broker, read=leaderboard, clock=lambda: now)
    worker.clock = lambda: now + 10
    signals.extend([trade(wallets[0], 0, "BUY"), trade(wallets[0], 1, "BUY")])
    first = worker.tick()
    assert first["ranked_wallets"] == 4
    assert first["open_positions"] == 1, first
    assert first["signals"] == {"buy:filled": 1}
    assert exchange.posts == 1
    inventory = copy.SharedDemoInventory(DemoTestInventory({}, "test-receipt"), tmp_path)
    inventory.reconcile({"positions": {}}, [{"ticker": "GAME", "position_fp": "1.00" if outcome=='yes' else '-1.00'}])
    with pytest.raises(ValueError, match="external_or_unreconciled_position"):
        inventory.reconcile({"positions": {}}, [{"ticker": "UNOWNED", "position_fp": "1.00"}])

    journal.close()
    journal = copy.open_journal(tmp_path)
    exchange.j = journal
    worker = copy.WalletCopy(tmp_path, db, BinaryDemoBroker(journal, exchange),
                             read=leaderboard, clock=lambda: now + 15)
    # The original owner drops out of the new ranking. Its fills must still be
    # monitored, and a full daily BUY allowance cannot strand an owned SELL.
    db.execute('UPDATE cohort SET wallets=?',(json.dumps([*wallets[1:],'0x'+'9'*40]),))
    if buy_budget_exhausted:
        for index in range(copy.MAX_DAILY_BUYS-1):
            db.execute("INSERT INTO signals VALUES(?,?,?,'{}','ioc_no_fill',NULL)",
                       (f'budget-{index}',now,'BUY'))
    signals.extend([trade(wallets[1], 2, "SELL"), trade(wallets[0], 3, "SELL", token="999"),
                    trade(wallets[0], 4, "SELL")])
    final = worker.tick()
    expected={"buy:filled":1,"sell:filled":1,"sell:no_owned_contract_for_wallet_sell":2}
    if buy_budget_exhausted:expected['buy:ioc_no_fill']=copy.MAX_DAILY_BUYS-1
    assert final['signals']==expected
    assert final['wallets_polled']==5
    assert final["open_positions"] == 0
    inventory.reconcile({"positions": {}}, [])
    assert journal.records()[-1]["payload"]["reduce_only"] is True
    assert worker.tick()["signals"] == final["signals"]
    assert exchange.posts == 2
    assert final['lifecycle']['sell_verification']=='broker_fill_verified'
    assert final['lifecycle']['sell_attempts']==1
    assert final['lifecycle']['sell_signals_observed']==3
    assert final['lifecycle']['unresolved_orders']==0
    from fractions import Fraction
    assert journal.accounting()['realized']==Fraction(-8,100)
    if buy_budget_exhausted:
        assert final['lifecycle']['remaining_buy_attempts']==0
        assert final['lifecycle']['sell_admission']=='eligible_owned_sells_only'
        journal.close();db.close();return
    exchange.timeout = True
    signals.append(trade(wallets[1], 5, 'BUY'))
    unresolved = worker.tick()
    assert unresolved['execution_error'] == 'submission_unresolved'
    assert unresolved['last_submission_failure'] == {'reason':'TimeoutError','observed_at':now+15}
    assert worker.tick()['last_submission_failure'] == unresolved['last_submission_failure']
    assert exchange.posts == 3
    journal.close()
    db.close()


from unittest import TestCase


class WalletClientOrderIdTests(TestCase):
    def test_lossless_legal_distinct_ids(self):
        import re
        keys = ['00' * 32, 'ff' * 32, 'ab' * 31 + 'ac']
        ids = [copy.client_order_id(key) for key in keys]
        self.assertEqual(len(set(ids)), len(keys))
        for key, cid in zip(keys, ids):
            self.assertEqual(len(cid), 55)
            self.assertLessEqual(len(cid), 64)
            self.assertIsNotNone(re.fullmatch(r'[A-Za-z0-9_-]+', cid))
            self.assertEqual(copy.signal_key(cid), key)
            self.assertEqual(copy.signal_key('wallet-copy-' + key), key)

    def test_strict_canonical_validation(self):
        good = copy.client_order_id('00' * 32)
        for bad in [None, '', good + '=', good[:-1], good[:-1] + 'B',
                    good.replace('wallet-copy-', 'other-'),
                    'wallet-copy-' + 'A' * 64, 'wallet-copy-' + '+' * 43]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                copy.signal_key(bad)
        for bad in [None, '', 'AA' * 32, 'a' * 63, 'g' * 64]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                copy.client_order_id(bad)

    def test_restart_and_quarantine_keep_full_key(self):
        from types import SimpleNamespace
        for legacy in (False, True):
            with self.subTest(legacy=legacy), sqlite3.connect(':memory:') as db:
                key = 'ab' * 32
                other = 'ab' * 31 + 'ac'
                cid = 'wallet-copy-' + key if legacy else copy.client_order_id(key)
                class Journal:
                    def __init__(self):
                        self.db = sqlite3.connect(':memory:')
                        self.db.execute('CREATE TABLE uncertain_quarantine(id TEXT)')
                    def records(self):
                        return [dict(payload=dict(client_order_id=cid), state='terminal', filled=0)]
                    def accounting(self):
                        return {'positions': {}}
                journal = Journal()
                broker = SimpleNamespace(journal=journal)
                worker = copy.WalletCopy(None, db, broker, clock=lambda: 100)
                for value in (key, other):
                    db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?)',
                               (value, 100, 'BUY', '{}', 'submission_unresolved', '{}'))
                worker.recover()
                self.assertEqual(db.execute('SELECT state FROM signals WHERE id=?', (key,)).fetchone()[0], 'ioc_no_fill')
                db.execute("UPDATE signals SET state='submission_unresolved' WHERE id=?", (key,))
                journal.db.execute('INSERT INTO uncertain_quarantine VALUES(?)', (cid,))
                restarted = copy.WalletCopy(None, db, broker, clock=lambda: 101)
                restarted.recover()
                self.assertEqual(db.execute('SELECT state FROM signals WHERE id=?', (key,)).fetchone()[0], 'quarantined_zero_fill')
                self.assertEqual(db.execute('SELECT state FROM signals WHERE id=?', (other,)).fetchone()[0], 'submission_unresolved')
                journal.db.close()


class SharedDemoInventoryQuantityTests(TestCase):
    def inventory(self, root, positions, baseline=None):
        from types import SimpleNamespace
        from unittest.mock import patch
        state = root / copy.DIRECTORY
        state.mkdir(exist_ok=True)
        (state / 'journal.sqlite3').touch()
        journal = SimpleNamespace(accounting=lambda: {'positions': positions}, close=lambda: None)
        self.patch = patch.object(copy, 'open_journal', return_value=journal)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        return copy.SharedDemoInventory(DemoTestInventory(baseline or {}, 'receipt'), root)

    def test_whole_fractions_are_json_safe_and_reconcile_exactly(self):
        from fractions import Fraction
        from pathlib import Path
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as directory:
            original = {'YES': Fraction(2), 'NO': Fraction(-3)}
            inventory = self.inventory(Path(directory), original, {'BASE': 1})
            positions = inventory.positions
            self.assertEqual(json.loads(json.dumps(positions)), {'BASE': 1, 'YES': 2, 'NO': -3})
            self.assertTrue(all(type(value) is int for value in positions.values()))
            self.assertTrue(all(type(value) is Fraction for value in original.values()))
            rows = [{'ticker': ticker, 'position_fp': str(value)} for ticker, value in positions.items()]
            inventory.reconcile({'positions': {}}, rows)
            with self.assertRaises(ValueError):
                inventory.reconcile({'positions': {}}, rows[:-1])
            with self.assertRaisesRegex(ValueError, 'maker_inventory_not_flat'):
                inventory.reconcile({'positions': {'MAKER': 1}}, rows)
            self.assertEqual(inventory.receipt_sha256, 'receipt')

    def test_overlap_remains_a_hold(self):
        from fractions import Fraction
        from pathlib import Path
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as directory:
            inventory = self.inventory(Path(directory), {'BASE': Fraction(1)}, {'BASE': 1})
            with self.assertRaisesRegex(ValueError, 'demo_inventory_overlap'):
                inventory.positions

    def test_unknown_and_nonintegral_quantities_fail_closed(self):
        from fractions import Fraction
        from decimal import Decimal
        from pathlib import Path
        from tempfile import TemporaryDirectory
        for quantity in (Fraction(1, 2), Fraction(-3, 2), None, '1', 1.0, True, Decimal('1'), object()):
            with self.subTest(quantity=quantity), TemporaryDirectory() as directory:
                inventory = self.inventory(Path(directory), {'WALLET': quantity})
                with self.assertRaisesRegex(ValueError, 'invalid_wallet_inventory_quantity'):
                    inventory.positions

    def test_baseline_only_does_not_open_wallet_journal(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest.mock import patch
        with TemporaryDirectory() as directory, patch.object(copy, 'open_journal') as opened:
            baseline = DemoTestInventory({'BASE': -2}, 'baseline-receipt')
            inventory = copy.SharedDemoInventory(baseline, Path(directory))
            self.assertIs(inventory.positions, baseline.positions)
            self.assertEqual(json.loads(json.dumps(inventory.positions)), {'BASE': -2})
            inventory.reconcile({'positions': {}}, [{'ticker': 'BASE', 'position_fp': '-2'}])
            opened.assert_not_called()
