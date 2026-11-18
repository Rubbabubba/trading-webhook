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


def test_ranked_copy_recovers_and_only_sells_matching_owned_contract(tmp_path, monkeypatch):
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
                    timestamp=now + number, side=side, price=".47", size="1")

    monkeypatch.setattr(copy, "read_trades", activity)
    monkeypatch.setattr(copy, "discover", lambda *_: {"ticker": "GAME", "side": "yes"})
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
    inventory.reconcile({"positions": {}}, [{"ticker": "GAME", "position_fp": "1.00"}])
    with pytest.raises(ValueError, match="external_or_unreconciled_position"):
        inventory.reconcile({"positions": {}}, [{"ticker": "UNOWNED", "position_fp": "1.00"}])

    journal.close()
    journal = copy.open_journal(tmp_path)
    exchange.j = journal
    worker = copy.WalletCopy(tmp_path, db, BinaryDemoBroker(journal, exchange),
                             read=leaderboard, clock=lambda: now + 15)
    signals.extend([trade(wallets[1], 2, "SELL"), trade(wallets[0], 3, "SELL", token="999"),
                    trade(wallets[0], 4, "SELL")])
    final = worker.tick()
    assert final["signals"] == {"buy:filled": 1, "sell:filled": 1,
                                "sell:no_owned_contract_for_wallet_sell": 2}
    assert final["open_positions"] == 0
    inventory.reconcile({"positions": {}}, [])
    assert journal.records()[-1]["payload"]["reduce_only"] is True
    assert worker.tick()["signals"] == final["signals"]
    assert exchange.posts == 2
    exchange.timeout = True
    signals.append(trade(wallets[0], 5, 'BUY'))
    unresolved = worker.tick()
    assert unresolved['execution_error'] == 'submission_unresolved'
    assert unresolved['last_submission_failure'] == {'reason':'TimeoutError','observed_at':now+15}
    assert worker.tick()['last_submission_failure'] == unresolved['last_submission_failure']
    assert exchange.posts == 3
    journal.close()
    db.close()


from unittest import TestCase
import tempfile
from pathlib import Path
import re


class WalletClientOrderIdTests(TestCase):
    def test_lossless_legal_distinct_and_legacy(self):
        keys = ['0' * 64, 'f' * 64, 'a' * 63 + '0', 'a' * 63 + '1']
        ids = [copy.client_order_id(key) for key in keys]
        self.assertEqual(len(set(ids)), len(keys))
        for key, cid in zip(keys, ids):
            self.assertEqual(len(cid), 55)
            self.assertLessEqual(len(cid), 64)
            self.assertIsNotNone(re.fullmatch(r'[A-Za-z0-9_-]+', cid))
            self.assertEqual(copy.signal_key(cid), key)
            self.assertEqual(copy.signal_key('wallet-copy-' + key), key)

    def test_canonical_validation(self):
        cid = copy.client_order_id('0' * 64)
        for bad in [None, '', cid + '=', cid[:-1], cid[:-1] + 'B',
                    cid.replace('wallet-copy-', ''), 'wallet-copy-' + 'A' * 64,
                    cid[:-1] + '/', cid + '\n']:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                copy.signal_key(bad)
        for bad in [None, 'a' * 63, 'A' * 64, 'z' * 64]:
            with self.assertRaises(ValueError):
                copy.client_order_id(bad)

    def test_restart_and_quarantine_keep_full_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            journal = copy.open_journal(root)
            db = sqlite3.connect(':memory:', isolation_level=None)
            worker = copy.WalletCopy(root, db, BinaryDemoBroker(journal, Exchange(journal)))
            for index, legacy in enumerate((False, True)):
                key = format(index + 1, '064x')
                cid = 'wallet-copy-' + key if legacy else copy.client_order_id(key)
                db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?)',
                           (key, 1, 'BUY', '{}', 'submitting', '{}'))
                journal.reserve(cid, 'GAME', 1, 30, 2, outcome='yes', action='buy',
                                account_snapshot=snapshot(journal))
                worker = copy.WalletCopy(root, db, BinaryDemoBroker(journal, Exchange(journal)))
                worker.recover()
                self.assertEqual(db.execute('SELECT state FROM signals WHERE id=?',
                                            (key,)).fetchone()[0], 'interrupted_before_submission')
                db.execute("UPDATE signals SET state='submission_unresolved' WHERE id=?", (key,))
                worker.record_quarantined_signal(cid)
                self.assertEqual(db.execute('SELECT state FROM signals WHERE id=?',
                                            (key,)).fetchone()[0], 'quarantined_zero_fill')
            journal.close()
            db.close()
