from datetime import datetime, timezone
import json
import sqlite3

import pytest

from opportunity_lab import kalshi_wallet_cap_shadow as shadow
from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
from opportunity_lab.kalshi_binary_journal import BinaryJournal
from test_kalshi_binary_journal import Exchange, quote


def test_cap_shadow_captures_only_fresh_post_start_buys(tmp_path, monkeypatch):
    now = 1795379000
    source = tmp_path / 'ranked-wallet-copy-v1'
    source.mkdir()
    archive = tmp_path / shadow.DIRECTORY
    archive.mkdir()
    conn = sqlite3.connect(source / 'signals.sqlite3', isolation_level=None)
    conn.execute('CREATE TABLE signals(id TEXT,trade TEXT,side TEXT,state TEXT,at INTEGER)')
    conn.executemany('INSERT INTO signals VALUES(?,?,?,?,?)', [
        ('prior', '{}', 'BUY', 'demo_entry_limit', now - 90),
        ('ignored', '{}', 'BUY', 'filled', now),
        ('sell', '{}', 'SELL', 'demo_entry_limit', now),
        ('keep', json.dumps({'timestamp': now}), 'BUY', 'demo_entry_limit', now),
    ])
    conn.close()
    db = shadow.open_archive(archive / 'research.sqlite3', now=now - 60)
    def status(unresolved):
        (source / 'status.json').write_text(json.dumps(dict(
            environment='demo', at=datetime.fromtimestamp(now, timezone.utc).isoformat(),
            execution_error=None, source_errors=[], lifecycle=dict(
                state='reconciled', unresolved_orders=unresolved))))
    seen = []
    def inspect(trade, **unused):
        seen.append(trade)
        return {'state': 'executable_quote', 'price_cents': 40}
    monkeypatch.setattr(shadow, 'inspect', inspect)
    status(1)
    assert shadow.tick(tmp_path, db, archive=archive, clock=lambda: now)['state'] == 'reconciliation_hold'
    assert seen == []
    status(0)
    assert shadow.tick(tmp_path, db, archive=archive, clock=lambda: now)['counts'] == {'executable_quote': 1}
    assert shadow.tick(tmp_path, db, archive=archive, clock=lambda: now)['state'] == 'no_fresh_candidate'
    ident, raw = db.execute('SELECT id,detail FROM captures').fetchone()
    assert ident == 'keep'
    assert json.loads(raw)['actual_fill'] is False
    db.close()


def test_six_position_demo_adapter_rehearsal_stops_on_reconciliation_mismatch(tmp_path):
    # Isolated synthetic exchange. Exercises the existing adapter, with no signed transport.
    journal = BinaryJournal(tmp_path / 'capacity-rehearsal.sqlite3',
                            order_limit_cents=100, capital_limit_cents=500, daily_loss_cents=500)
    exchange = Exchange(journal)
    broker = BinaryDemoBroker(journal, exchange)
    for number in range(6):
        cid, ticker = f'capacity-{number}', f'CAPACITY-EVENT-{number}'
        journal.reserve(cid, ticker, 1, 35, 2, outcome='yes', action='buy',
                        account_snapshot=broker.snapshot())
        assert broker.submit(cid, quote_snapshot=quote(journal, cid))['state'] == 'terminal'
        assert broker.reconcile_positions()['position_count'] == number + 1
    assert len(journal.accounting()['positions']) == 6
    assert journal.accounting()['capital_at_risk'] < 5
    exchange.positions['UNEXPECTED'] = 1
    with pytest.raises(ValueError, match='external_or_unreconciled_position'):
        broker.reconcile_positions()
    exchange.positions.pop('UNEXPECTED')
    with pytest.raises(ValueError, match='stopped'):
        journal.reserve('never-sent', 'SEVENTH', 1, 35, 2, outcome='yes', action='buy',
                        account_snapshot=broker.snapshot())
    assert exchange.posts == 6
    journal.close()
