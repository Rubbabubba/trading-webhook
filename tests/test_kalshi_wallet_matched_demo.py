from datetime import datetime, timezone
import json

import pytest

from opportunity_lab import kalshi_wallet_matched_demo as matched
from opportunity_lab.kalshi_wallet_matched_decision import evaluate_holdout


def quote(ticker, parent, at, **changes):
    return dict(ticker=ticker, parent=parent, side='yes', series='KXNBAGAME',
                ask_cents='51', received_at=at, ready_at=at,
                expected_settlement=at + 9000, **changes)


def test_matched_reference_excludes_future_quotes_and_shared_parents():
    now = matched.FIRST + 100
    wallet = quote('W', 'NBA:1', now, decision_at=now + 1)
    future = quote('A', 'NBA:2', now + 2)
    shared = quote('B', 'NBA:1', now)
    valid = quote('C', 'NBA:3', now - 1)
    stale = quote('D', 'NBA:4', now - 61)
    assert matched.matching_quote(wallet, [future, shared, valid, stale], set()) == valid
    assert matched.matching_quote(wallet, [future, shared, valid, stale], {'NBA:3'}) is None
    assert matched.normalized_parent('KXNBASPREAD-26NOV23BOSNYK', 'KXNBASPREAD') == 'NBA:26NOV23BOSNYK'


def test_quote_is_demo_executable_and_transport_time_is_checked():
    now = matched.FIRST + 100
    frame = dict(environment='demo', ticker='KXNBAGAME-26NOV23BOSNYK-BOS',
                 side='yes', observed_at=now, ask='0.51', depth='1.00')
    frame['market'] = dict(ticker=frame['ticker'], event_ticker='KXNBAGAME-26NOV23BOSNYK',
                           expected_expiration_time=datetime.fromtimestamp(now + 9000, timezone.utc).isoformat(),
                           rules_primary='A', rules_secondary='B')
    basis = dict(series_ticker='KXNBAGAME', fee_multiplier='1')
    assert matched.checked_quote(frame, basis, ready=now + 1)['fee_cents'] == 2
    with pytest.raises(ValueError, match='timely_demo_book_required'):
        matched.checked_quote(frame, basis, ready=now + 6)
    with pytest.raises(ValueError, match='demo_book_required'):
        matched.checked_quote(dict(frame, environment='production'), basis, ready=now)
    with pytest.raises(ValueError, match='one_contract_book_required'):
        matched.checked_quote(dict(frame, depth='0.99'), basis, ready=now)


def test_first_finalized_settlement_is_immutable(tmp_path):
    now = matched.FIRST + 200
    ticker = 'KXNBAGAME-26NOV23BOSNYK-BOS'
    db = matched.open_archive(tmp_path)
    db.execute('INSERT INTO outcomes VALUES(?,0,NULL)', (ticker,))
    calls = []

    def read(url):
        calls.append(url)
        status = 'active' if len(calls) == 1 else 'finalized'
        result = {'ticker': ticker, 'status': status, 'result': 'yes'}
        return {'market': result}, now, now, None, None

    matched.observe_settlement(db, read=read, clock=lambda: now)
    matched.observe_settlement(db, read=read, clock=lambda: now)
    assert len(calls) == 1
    now += 300
    matched.observe_settlement(db, read=read, clock=lambda: now)
    now += 300
    matched.observe_settlement(db, read=read, clock=lambda: now)
    evidence = json.loads(db.execute('SELECT detail FROM outcomes').fetchone()[0])
    assert len(calls) == 2 and all(url.startswith(matched.DEMO) for url in calls)
    assert evidence['received_at'] == now - 300
    db.close()


def test_awaiting_start_has_no_market_calls(tmp_path):
    root = tmp_path / matched.DIRECTORY
    db = matched.open_archive(root)

    def no_calls(*args, **kwargs):
        pytest.fail('unexpected market call')

    report = matched.run_tick(tmp_path, db, read=no_calls, clock=lambda: matched.FIRST - 1)
    assert report['state'] == 'awaiting_start'
    assert matched.read_status(root / 'status.json', now=matched.FIRST)['pairs'] == {}
    assert matched.read_status(root / 'status.json', now=matched.FIRST + 200) is None
    db.close()


def test_frozen_holdout_needs_complete_three_day_evidence_and_beats_both_controls():
    first = matched.FIRST + 100
    picks = []
    for i in range(20):
        at = first + i % 3 * 86400
        wallet = quote('W', str(i), at, decision_at=at)
        control = quote('C', 'ref' + str(i), at)
        wallet.update(fee_cents=2, ask_cents='20' if i < 10 else '85')
        control.update(fee_cents=2)
        picks.append(dict(wallet=wallet, control=control, control_payout_cents=0,
                          baseline_settlement_net_cents='78' if i < 10 else '-87',
                          same_exit_verified=True))
    frozen = {'lower_cents': '60'}
    now = first + 4 * 86400
    result = evaluate_holdout(picks, frozen, now=now)
    assert result['state'] == 'positive_holdout_requires_demo_confirmation'
    assert result['avoided_losses'] == 10
    picks[0]['same_exit_verified'] = False
    assert evaluate_holdout(picks, frozen, now=now)['state'] == 'collecting_holdout'
    assert evaluate_holdout(picks, frozen, now=first + 61 * 86400)['state'] == 'insufficient_evidence'
