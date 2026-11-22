"""Frozen Demo research decision. No broker transport, grants or order authority."""
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
import statistics


MIN_EVENTS = 20
MIN_DAYS = 3
Z = Decimal('3.5')
MAX_HOLDOUT_SECONDS = 60 * 86400


def day(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).date()


def payout(quote, evidence):
    if not evidence or evidence.get('received_at', 0) < quote['ready_at']:
        return None
    market = evidence['market']
    if (market.get('ticker') != quote['ticker'] or market.get('event_ticker') != quote['event']
            or market.get('status') != 'finalized' or market.get('result') not in ('yes', 'no')
            or any(market.get(key) != value for key, value in quote['rules'].items())):
        return None
    return 100 if market['result'] == quote['side'] else 0


def lower_value(wins, count):
    with localcontext() as context:
        context.prec = 32
        n = Decimal(count)
        p = Decimal(wins) / n
        z2 = Z * Z
        return max(Decimal(0), 100 * ((p + z2 / (2 * n))
                   - Z * ((p * (1 - p) + z2 / (4 * n)) / n).sqrt()) / (1 + z2 / n))


def lower_mean(values):
    return statistics.mean(values) - Z * statistics.stdev(values) / Decimal(len(values)).sqrt()


def group(quote):
    return (quote['wallet'], quote['series'], quote['side'])


def admitted(wallet, frozen):
    return Decimal(wallet['ask_cents']) + wallet['fee_cents'] + 1 < Decimal(frozen['lower_cents'])


def evaluate_holdout(rows, frozen, *, now):
    """Rows stay unavailable when a same-entry executable outcome is missing."""
    if not rows:
        return {'state': 'awaiting_first_holdout_entry', 'complete': 0}
    elapsed = now - rows[0]['wallet']['decision_at']
    ready = [r for r in rows if r.get('same_exit_verified') is True]
    days = {day(r['wallet']['decision_at']) for r in ready}
    result = dict(state='collecting_holdout', complete=len(ready), days=len(days),
                  unavailable=len(rows) - len(ready))
    if result['unavailable'] or len(ready) < MIN_EVENTS or len(days) < MIN_DAYS:
        if elapsed >= MAX_HOLDOUT_SECONDS:
            result['state'] = 'insufficient_evidence'
        return result
    comparisons = []
    result['missed_winners'] = 0
    result['avoided_losses'] = 0
    for stress in (1, 2):
        matched, base, control = [], [], []
        for row in ready:
            wallet = row['wallet']
            base_return = Decimal(row['baseline_settlement_net_cents']) - stress
            reference = row['control']
            control_return = (Decimal(row['control_payout_cents']) - Decimal(reference['ask_cents'])
                              - reference['fee_cents'] - stress)
            base.append(base_return)
            control.append(control_return)
            takes = admitted(wallet, frozen)
            matched.append(base_return if takes else Decimal(0))
            if not takes and stress == 1:
                result['missed_winners'] += int(base_return > 0)
                result['avoided_losses'] += int(base_return < 0)
        delta_base = [a - b for a, b in zip(matched, base)]
        delta_ref = [a - b for a, b in zip(matched, control)]
        comparisons.append(dict(stress=stress, shadow=str(sum(matched)), baseline=str(sum(base)),
                                independent=str(sum(control)), lower=str(lower_mean(matched)),
                                lower_vs_baseline=str(lower_mean(delta_base)),
                                lower_vs_independent=str(lower_mean(delta_ref))))
    result['comparisons_cents'] = comparisons
    passed = all(Decimal(c[k]) > 0 for c in comparisons
                 for k in ('shadow', 'lower', 'lower_vs_baseline', 'lower_vs_independent'))
    result['state'] = 'positive_holdout_requires_demo_confirmation' if passed else 'rejected'
    return result


def assess(db, *, now):
    """Freeze only on prior-day receipts, then use first independent parent occurrences."""
    rows = defaultdict(dict)
    evidence = {ticker: json.loads(raw)
                for ticker, raw in db.execute('SELECT ticker,detail FROM outcomes WHERE detail IS NOT NULL')}
    unassigned_execution_gaps = []
    for ident, raw in db.execute('SELECT id,detail FROM pairs ORDER BY rowid'):
        item = json.loads(raw)
        wallet = item.get('wallet')
        if not wallet:
            if type(item.get('submitted_quote_at')) in (float, int):
                unassigned_execution_gaps.append(item['submitted_quote_at'])
            continue
        item['id'] = ident
        rows[group(wallet)].setdefault(wallet['parent'], item)
    frozen = {key: json.loads(value) for key, value in db.execute('SELECT id,detail FROM frozen')}
    decisions = {}
    max_training = 0
    for identity, parents in rows.items():
        key = json.dumps(identity)
        items = sorted(parents.values(), key=lambda r: (r['wallet']['decision_at'], r['id']))
        training = []
        for row in items:
            wallet = row['wallet']
            outcome = evidence.get(wallet['ticker'])
            pay = payout(wallet, outcome)
            if (pay is not None and day(outcome['received_at']) < day(now)
                    and day(wallet['decision_at']) < day(now)):
                training.append((wallet, pay))
        max_training = max(max_training, len(training))
        if key not in frozen and len(training) >= MIN_EVENTS and len(
                {day(w['decision_at']) for w, _ in training}) >= MIN_DAYS:
            frozen[key] = dict(at=now, parents=[w['parent'] for w, _ in training],
                               lower_cents=str(lower_value(sum(p == 100 for _, p in training),
                                                           len(training))))
            db.execute('INSERT INTO frozen VALUES(?,?)', (key, json.dumps(frozen[key], sort_keys=True)))
        if key not in frozen:
            continue
        value = frozen[key]
        exclusions = set(value['parents'])
        holdout = [r for r in items if r['wallet']['first_seen_at'] >= value['at']
                   and r['wallet']['parent'] not in exclusions]
        for row in holdout:
            control = row.get('control')
            if not control or row.get('baseline_settlement_net_cents') is None:
                continue
            pay = payout(control, evidence.get(control['ticker']))
            if pay is not None:
                row['control_payout_cents'] = pay
                row['same_exit_verified'] = True
        previous = db.execute('SELECT detail FROM decisions WHERE id=?', (key,)).fetchone()
        result = json.loads(previous[0]) if previous else evaluate_holdout(holdout, value, now=now)
        gaps = sum(stamp >= value['at'] for stamp in unassigned_execution_gaps)
        if gaps and not previous:
            result['unassigned_execution_gaps'] = gaps
            if result['state'] in ('positive_holdout_requires_demo_confirmation', 'rejected'):
                expired = now - holdout[0]['wallet']['decision_at'] >= MAX_HOLDOUT_SECONDS
                result['state'] = 'insufficient_evidence' if expired else 'collecting_holdout'
        if result['state'] in ('positive_holdout_requires_demo_confirmation', 'rejected', 'insufficient_evidence'):
            db.execute('INSERT OR IGNORE INTO decisions VALUES(?,?)',
                       (key, json.dumps(result, sort_keys=True)))
        decisions[key] = result
    counts = defaultdict(int)
    for record in decisions.values():
        counts[record['state']] += 1
    return dict(max_training=max_training, frozen_groups=len(frozen), decisions=dict(counts),
                max_complete_holdout=max((row['complete'] for row in decisions.values()), default=0))
