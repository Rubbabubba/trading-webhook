"""Separate receipt-clock, authority and cash checks on the retained result."""
import hashlib, json
from pathlib import Path

ROOT = Path(__file__).parent


def run():
    report = json.loads((ROOT/'review.json').read_text())
    registration = (ROOT/'registration.json').read_bytes()
    assert report['registration_sha256'] == hashlib.sha256(registration).hexdigest()
    assert report['actual_orders'] == report['actual_fills'] == 0
    assert not report['execution_enabled'] and not report['profitability_proven']
    selected = report['selected']
    assert len(selected) == len({a['event_id'] for a in selected})
    windows = views = entries = exits = 0
    for a in selected:
        assert report['decision_window'][0] <= a['decision_at'] <= report['decision_window'][1]
        assert a['signal_details']['v15_ioc']['price_cents'] == a['signal_details']['frozen_v12']['price_cents']+2
        for name, p in a['paths'].items():
            if p['state'] != 'conditional_entry':
                continue
            assert 1 <= p['entry_at']-a['decision_at'] <= 2
            assert p['entry_cost_price'] <= p['limit']
            assert p['entry_view']['ask_depth'] >= 1
            entries += 1
            for h in ('5','30','300'):
                e = p['exits'][h]
                if e['state'] != 'conditional_exit':
                    continue
                assert int(h) <= e['at']-p['entry_at'] <= int(h)+5
                assert e['view']['bid_depth'] >= 1
                exits += 1
            for v, at in [(p['entry_view'], p['entry_at'])]+[
                    (e['view'], e['at']) for e in p['exits'].values() if e['state']=='conditional_exit']:
                assert v['ticker_receipt_at'] <= v['available_at'] <= at
                assert 0 <= at-v['channel_receipt_at'] <= 1
                views += 1
        windows += 1
    for name, track in report['tracks'].items():
        paths = [a['paths'][name] for a in selected if a['paths'][name]['state']=='conditional_entry']
        for h, summary in track['horizons'].items():
            complete = [p for p in paths if p['exits'][h]['state']=='conditional_exit']
            unknown = [p for p in paths if p['exits'][h]['state']!='conditional_exit']
            for stress in (0,2,5):
                # Independent debit/credit formulation, rather than importing
                # the replay's accounting function.
                net = sum(p['exits'][h]['proceeds_price']-(2+stress) for p in complete)
                net -= sum(p['entry_cost_price']+(2+stress) for p in complete)
                worst = -sum(p['entry_cost_price']+2+stress for p in unknown)
                s = summary['scenarios'][str(stress)]
                assert s['complete_net_cents'] == (net if complete else None)
                assert s['combined_with_unresolved_stress_cents'] == (net+worst if paths else None)
    receipt = dict(schema='v15_delayed_ioc_independent_accounting_v1',
        reviewed_sha256=hashlib.sha256((ROOT/'review.json').read_bytes()).hexdigest(),
        official_event_windows_checked=windows, conditional_entries_checked=entries,
        conditional_exits_checked=exits, causal_quote_views_checked=views,
        result='passed_retained_result_consistency_only',
        actual_orders=0, actual_fills=0, profitability_proven=False,
        note='Consistency is not verification of external quote truth, historical fees, fills, independence or profitability.')
    with (ROOT/'independent-accounting.json').open('x') as f:
        json.dump(receipt, f, indent=2, allow_nan=False)
    print(json.dumps(receipt))


if __name__ == '__main__':
    run()
