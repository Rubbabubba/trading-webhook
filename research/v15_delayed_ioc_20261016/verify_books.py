"""Independently recover entry observations from immutable raw source files.

Does not import the experiment's decision, step, view or accounting functions.
The previously audited raw source reconstruction is shared and remains a source
assumption; this does not independently certify the vendor or broker execution.
"""
import gzip, hashlib, heapq, json, math
from pathlib import Path
from research.reconstructed_markdown_20261016.feed import Feed

ROOT = Path(__file__).parent


def run():
    report = json.loads((ROOT/'review.json').read_text())
    manifest = json.loads((ROOT/'acquisition.json').read_text())['files']
    queue = []; targets = []; feed = Feed(); latest = {}; universe = set(); rows = 0
    for a in report['selected']:
        v = a['decision_view']
        assert v['ticker_receipt_at'] <= v['available_at'] <= a['decision_at']
        assert 0 <= a['decision_at']-v['channel_receipt_at'] <= 1
        for name, path in a['paths'].items():
            if path['state'] == 'benchmark_no_trade':
                continue
            index = len(targets)
            targets.append(dict(ticker=a['ticker'], event_id=a['event_id'], track=name,
                decision_at=a['decision_at'], side=path['side'], limit=path['limit'],
                reported_state=path['state']))
            heapq.heappush(queue, (a['decision_at']+1, index))
    for file_number, receipt in enumerate(manifest, 1):
        p = Path(receipt['path'])
        assert hashlib.sha256(p.read_bytes()).hexdigest() == receipt['sha256']
        for line in gzip.open(p, 'rt', encoding='utf-8'):
            raw = json.loads(line); rows += 1
            observed = feed.ingest(raw); at = feed.available_ns/1e9
            if raw['record_type'] == 'universe':
                universe = {m['ticker'] for m in raw['markets']}
            else:
                ticker = raw['ticker']
                if observed is None:
                    latest.pop(ticker, None)
                else:
                    latest[ticker] = observed
            while queue and queue[0][0] <= at:
                due, index = heapq.heappop(queue); target = targets[index]
                ticker = target['ticker']; r = latest.get(ticker)
                valid = r is not None and ticker in universe and feed.valid(r['key'])
                channel_at = feed.audit.channel_times.get(tuple(r['key'][:2]))/1e9 if valid else None
                valid = valid and 0 <= at-channel_at <= 1
                if at-due>1 or not valid:
                    expected = 'entry_unavailable_or_late'; quote = None
                else:
                    book = feed.audit.books[(tuple(r['key'][:2]), ticker)]['book']
                    opposite = 'no' if target['side']=='yes' else 'yes'
                    best_bid, size = max((p, q) for p, q in book[opposite].items()
                                         if 0<p<100 and q>=1)
                    ask = float(100-best_bid)
                    quote = dict(ask=ask, ask_depth=float(size), channel_at=channel_at,
                                 ticker_receipt_at=r['receipt_at'], available_at=r['available_at'])
                    expected = ('entry_insufficient_depth' if size<1 else
                        'entry_limit_not_marketable_after_slippage' if math.ceil(ask)+1>target['limit']
                        else 'conditional_entry')
                assert expected == target['reported_state'], (target, at, quote, expected)
                target.update(independent_state=expected, observed_at=at, due=due, quote=quote)
        print(json.dumps(dict(independent_file=file_number, records=rows,
                              remaining_entry_checks=len(queue))), flush=True)
    assert not queue
    result = dict(schema='v15_delayed_ioc_independent_book_verification_v1',
        review_sha256=hashlib.sha256((ROOT/'review.json').read_bytes()).hexdigest(),
        source_records=rows, checked_entry_paths=len(targets),
        all_reported_entry_states_reproduced=True, observations=targets,
        actual_orders=0, actual_fills=0, profitability_proven=False,
        limitation='Independent entry-observation loop; shared audited source decoder, not independent vendor or broker verification.')
    with (ROOT/'independent-book-verification.json').open('x') as f:
        json.dump(result, f, indent=2, allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k!='observations'}))


if __name__ == '__main__':
    run()
