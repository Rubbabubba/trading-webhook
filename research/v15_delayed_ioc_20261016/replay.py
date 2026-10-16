"""Fixed delayed IOC screen. Displayed-book paths are conditional, never fills."""
import gzip, hashlib, heapq, json, math
from collections import Counter, deque
from pathlib import Path
from decimal import Decimal
from fractions import Fraction

from research.reconstructed_markdown_20261016.feed import Feed
from opportunity_lab.kalshi_maker_v12 import shadow_decision as v12
from opportunity_lab.kalshi_maker_v10 import shadow_decision as v10

ROOT = Path(__file__).parent
HORIZONS = (5, 30, 300)


def decision(history, frame):
    base, reason = v12(history, frame)
    if not base or Decimal(base['gross_edge_cents']) < 5:
        return None, reason if not base else 'v15_gross_edge_below_five'
    benchmark, _ = v10(history, frame)
    return {'v15_ioc': {**base, 'price_cents': base['price_cents'] + 2},
            'frozen_v12': base, 'frozen_v10': benchmark}, 'signal'


def step(path, phase, due, at, view):
    """One observation per IOC or exit; no retry until a favorable price."""
    if phase == 'entry':
        if at - due > 1 or view is None:
            path['state'] = 'entry_unavailable_or_late'
        elif view['ask_depth'] < 1:
            path['state'] = 'entry_insufficient_depth'
        elif math.ceil(view['ask']) + 1 > path['limit']:
            path['state'] = 'entry_limit_not_marketable_after_slippage'
        else:
            path.update(state='conditional_entry', entry_at=at,
                        entry_ask=view['ask'], entry_cost_price=math.ceil(view['ask']) + 1,
                        entry_view=view, exits={})
        return
    horizon = int(phase)
    if at - due > 5 or view is None or view['bid_depth'] < 1:
        path['exits'][str(horizon)] = {'state': 'unresolved_exit', 'observed_at': at}
    else:
        path['exits'][str(horizon)] = dict(state='conditional_exit', at=at,
            bid=view['bid'], proceeds_price=max(0, math.floor(view['bid']) - 1), view=view)


def run():
    spec = json.loads((ROOT / 'registration.json').read_text())
    files = json.loads((ROOT / 'acquisition.json').read_text())['files']
    feed = Feed(); counts = Counter(); latest = {}; histories = {}; universe = None
    attempts = []; seen = set(); queue = []; grid = None; window = None; fault = None

    def view(ticker, side, at):
        r = latest.get(ticker)
        if r is None or universe is None or ticker not in universe or not feed.valid(r['key']):
            return None
        received = feed.audit.channel_times.get(tuple(r['key'][:2]))
        if received is None or not 0 <= at - received / 1e9 <= 1:
            return None
        book = feed.audit.books[(tuple(r['key'][:2]), ticker)]['book']
        y = max((p, n) for p, n in book['yes'].items() if 0 < p < 100 and n >= 1)
        n = max((p, q) for p, q in book['no'].items() if 0 < p < 100 and q >= 1)
        own, other = (y, n) if side == 'yes' else (n, y)
        return dict(bid=float(own[0]), ask=float(100-other[0]),
                    bid_depth=float(own[1]), ask_depth=float(other[1]),
                    ticker_receipt_at=r['receipt_at'], channel_receipt_at=received/1e9,
                    available_at=r['available_at'], epoch_key=list(r['key']))

    def decide(at, enabled):
        for ticker, r in list(latest.items()):
            v = view(ticker, 'yes', at)
            if v is None:
                counts['unavailable_grid_views'] += 1
                continue
            history = histories.setdefault(ticker, deque())
            while history and (history[0][0] < at-300 or history[0][2] != r['key']):
                history.popleft()
            frame = dict(received_at=at, orderbook_fp={
                'yes_dollars': [[str(Decimal(str(v['bid']))/100), str(v['bid_depth'])]],
                'no_dollars': [[str(1-Decimal(str(v['ask']))/100), str(v['ask_depth'])]]})
            if enabled and ticker not in seen and len(attempts) < spec['limits']['maximum_preliminary_tickers']:
                tracks, reason = decision([(t, m) for t, m, k in history], frame)
                counts['decision_' + reason] += 1
                if tracks:
                    seen.add(ticker); index = len(attempts)
                    a = dict(ticker=ticker, decision_at=at, decision_view=v,
                             signal_details=tracks, paths={})
                    for name, signal in tracks.items():
                        if signal is None:
                            a['paths'][name] = {'state': 'benchmark_no_trade'}
                        else:
                            a['paths'][name] = dict(state='awaiting_entry',
                                side=signal['outcome'], limit=signal['price_cents'])
                            heapq.heappush(queue, (at+1, index, name, 'entry'))
                    attempts.append(a)
            history.append((at, Fraction(str((v['bid']+v['ask'])/200)), r['key']))

    try:
        for i, receipt in enumerate(files):
            p = Path(receipt['path'])
            if hashlib.sha256(p.read_bytes()).hexdigest() != receipt['sha256']:
                raise ValueError('source_hash_changed')
            for line in gzip.open(p, 'rt', encoding='utf-8'):
                counts['records'] += 1
                if counts['records'] > spec['limits']['maximum_records']:
                    raise ValueError('record_cap')
                raw = json.loads(line)
                at = max(feed.available_ns or raw['received_ts_ns'], raw['received_ts_ns'])/1e9
                if grid is None:
                    grid = math.ceil(at/5)*5
                if i >= spec['warmup_files'] and window is None:
                    window = at
                # Decisions precede the first later-available row. Never feed
                # a future update into the earlier grid decision.
                while grid < at:
                    decide(grid, window is not None and grid >= window)
                    grid += 5
                r = feed.ingest(raw)
                if raw['record_type'] == 'universe':
                    universe = {m['ticker'] for m in raw['markets']}
                elif not raw['ticker'].startswith('KXMVE'):
                    if r is None:
                        latest.pop(raw['ticker'], None)
                    else:
                        latest[raw['ticker']] = r
                while queue and queue[0][0] <= at:
                    due, index, name, phase = heapq.heappop(queue)
                    a = attempts[index]; path = a['paths'][name]
                    v = view(a['ticker'], path['side'], at)
                    step(path, phase, due, at, v)
                    if phase == 'entry' and path['state'] == 'conditional_entry':
                        for horizon in HORIZONS:
                            heapq.heappush(queue, (at+horizon, index, name, str(horizon)))
            print(json.dumps(dict(file_complete=i+1, global_file=spec['global_file_numbers'][i],
                records=counts['records'], preliminary_attempts=len(attempts))), flush=True)
    except (ValueError, KeyError, TypeError, ArithmeticError, OSError) as e:
        fault = dict(reason=type(e).__name__+': '+str(e), record=counts['records'])
    for a in attempts:
        for path in a['paths'].values():
            if path['state'] == 'awaiting_entry':
                path['state'] = 'entry_unobserved_capture_end'
            if path['state'] == 'conditional_entry':
                for h in HORIZONS:
                    path['exits'].setdefault(str(h), {'state': 'unresolved_capture_end'})
    result = dict(schema='v15_delayed_ioc_preliminary_v1',
        registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),
        complete_registered_window=fault is None, source_fault=fault,
        decision_window=[window, feed.available_ns/1e9 if feed.available_ns else None],
        counts=dict(counts), feed_counts=dict(feed.counts), audit_counts=dict(feed.audit.counts),
        preliminary_attempts=attempts, actual_orders=0, actual_fills=0,
        execution_enabled=False, profitability_proven=False)
    with (ROOT/'preliminary.json').open('x') as f:
        json.dump(result, f, indent=2, allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k != 'preliminary_attempts'}))


if __name__ == '__main__':
    run()
