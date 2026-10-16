"""Causal one-second source views for a preregistered resting-order test."""
import gzip, hashlib, json, math
from collections import Counter, deque
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from research.reconstructed_markdown_20261016.feed import Feed
from research.v15_delayed_ioc_20261016.replay import decision

ROOT = Path(__file__).parent


def run():
    spec = json.loads((ROOT/'registration.json').read_text())
    files = json.loads((ROOT/'acquisition.json').read_text())['files']
    cache = ROOT.parents[2]/'research/maker-tape-cache-20261016'
    cache.mkdir(exist_ok=True)
    output = cache/'views.jsonl.gz'
    if output.exists():
        raise ValueError('retain_existing_source_views')
    feed = Feed(); counts = Counter(); latest = {}; histories = {}; universe = set()
    seen = set(); tracked = {}; attempts = []; grid = None; window = [None, None]; fault = None

    def view(ticker, at):
        r = latest.get(ticker)
        if r is None or ticker not in universe or not feed.valid(r['key']):
            return None
        channel_at = feed.audit.channel_times[tuple(r['key'][:2])]/1e9
        if not 0<=at-channel_at<=1:
            return None
        book = feed.audit.books[(tuple(r['key'][:2]), ticker)]['book']
        y = max((p,q) for p,q in book['yes'].items() if 0<p<100 and q>=1)
        n = max((p,q) for p,q in book['no'].items() if 0<p<100 and q>=1)
        return dict(yes_bid=float(y[0]), yes_ask=float(100-n[0]),
            yes_bid_size=float(y[1]), no_bid_size=float(n[1]),
            channel_at=channel_at, ticker_receipt_at=r['receipt_at'],
            available_at=r['available_at'], epoch_key=list(r['key']))

    def sample(at, enabled, out):
        counts['grid_samples'] += 1
        if counts['grid_samples']>spec['limits']['maximum_grid_samples']:
            raise ValueError('grid_capacity')
        for ticker, until in list(tracked.items()):
            if at>until:
                del tracked[ticker]; continue
            v = view(ticker, at)
            counts['cached_views'] += 1
            if counts['cached_views']>spec['limits']['maximum_cached_views']:
                raise ValueError('view_capacity')
            out.write(json.dumps(dict(ticker=ticker, at=at, view=v), separators=(',',':'))+'\n')
        if at%5:
            return
        for ticker, r in list(latest.items()):
            v = view(ticker, at)
            if v is None:
                counts['unavailable_decision_views'] += 1; continue
            h = deque(x for x in histories.get(ticker, ()) if x[0]>=at-300 and x[2]==r['key'])
            histories[ticker] = h
            frame = dict(received_at=at, orderbook_fp={
                'yes_dollars': [[str(Decimal(str(v['yes_bid']))/100),str(v['yes_bid_size'])]],
                'no_dollars': [[str(1-Decimal(str(v['yes_ask']))/100),str(v['no_bid_size'])]]})
            if enabled and ticker not in seen and len(attempts)<spec['limits']['maximum_preliminary_tickers']:
                signals, reason = decision([(t,m) for t,m,k in h], frame)
                counts['decision_'+reason] += 1
                if signals:
                    seen.add(ticker); tracked[ticker] = at+370
                    attempts.append(dict(ticker=ticker, decision_at=at, activation_at=at+1,
                        cancel_request_at=at+61, cancel_ack_at=at+62,
                        decision_view=v, signal_details=signals))
            h.append((at, Fraction(str((v['yes_bid']+v['yes_ask'])/200)), r['key']))

    with gzip.open(output, 'xt', encoding='utf-8') as out:
        try:
            for i, receipt in enumerate(files):
                p = Path(receipt['path'])
                if hashlib.sha256(p.read_bytes()).hexdigest()!=receipt['sha256']:
                    raise ValueError('source_hash_changed')
                enabled = spec['global_file_numbers'][i] in spec['decision_files']
                for line in gzip.open(p,'rt',encoding='utf-8'):
                    counts['records'] += 1
                    if counts['records']>spec['limits']['maximum_records']:
                        raise ValueError('record_capacity')
                    raw = json.loads(line)
                    at = max(feed.available_ns or raw['received_ts_ns'],raw['received_ts_ns'])/1e9
                    if grid is None:
                        grid = math.ceil(at)
                    if enabled:
                        if window[0] is None:window[0]=at
                        window[1]=at
                    # The later row is not ingested until earlier grid samples
                    # and decisions have finished. No future-state lookup.
                    while grid<at:
                        sample(grid, enabled and grid>=window[0] if enabled else False, out)
                        grid += 1
                    r = feed.ingest(raw)
                    if raw['record_type']=='universe':
                        universe={m['ticker'] for m in raw['markets']}
                    elif not raw['ticker'].startswith('KXMVE'):
                        if r is None:latest.pop(raw['ticker'],None)
                        else:latest[raw['ticker']]=r
                print(json.dumps(dict(file_complete=i+1, global_file=spec['global_file_numbers'][i],
                    records=counts['records'], attempts=len(attempts), cached_views=counts['cached_views'])),flush=True)
        except (ValueError,KeyError,TypeError,ArithmeticError,OSError) as e:
            fault=dict(reason=type(e).__name__+': '+str(e),record=counts['records'])
    result=dict(schema='v15_resting_source_books_v1',
        registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),
        complete_registered_window=fault is None, source_fault=fault, decision_window=window,
        counts=dict(counts), feed_counts=dict(feed.counts), audit_counts=dict(feed.audit.counts),
        views=dict(path=str(output),sha256=hashlib.sha256(output.read_bytes()).hexdigest(),bytes=output.stat().st_size),
        preliminary_attempts=attempts, actual_orders=0, actual_fills=0, execution_enabled=False,
        profitability_proven=False)
    with (ROOT/'preliminary.json').open('x') as f:json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k!='preliminary_attempts'}))


if __name__=='__main__':run()
