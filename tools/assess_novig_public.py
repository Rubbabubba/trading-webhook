"""Download immutable copies of recent anonymous Novig data; assess activity only."""
import csv
from decimal import Decimal
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from opportunity_lab.sports_capture_v3 import request

BASE = 'https://data.novig.com/reporting/trade-data/'


def summarize(raw):
    totals = {}
    for row in csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))):
        if row['side'] != 'TAKER' or row['tradeType'] != 'STRAIGHT':
            continue
        key = row['league'] or 'unknown'
        total = totals.setdefault(key, {'trades': 0, 'contracts': Decimal(0), 'taker_stake': Decimal(0), 'markets': set()})
        qty, cost = Decimal(row['qty']), Decimal(row['cost'])
        if qty <= 0 or not 0 <= cost <= qty:
            raise ValueError('Invalid quantity/cost')
        total['trades'] += 1
        total['contracts'] += qty
        total['taker_stake'] += cost
        total['markets'].add(row['marketId'])
    return {k: {**v, 'contracts': str(v['contracts']), 'taker_stake': str(v['taker_stake']), 'markets': len(v['markets'])} for k, v in totals.items()}


def main():
    output = Path('sports_paper/novig_public_20260910')
    output.mkdir(parents=True, exist_ok=True)
    provenance = []
    def download(path):
        response = request(BASE + path)
        if response['error']:
            raise ValueError(response['error'])
        raw = response.pop('raw')
        digest = hashlib.sha256(raw).hexdigest()
        (output / (path.replace('/', '_') + '.' + digest[:12] + '.gz')).write_bytes(gzip.compress(raw))
        provenance.append({**response, 'sha256': digest})
        return raw
    index = json.loads(download('index.json'))
    results = {}
    for date in sorted(index['dates'])[-7:]:
        results[date] = summarize(download(date + '/trades.csv'))
    # Market census is useful context; zero-volume listings are not liquidity.
    for date in sorted(index.get('marketDates', []))[-1:]:
        download(date + '/markets.csv')
    (output / 'assessment.json').write_text(json.dumps({'days': results, 'sources': provenance}, indent=2))
    lines = ['# Novig public-data assessment', '', 'Executed straight trades only, counting TAKER rows once. Contracts pay $1; taker stake excludes fees. This is historical activity, not order-book depth, simulated fills or profitability.', '', '| Date | League | Trades | Contracts | Taker stake ($) | Active markets |', '|---|---|---:|---:|---:|---:|']
    for date, leagues in results.items():
        for league, row in sorted(leagues.items()):
            lines.append(f"| {date} | {league} | {row['trades']} | {row['contracts']} | {row['taker_stake']} | {row['markets']} |")
    lines += ['', 'Source: https://docs.novig.com/api-reference/trade-data and the downloaded files documented in assessment.json.', '', 'Next integration requirement: Novig-issued API credentials for live quotes/order books. Public files contain anonymous market IDs, so named-game cross-venue matching is not established. No live Novig connection or orders are configured.']
    (output / 'assessment.md').write_text('\n'.join(lines), encoding='utf-8')
    print(json.dumps({'days': list(results), 'leagues': sorted({k for v in results.values() for k in v}), 'report': str(output / 'assessment.md')}))


if __name__ == '__main__':
    main()
