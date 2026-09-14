"""Public GET-only collector for a single shared YES/NO shadow portfolio."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

from .kalshi_shadow import Shadow, cost, price_book

BASE = 'https://external-api.kalshi.com/trade-api/v2'


def epoch(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timezone_required')
    return result.timestamp()


def identity(m):
    return tuple(m.get(k) for k in ('ticker', 'event_ticker', 'rules_primary', 'rules_secondary'))


def choose_universe(cache, now, stop, count):
    """Deterministic category round-robin; volume ranks within each category."""
    groups = defaultdict(list)
    for ticker, context in cache.items():
        m, series = context['market'], context['series']
        try:
            eligible = (ticker == m['ticker'] and not context.get('changed')
                        and m.get('market_type') == 'binary' and m.get('status') == 'active'
                        and m.get('rules_primary') and m.get('event_ticker')
                        and now + 1800 < epoch(m['close_time']) <= stop
                        and series.get('fee_type') in ('quadratic', 'quadratic_with_maker_fees'))
            volume = float(m.get('volume_24h_fp') or m.get('volume_24h') or 0)
            if eligible and math.isfinite(volume):
                groups[series.get('category', 'Unknown')].append((-volume, ticker, context))
        except (KeyError, TypeError, ValueError):
            continue
    for group in groups.values():
        group.sort(key=lambda item: item[:2])
    selected = {}
    while groups and len(selected) < count:
        for category in sorted(list(groups)):
            _, ticker, context = groups[category].pop(0)
            selected[ticker] = context
            if not groups[category]:
                del groups[category]
            if len(selected) == count:
                break
    return selected


def entry_signal(history, frame, config):
    """Frozen hypothesis: 4c drop from a 4-7m midpoint, >=2c modeled rebound."""
    at = frame['received_at']
    old = [mid for when, mid in history if 240 <= at - when <= 420]
    if not old:
        return None
    coefficient = Decimal(frame['fee_coefficient'])
    for side in ('yes', 'no'):
        bid, ask, _, size = price_book(frame, side)
        reference = old[-1] if side == 'yes' else 1 - old[-1]
        if (size >= 1 and ask - bid <= Fraction(5, 100)
                and reference - (bid + ask) / 2 >= Fraction(4, 100)
                and cost(reference, coefficient, config['slippage_cents'], False)
                - cost(ask, coefficient, config['slippage_cents'], True) >= 2):
            return {'side': side, 'strategy_id': config['strategy_id']}
    return None


class PublicBooks:
    def __init__(self, db):
        self.db, self.next_at = db, 0
        db.execute('CREATE TABLE IF NOT EXISTS public_responses(id INTEGER PRIMARY KEY, at REAL, path TEXT, body TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS collector_observations(at REAL,ticker TEXT,result TEXT)')

    def get(self, kind, ticker, book=False):
        if kind not in ('markets', 'series') or (book and kind != 'markets'):
            raise ValueError('public_route_only')
        path = '/' + kind + '/' + quote(ticker, safe='') + ('/orderbook?depth=20' if book else '')
        time.sleep(max(0, self.next_at - time.monotonic()))
        started = time.monotonic()
        try:
            with urlopen(Request(BASE + path, headers={'Accept': 'application/json'}), timeout=5) as response:
                data = json.load(response)
                age = float(response.headers.get('Age', '0'))
            at = time.time()
            if not math.isfinite(age) or age < 0 or age > 2 or time.monotonic() - started > 2:
                raise ValueError('stale_transport')
            cursor = self.db.execute('INSERT INTO public_responses(at,path,body) VALUES(?,?,?)',
                                     (at, path, json.dumps(data)))
            return data, at, str(cursor.lastrowid)
        finally:
            self.next_at = time.monotonic() + 2


class Coordinator:
    def __init__(self, engine, universe, stop_at):
        self.engine, self.universe, self.stop_at = engine, universe, stop_at
        engine.db.executescript('CREATE TABLE IF NOT EXISTS history(at REAL,ticker TEXT,mid TEXT);'
                               'CREATE TABLE IF NOT EXISTS runtime(name TEXT PRIMARY KEY,value TEXT);')

    def observe(self, frame, market, series, *, now):
        ticker = frame['ticker']
        expected = self.universe[ticker]
        if identity(market) != identity(expected['market']):
            raise ValueError('market_identity_changed')
        if market.get('status') != 'active' or market.get('market_type') != 'binary':
            raise ValueError('market_unavailable_inventory_retained')
        if (series.get('ticker') != expected['series']['ticker']
                or series.get('fee_type') not in ('quadratic', 'quadratic_with_maker_fees')):
            raise ValueError('unsupported_series')
        coefficient = Decimal('.07') * Decimal(str(series['fee_multiplier']))
        if not coefficient.is_finite() or not 0 <= coefficient <= Decimal('.14'):
            raise ValueError('unsupported_fee')
        frame = dict(frame, fee_coefficient=str(coefficient.normalize()))
        bid, ask, _, _ = price_book(frame, 'yes')
        state = self.engine.state()
        active = state['position'] or state['pending']
        if active and active['ticker'] != ticker:
            return {'action': 'portfolio_busy', 'execution_enabled': False}
        if state['realized_cents'] <= -1000:
            self.engine.db.execute("INSERT OR IGNORE INTO runtime VALUES('loss_stop','true')")
        stopped = self.engine.db.execute("SELECT 1 FROM runtime WHERE name='loss_stop'").fetchone()
        closing = now >= min(epoch(market['close_time']) - 1200, self.stop_at - 1200)
        if closing:
            frame['signal_valid'] = False
        rows = self.engine.db.execute('SELECT at,mid FROM history WHERE ticker=? AND at>=? ORDER BY at',
                                      (ticker, now - 600)).fetchall()
        history = [(at, Fraction(mid)) for at, mid in rows]
        signal = None if closing or stopped or frame.get('signal_valid') is False or state['position'] else entry_signal(history, frame, self.engine.config)
        if state['pending'] and state['pending']['kind'] == 'entry' and (
                signal is None or signal['side'] != state['pending']['side']):
            frame['signal_valid'] = False
        result = self.engine.step(frame, now=now, signal=signal)
        self.engine.db.execute('INSERT INTO history VALUES(?,?,?)', (frame['received_at'], ticker, str((bid + ask) / 2)))
        self.engine.db.execute('DELETE FROM history WHERE at<?', (now - 600,))
        return result


def run(config_path, once=False):
    config = json.loads(Path(config_path).read_text())
    if config['execution_enabled'] is not False:
        raise ValueError('paper_only')
    for path, digest in config['source_sha256'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
            raise ValueError('frozen_source_changed')
    stop = epoch(config['stop_at'])
    output = Path(config['output']); output.mkdir(parents=True, exist_ok=True)
    # OS lock lives for the process lifetime; never remove another worker's lock.
    lock = (output / 'worker.lock').open('a+b')
    lock.seek(0); lock.write(b'0'); lock.flush(); lock.seek(0)
    import msvcrt
    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
    engine = Shadow(output / 'shadow.sqlite3', config['engine'])
    try:
        encoded = json.dumps(config, sort_keys=True)
        engine.db.execute('INSERT OR IGNORE INTO settings VALUES(2,?)', (encoded,))
        if engine.db.execute('SELECT detail FROM settings WHERE id=2').fetchone()[0] != encoded:
            raise ValueError('frozen_manifest_changed')
        saved = engine.db.execute('SELECT detail FROM settings WHERE id=3').fetchone()
        if saved:
            universe = json.loads(saved[0])
        else:
            source = Path(config['universe_source'])
            if time.time() - source.stat().st_mtime > 900:
                raise ValueError('discovery_snapshot_stale')
            universe = choose_universe(json.loads(source.read_text()), time.time(), stop, 12)
            if not universe:
                raise ValueError('empty_universe')
            engine.db.execute('INSERT INTO settings VALUES(3,?)', (json.dumps(universe, sort_keys=True),))
        coordinator = Coordinator(engine, universe, stop)
        client = PublicBooks(engine.db); index = 0
        while time.time() < stop:
            state = engine.state(); active = state['position'] or state['pending']
            ticker = active['ticker'] if active else sorted(universe)[index % len(universe)]
            if not active:
                index += 1
            result = {'action': 'paused', 'execution_enabled': False}
            try:
                if active or not (output / 'PAUSE').exists():
                    market, _, _ = client.get('markets', ticker)
                    series, _, _ = client.get('series', universe[ticker]['series']['ticker'])
                    book, at, rid = client.get('markets', ticker, book=True)
                    frame = dict(book, ticker=ticker, received_at=at, book_id=rid)
                    if (output / 'PAUSE').exists():
                        frame['signal_valid'] = False
                    result = coordinator.observe(frame, market['market'], series['series'], now=time.time())
            except Exception as exc:
                result = {'action': 'observation_rejected', 'error_type': type(exc).__name__,
                          'execution_enabled': False}
            engine.db.execute('INSERT INTO collector_observations VALUES(?,?,?)',
                              (time.time(), ticker, json.dumps(result)))
            status = dict(at=datetime.now(timezone.utc).isoformat(), execution_enabled=False,
                          strategy_id=config['engine']['strategy_id'], universe_size=len(universe),
                          phase='running', ticker=ticker,
                          categories=sorted({v['series'].get('category', 'Unknown') for v in universe.values()}),
                          state=engine.state(), last_result=result, stop_at=config['stop_at'])
            temporary = output / 'status.tmp'
            temporary.write_text(json.dumps(status, indent=2)); temporary.replace(output / 'status.json')
            if once:
                break
            time.sleep(2)
        if time.time() >= stop:
            (output / 'status.json').write_text(json.dumps(dict(
                at=datetime.now(timezone.utc).isoformat(), phase='stopped', execution_enabled=False,
                state=engine.state(), stop_at=config['stop_at'],
                note='Any unresolved inventory remains recorded; no assumed liquidation.'), indent=2))
    finally:
        engine.close(); lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    run(args.config, args.once)
