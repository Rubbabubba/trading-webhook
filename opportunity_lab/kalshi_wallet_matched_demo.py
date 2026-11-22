"""Bounded independent Demo quotes and contemporaneous wallet comparison receipts."""
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import time

from .kalshi_wallet_monitor import DEMO, TICKER, public_get, read_quote, read_fee
from .kalshi_wallet_matched_decision import assess, payout
from .kalshi_wallet_profitability import fee


DIRECTORY = 'wallet-matched-demo-v1'
FIRST = datetime(2026, 11, 23, tzinfo=timezone.utc).timestamp()
SERIES = ('KXNBAGAME', 'KXNBASPREAD', 'KXNBATOTAL', 'KXNFLGAME', 'KXNFLSPREAD',
          'KXNFLTOTAL', 'KXNHLGAME')
MAX_BYTES = 64 * 1024 * 1024
FREE_FLOOR = 128 * 1024 * 1024
MAX_WINDOW = 60
PAGE_SIZE = 30
REGISTRATION_SHA256 = '410bd4a25455ad596f2f245bb14b30a5ce2e71e8e08b9aec0bdadff85df63399'


def read_status(path, *, now=None):
    try:
        with Path(path).open('rb') as stream:
            data = stream.read(4097)
        if len(data) > 4096:
            return None
        status = json.loads(data)
        age = (time.time() if now is None else now) - status['at']
        if (status['schema'] != 'wallet_matched_demo_v1' or status['environment'] != 'demo'
                or not 0 <= age <= 180):
            return None
        return status
    except (OSError, ValueError, TypeError, KeyError):
        return None


def utc_date(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).date()


def normalized_parent(event, series):
    prefix = series + '-'
    if series not in SERIES or not event.startswith(prefix):
        raise ValueError('unverified_demo_parent')
    suffix = event.removeprefix(prefix)
    league = next(name for name in ('NBA', 'NFL', 'NHL') if series.startswith('KX' + name))
    if not suffix or not TICKER.fullmatch(suffix):
        raise ValueError('unverified_demo_parent')
    return league + ':' + suffix


def stratum(quote):
    duration = quote['expected_settlement'] - quote['received_at']
    if duration <= 0:
        raise ValueError('unknown_settlement_horizon')
    return (quote['series'], utc_date(quote['received_at']), Decimal(quote['ask_cents']) // 5,
            int(duration // 3600))


def matching_quote(wallet, candidates, used):
    wanted = stratum(wallet)
    ready = wallet['decision_at']
    eligible = [
        q for q in candidates
        if q['parent'] not in used and q['parent'] != wallet['parent']
        and stratum(q) == wanted and q['received_at'] <= q['ready_at'] <= ready
        and abs(q['received_at'] - wallet['received_at']) <= MAX_WINDOW
    ]
    return min(eligible, key=lambda q: (abs(q['received_at'] - wallet['received_at']),
                                        q['ticker'], q['side']), default=None)


def checked_quote(frame, basis, *, ready):
    if frame.get('environment') != 'demo':
        raise ValueError('demo_book_required')
    market = frame['market']
    ticker, side = frame['ticker'], frame['side']
    event, series = market['event_ticker'], basis['series_ticker']
    if (ticker != market['ticker'] or not ticker.startswith(event + '-')
            or side not in ('yes', 'no') or not 0 <= ready - frame['observed_at'] <= 5):
        raise ValueError('timely_demo_book_required')
    ask, size = Decimal(frame['ask']) * 100, Decimal(frame['depth'])
    if not ask.is_finite() or not size.is_finite() or not 0 < ask < 100 or size < 1:
        raise ValueError('one_contract_book_required')
    expected = datetime.fromisoformat(market['expected_expiration_time'])
    if expected.utcoffset() is None:
        raise ValueError('unknown_settlement_horizon')
    return dict(ticker=ticker, event=event, series=series, side=side,
                parent=normalized_parent(event, series), ask_cents=str(ask),
                depth=str(size), fee_cents=fee(ask, basis['fee_multiplier']),
                received_at=frame['observed_at'], ready_at=ready,
                expected_settlement=expected.timestamp(),
                frame_sha256=hashlib.sha256(json.dumps(frame, sort_keys=True).encode()).hexdigest(),
                rules={key: market[key] for key in ('rules_primary', 'rules_secondary')})


def capture_control(slot, *, read, clock):
    series = SERIES[slot % len(SERIES)]
    data = read(DEMO + '/markets', {'series_ticker': series, 'status': 'open', 'limit': PAGE_SIZE})[0]
    roster = sorted((m for m in data['markets'] if m.get('market_type') == 'binary'
                     and m.get('event_ticker', '').startswith(series + '-')),
                    key=lambda m: m['ticker'])
    if not roster:
        raise ValueError('no_demo_reference_roster')
    rotation = slot // len(SERIES)
    market = roster[(rotation // 2) % len(roster)]
    side = ('yes', 'no')[rotation % 2]
    mapping = dict(ticker=market['ticker'], side=side, event_ticker=market['event_ticker'],
                   exchange_index=market.get('exchange_index', 0),
                   kalshi_rules={key: market[key] for key in ('rules_primary', 'rules_secondary')})
    frame = read_quote(mapping, DEMO, read)
    basis = read_fee(frame, DEMO, read)
    result = checked_quote(frame, basis, ready=clock())
    result['roster_paged'] = bool(data.get('cursor'))
    result['roster_size'] = len(roster)
    result['frame'] = frame
    result['fee_basis'] = basis
    return result


def open_archive(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / 'quotes.sqlite3', isolation_level=None)
    db.execute('PRAGMA max_page_count=16384')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS controls(slot INTEGER PRIMARY KEY, detail TEXT);
      CREATE TABLE IF NOT EXISTS pairs(id TEXT PRIMARY KEY, detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS cursors(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS outcomes(ticker TEXT PRIMARY KEY, checked REAL NOT NULL,
                                         detail TEXT);
      CREATE TABLE IF NOT EXISTS frozen(id TEXT PRIMARY KEY, detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY, detail TEXT NOT NULL);
    ''')
    return db


def read_only(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)


def collect_pairs(root, db):
    copier = Path(root) / 'ranked-wallet-copy-v1'
    cursor = db.execute("SELECT value FROM cursors WHERE name='submit_quotes'").fetchone()
    with (closing(read_only(copier / 'journal.sqlite3')) as journal,
          closing(read_only(copier / 'signals.sqlite3')) as signals):
        if journal.execute('SELECT environment FROM journal_scope').fetchone() != ('demo',):
            raise ValueError('demo_journal_required')
        rows = journal.execute('SELECT rowid,client_id,detail FROM submission_quotes '
                               'WHERE rowid>? ORDER BY rowid LIMIT 40', cursor or (0,)).fetchall()
        for rowid, client, raw in rows:
            from .kalshi_wallet_copy_worker import signal_key
            frame = json.loads(raw)
            if not client.startswith('wallet-copy-') or frame['observed_at'] < FIRST:
                save_pair(db, rowid, client, None)
                continue
            key = signal_key(client)
            row = signals.execute('SELECT side,trade,state FROM signals WHERE id=?', (key,)).fetchone()
            if not row or row[2] in ('observed', 'submitting', 'submission_unresolved'):
                break
            if row[0] != 'BUY':
                save_pair(db, rowid, client, None)
                continue
            result = dict(state='missing_execution_evidence', baseline_state=row[2],
                          submitted_quote_at=frame.get('observed_at'))
            try:
                basis = frame['fee_basis']
                ready = datetime.fromisoformat(basis['metadata_fetched_at']).timestamp()
                wallet = checked_quote(frame, basis, ready=ready)
                trade = json.loads(row[1])
                wallet.update(decision_at=ready, first_seen_at=trade['observed_at'], wallet=trade['proxy_wallet'])
                if key != frame['wallet_signal_id'] or not trade['timestamp'] <= wallet['first_seen_at'] <= wallet['received_at']:
                    raise ValueError('wallet_clock_or_identity_mismatch')
                controls = [json.loads(r[0]) for r in db.execute(
                    'SELECT detail FROM controls WHERE slot BETWEEN ? AND ? AND detail IS NOT NULL',
                    (int((wallet['received_at'] - MAX_WINDOW) // 30), int(ready // 30)))]
                used = {r[0] for r in db.execute("SELECT json_extract(detail,'$.control.parent') FROM pairs "
                                                "WHERE json_type(detail,'$.control')='object'")}
                reference = matching_quote(wallet, controls, used)
                if reference is not None:
                    reference = {k: v for k, v in reference.items() if k not in ('frame', 'fee_basis')}
                result.update(wallet=wallet, control=reference,
                              state='matched' if reference else 'missing_contemporaneous_control')
            except (KeyError, TypeError, ValueError, ArithmeticError) as error:
                result['reason'] = type(error).__name__
            save_pair(db, rowid, client, result)


def save_pair(db, rowid, client, result):
    db.execute('BEGIN')
    try:
        if result is not None:
            db.execute('INSERT OR IGNORE INTO pairs VALUES(?,?)',
                       (client, json.dumps(result, sort_keys=True)))
            for role in ('wallet', 'control'):
                if result.get(role):
                    db.execute('INSERT OR IGNORE INTO outcomes VALUES(?,0,NULL)',
                               (result[role]['ticker'],))
        db.execute("INSERT INTO cursors VALUES('submit_quotes',?) ON CONFLICT(name) "
                   "DO UPDATE SET value=excluded.value", (rowid,))
        db.execute('COMMIT')
    except sqlite3.Error:
        if db.in_transaction:
            db.execute('ROLLBACK')
        raise


def observe_settlement(db, *, read, clock):
    now = clock()
    pending = db.execute("SELECT ticker FROM outcomes WHERE detail IS NULL AND checked<=? "
                         "ORDER BY checked,ticker LIMIT 1", (now - 300,)).fetchone()
    if not pending:
        return
    ticker, = pending
    db.execute('UPDATE outcomes SET checked=? WHERE ticker=?', (now, ticker))
    if not TICKER.fullmatch(ticker):
        raise ValueError('invalid_demo_settlement_ticker')
    payload, sent, received, _, _ = read(DEMO + '/markets/' + ticker)
    market = payload['market']
    if market['ticker'] != ticker or not now <= sent <= received <= clock():
        raise ValueError('invalid_demo_settlement_receipt')
    if market['status'] != 'finalized' or market.get('result') not in ('yes', 'no'):
        return
    evidence = dict(market=market, requested_at=sent, received_at=received)
    db.execute('UPDATE outcomes SET detail=? WHERE ticker=? AND detail IS NULL',
               (json.dumps(evidence, sort_keys=True), ticker))


def join_broker_settlements(root, db):
    """Use one independently recorded owned contract; never impute a fill or sale."""
    journal_path = Path(root) / 'ranked-wallet-copy-v1' / 'journal.sqlite3'
    with closing(read_only(journal_path)) as journal:
        if journal.execute('SELECT environment FROM journal_scope').fetchone() != ('demo',):
            raise ValueError('demo_journal_required')
        rows = db.execute("""SELECT pairs.id,pairs.detail,outcomes.detail FROM pairs
          JOIN outcomes ON outcomes.ticker=json_extract(pairs.detail,'$.wallet.ticker')
          WHERE outcomes.detail IS NOT NULL AND json_type(pairs.detail,'$.broker_join') IS NULL
          AND json_extract(pairs.detail,'$.baseline_state')='filled'""").fetchall()
        for ident, body, outcome in rows:
            pair = json.loads(body)
            wallet = pair['wallet']
            ticker = wallet['ticker']
            receipt = journal.execute('SELECT detail FROM settlements WHERE ticker=?', (ticker,)).fetchone()
            if not receipt:
                continue
            own = journal.execute(
                "SELECT id FROM intents WHERE filled>0 AND json_extract(payload,'$.ticker')=?",
                (ticker,)).fetchall()
            pair['broker_join'] = 'shared_or_closed_by_sell'
            if own == [(ident,)]:
                record = journal.execute('SELECT detail FROM broker_evidence WHERE client_id=?',
                                         (ident,)).fetchone()
                if not record:
                    continue
                result, fill = json.loads(receipt[0]), json.loads(record[0])
                side = wallet['side']
                settled = payout(wallet, json.loads(outcome))
                if (settled is None or result.get('event_ticker') != wallet['event']
                        or result.get('market_result') != json.loads(outcome)['market']['result']
                        or Decimal(result[side + '_count_fp']) != 1
                        or Decimal(result[('no' if side == 'yes' else 'yes') + '_count_fp']) != 0
                        or sum(Decimal(f['count_fp']) for f in fill['fills']) != 1):
                    pair['broker_join'] = 'settlement_identity_mismatch'
                else:
                    yes_gross = Decimal(fill['gross_dollars'])
                    gross = yes_gross if side == 'yes' else 1 - yes_gross
                    fees = Decimal(fill['fees_dollars']) + Decimal(result['fee_cost'])
                    pair['baseline_settlement_net_cents'] = str(settled - 100 * (gross + fees))
                    pair['broker_join'] = 'one_owned_contract_settled'
            db.execute('UPDATE pairs SET detail=? WHERE id=?', (json.dumps(pair, sort_keys=True), ident))


def first_matched_receipt(db):
    """Keep quote and finalized-market proof on the same first observed pair."""
    first = db.execute("SELECT id,detail FROM pairs WHERE json_extract(detail,'$.state')='matched' "
                       "ORDER BY rowid LIMIT 1").fetchone()
    if first is None:
        return None
    ident, detail = first
    pair = json.loads(detail)
    proof = {'id_sha256': hashlib.sha256(ident.encode()).hexdigest()}
    for side in ('wallet', 'control'):
        quote = pair[side]
        result = dict(ticker=quote['ticker'], quote_at=quote['received_at'],
                      quote_sha256=quote['frame_sha256'])
        row = db.execute('SELECT detail FROM outcomes WHERE ticker=?', (quote['ticker'],)).fetchone()
        if row and row[0]:
            raw, = row
            settlement = json.loads(raw)
            if payout(quote, settlement) is not None:
                result['settlement_at'] = settlement['received_at']
                result['settlement_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        proof[side] = result
    return proof


def run_tick(root, db, *, read=public_get, clock=time.time):
    now = clock()
    archive = Path(root) / DIRECTORY
    status = dict(schema='wallet_matched_demo_v1', at=now, environment='demo',
                  state='awaiting_start', decision='awaiting_eligible_training',
                  registration_sha256=REGISTRATION_SHA256)
    if now >= FIRST:
        state = 'collecting'
        stopped = db.execute("SELECT 1 FROM cursors WHERE name='capacity_stop'").fetchone()
        if (stopped or shutil.disk_usage(archive).free < FREE_FLOOR
                or (archive / 'quotes.sqlite3').stat().st_size >= MAX_BYTES - 1024 * 1024):
            db.execute("INSERT OR IGNORE INTO cursors VALUES('capacity_stop',?)", (int(now),))
            state = 'capacity_stop'
            status['decision'] = 'insufficient_evidence'
        else:
            slot = int(now // 30)
            if db.execute('INSERT OR IGNORE INTO controls VALUES(?,NULL)', (slot,)).rowcount:
                try:
                    quote = capture_control(slot, read=read, clock=clock)
                    db.execute('UPDATE controls SET detail=? WHERE slot=?',
                               (json.dumps(quote, sort_keys=True), slot))
                except Exception as error:
                    status['capture_error'] = type(error).__name__
            collect_pairs(root, db)
            try:
                observe_settlement(db, read=read, clock=clock)
                join_broker_settlements(root, db)
            except Exception as error:
                status['settlement_error'] = type(error).__name__
            status['qualification'] = assess(db, now=clock())
            if status['qualification']['frozen_groups']:
                status['decision'] = 'see_group_decisions'
        status['state'] = state
    status['control_quotes'] = db.execute('SELECT count(detail) FROM controls').fetchone()[0]
    status['pairs'] = dict(db.execute("SELECT json_extract(detail,'$.state'),count(*) "
                                      'FROM pairs GROUP BY 1'))
    status['settlements'] = db.execute('SELECT count(detail) FROM outcomes').fetchone()[0]
    status['pending_settlements'] = db.execute(
        'SELECT count(*) FROM outcomes WHERE detail IS NULL').fetchone()[0]
    status['final_decisions'] = db.execute('SELECT count(*) FROM decisions').fetchone()[0]
    first = first_matched_receipt(db)
    if first is not None:
        status['first_matched_receipt'] = first
    last_decision = db.execute('SELECT id,detail FROM decisions ORDER BY rowid DESC LIMIT 1').fetchone()
    if last_decision:
        status['latest_decision'] = dict(
            group_sha256=hashlib.sha256(last_decision[0].encode()).hexdigest(),
            **json.loads(last_decision[1]))
    status['at'] = clock()
    temp = archive / 'status.tmp'
    temp.write_text(json.dumps(status, sort_keys=True), encoding='utf-8')
    temp.replace(archive / 'status.json')
    return status


def run(root):
    registration = (Path(__file__).resolve().parents[1] / 'configs' /
                    'kalshi_wallet_price_discipline_v1_20261122' / 'registration.json')
    normalized = registration.read_bytes().replace(b'\r\n', b'\n')
    if hashlib.sha256(normalized).hexdigest() != REGISTRATION_SHA256:
        raise ValueError('wallet_matched_registration_changed')
    archive = Path(root) / DIRECTORY
    archive.mkdir(parents=True, exist_ok=True)
    from .kalshi_process_lock import acquire
    with acquire(archive / 'worker.lock'), closing(open_archive(archive)) as db:
        while True:
            start = time.monotonic()
            run_tick(root, db)
            time.sleep(max(5, 30 - (time.monotonic() - start)))
