"""Prospective timing observations. Missing future quotes remain missing."""
from collections import deque
import math
import json
from .sports_research_execution import cost


class Latency:
    def __init__(self):
        self.samples = deque(maxlen=100)

    def add(self, seconds):
        if math.isfinite(seconds) and seconds > 0:
            self.samples.append(seconds)

    def estimate(self, processing=0):
        if len(self.samples) < 5:
            return None
        ordered = sorted(self.samples)
        # Conservative full measured GET RTT, not an assertion of order-routing latency.
        return max(.25, ordered[min(len(ordered)-1, math.ceil(.95*len(ordered))-1)] + processing)


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, game TEXT, ticker TEXT,
        signature TEXT, at REAL, detail TEXT, UNIQUE(game,ticker,signature));
      CREATE TABLE IF NOT EXISTS markouts(signal_id INTEGER, seconds INTEGER, observed_at REAL,
        detail TEXT, PRIMARY KEY(signal_id,seconds));
      CREATE INDEX IF NOT EXISTS signal_ticker_time ON signals(ticker,at);
      CREATE TABLE IF NOT EXISTS coverage(at REAL, game TEXT, detail TEXT);
      CREATE INDEX IF NOT EXISTS coverage_game_time ON coverage(game,at);
    ''')


def signal(db, game, ticker, signature, at, market, probability, accepted):
    if not market.get('valid') or not market.get('quote'):
        return
    q = market['quote']; coefficient = market['fee_coefficient']
    detail = {'ask':q['ask'], 'bid':q['bid'], 'probability':probability,
              'initial_book_id':market['book_id'], 'coefficient':coefficient,
              'one_contract_entry_cost':cost(q['ask'],1,coefficient,True)['total'],
              'stage_eligible':accepted}
    db.execute('INSERT OR IGNORE INTO signals VALUES(NULL,?,?,?,?,?)',
               (game,ticker,signature,at,json.dumps(detail)))


def update_markouts(db, ticker, at, market):
    # Bound work to unresolved horizons within the last two minutes.
    for sid, start, raw in db.execute('SELECT id,at,detail FROM signals WHERE ticker=? AND at>=?',
                                      (ticker,at-120)).fetchall():
        detail = json.loads(raw)
        for seconds in (5,15,30,60):
            if at < start+seconds:
                continue
            if db.execute('SELECT 1 FROM markouts WHERE signal_id=? AND seconds=?',(sid,seconds)).fetchone():
                continue
            lag = at-start-seconds
            valid = (lag <= 5 and market.get('valid') and market.get('quote')
                     and market['book_id'] != detail['initial_book_id'])
            result = {'status':'observed' if valid else 'missing', 'lag_seconds':lag}
            if valid:
                q=market['quote']
                result.update(bid=q['bid'], ask=q['ask'], book_id=market['book_id'],
                    gross_change_cents=100*(q['bid']-detail['ask']),
                    modeled_net_cents=cost(q['bid'],1,detail['coefficient'],False)['total']-detail['one_contract_entry_cost'])
            db.execute('INSERT INTO markouts VALUES(?,?,?,?)',(sid,seconds,at,json.dumps(result)))


def expire_markouts(db, now):
    # Close every missing horizon even when a market disappears entirely.
    db.execute('''INSERT OR IGNORE INTO markouts
        SELECT s.id,h.seconds,?,? FROM signals s
        CROSS JOIN (SELECT 5 seconds UNION ALL SELECT 15 UNION ALL SELECT 30 UNION ALL SELECT 60) h
        WHERE s.at+h.seconds+5 < ?''',
        (now,json.dumps({'status':'missing','reason':'no_fresh_post_horizon_quote'}),now))
