"""Independent one-contract YES/NO shadow ledger; consumes books, never orders.

Signals are supplied by a separately frozen strategy. This engine does not invent
an entry edge. One position or pending entry per engine prevents capital reuse.
"""
import json
import sqlite3
from decimal import Decimal, ROUND_CEILING, InvalidOperation
from .kalshi_risk import amount


def price_book(frame, side):
    sides=[]
    for name in ('yes_dollars','no_dollars'):
        levels=frame['orderbook_fp'][name]
        if not isinstance(levels,list) or not levels:raise ValueError('missing_book')
        parsed=[(amount(p),amount(q)) for p,q in levels]
        if any(not 0<p<1 or q<=0 for p,q in parsed) or len({p for p,q in parsed})!=len(parsed):
            raise ValueError('invalid_book')
        sides.append(max(parsed))
    own,other=sides if side=='yes' else sides[::-1]
    bid,ask=own[0],1-other[0]
    if bid>=ask:raise ValueError('crossed_book')
    return bid,ask,own[1],other[1]


def cost(price, coefficient, slippage, buy):
    # One-contract, whole-cent shadow cash. Fee rounding applies per transaction.
    p=Decimal(price.numerator)/Decimal(price.denominator)
    fee=int((coefficient*p*(1-p)*100).to_integral_value(rounding=ROUND_CEILING))
    gross=p*100
    rounding=ROUND_CEILING if buy else 'ROUND_FLOOR'
    cents=int(gross.to_integral_value(rounding=rounding))
    return cents+fee+slippage if buy else max(0,cents-fee-slippage)


class Shadow:
    def __init__(self,path,config):
        required={'strategy_id','initial_cash_cents','take_profit_cents','stop_loss_cents',
                  'max_hold_seconds','latency_seconds','slippage_cents'}
        if set(config)!=required or not isinstance(config['strategy_id'],str) or not config['strategy_id']:
            raise ValueError('invalid_config')
        if any(type(config[k]) is not int or config[k]<1 for k in required-{'strategy_id','slippage_cents'}):
            raise ValueError('invalid_config')
        if type(config['slippage_cents']) is not int or config['slippage_cents']<0:raise ValueError('invalid_config')
        self.config=config.copy();self.db=sqlite3.connect(path,isolation_level=None)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS settings(id INTEGER PRIMARY KEY,detail TEXT);'
            'CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY,detail TEXT);'
            'CREATE TABLE IF NOT EXISTS actions(id INTEGER PRIMARY KEY,detail TEXT);')
        encoded=json.dumps(config,sort_keys=True)
        self.db.execute('INSERT OR IGNORE INTO settings VALUES(1,?)',(encoded,))
        if self.db.execute('SELECT detail FROM settings WHERE id=1').fetchone()[0]!=encoded:
            self.db.close();raise ValueError('frozen_config_changed')
        self.db.execute('INSERT OR IGNORE INTO state VALUES(1,?)',(json.dumps(dict(
            cash_cents=config['initial_cash_cents'],realized_cents=0,position=None,pending=None,last_at=None)),))

    def close(self):self.db.close()

    def state(self):return json.loads(self.db.execute('SELECT detail FROM state WHERE id=1').fetchone()[0])

    def step(self,frame,*,now,signal=None):
        """Frame: ticker, book_id, received_at (epoch), fee_coefficient, orderbook_fp.

        Optional signal: side yes/no and strategy_id. Optional signal_valid=false
        invalidates a held position. A fill always requires a later distinct book.
        """
        import math
        at=frame.get('received_at')
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (now,at)) or not 0<=now-at<=5:
            raise ValueError('stale_frame')
        ticker,bid=frame.get('ticker'),frame.get('book_id')
        if not isinstance(ticker,str) or not ticker or not isinstance(bid,str) or not bid:raise ValueError('invalid_identity')
        try:coefficient=Decimal(str(frame['fee_coefficient']))
        except InvalidOperation:raise ValueError('unsupported_fee') from None
        if not coefficient.is_finite() or not 0<=coefficient<=Decimal('.14'):raise ValueError('unsupported_fee')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            s=self.state();p=s['position'];pending=s['pending'];action={'action':'wait'}
            if s['last_at'] is not None and at<=s['last_at']:raise ValueError('non_increasing_frame')
            s['last_at']=at
            if (p or pending) and ticker!=(p or pending)['ticker']:
                raise ValueError('position_market_mismatch')
            if p and str(coefficient)!=p['coefficient']:raise ValueError('fee_changed')
            side=(p or pending or signal or {}).get('side')
            if side not in ('yes','no'):
                if signal is not None:raise ValueError('invalid_side')
            else:
                best_bid,ask,bid_size,ask_size=price_book(frame,side)
                slip=self.config['slippage_cents']
                if pending:
                    if str(coefficient)!=pending['coefficient']:raise ValueError('fee_changed')
                    if at-pending['at']>30:
                        s['pending']=None;action={'action':'expired'}
                    elif bid!=pending['book_id'] and at-pending['at']>=self.config['latency_seconds']:
                        if pending['kind']=='entry':
                            debit=cost(ask,coefficient,slip,True)
                            if frame.get('signal_valid') is False or ask_size<1 or debit>pending['max_cost'] or debit>s['cash_cents']:
                                action={'action':'entry_rejected'};s['pending']=None
                            else:
                                s['cash_cents']-=debit;s['pending']=None
                                s['position']=dict(ticker=ticker,side=side,basis=debit,opened_at=at,coefficient=str(coefficient))
                                action={'action':'buy','side':side,'cost_cents':debit}
                        elif bid_size>=1:
                            proceeds=min(pending['proceeds'],cost(best_bid,coefficient,slip,False))
                            profit=proceeds-p['basis'];s['cash_cents']+=proceeds;s['realized_cents']+=profit
                            s['position']=s['pending']=None
                            action={'action':'sell','side':side,'proceeds_cents':proceeds,'net_cents':profit,'reason':pending['reason']}
                elif p:
                    net=cost(best_bid,coefficient,slip,False)-p['basis']
                    reason=('signal_invalidated' if frame.get('signal_valid') is False else
                            'stop_loss' if net<=-self.config['stop_loss_cents'] else
                            'take_profit' if net>=self.config['take_profit_cents'] else
                            'time_limit' if at-p['opened_at']>=self.config['max_hold_seconds'] else None)
                    if reason and bid_size>=1:
                        s['pending']=dict(kind='exit',ticker=ticker,side=side,at=at,book_id=bid,
                            proceeds=cost(best_bid,coefficient,slip,False),reason=reason,coefficient=str(coefficient))
                        action={'action':'stage_exit','reason':reason}
                elif signal:
                    if signal.get('strategy_id')!=self.config['strategy_id']:raise ValueError('strategy_mismatch')
                    debit=cost(ask,coefficient,slip,True)
                    if frame.get('signal_valid') is not False and ask_size>=1 and debit<=s['cash_cents']:
                        s['pending']=dict(kind='entry',ticker=ticker,side=side,at=at,book_id=bid,max_cost=debit,coefficient=str(coefficient))
                        action={'action':'stage_entry','side':side}
            action.update(at=at,ticker=ticker,execution_enabled=False)
            self.db.execute('UPDATE state SET detail=? WHERE id=1',(json.dumps(s),))
            self.db.execute('INSERT INTO actions(detail) VALUES(?)',(json.dumps(action),))
            self.db.execute('COMMIT');return action
        except Exception:
            self.db.execute('ROLLBACK');raise


def main():
    """Consume newline-delimited live observations on stdin; no network client."""
    import argparse
    import sys
    import time
    from pathlib import Path
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--ledger',required=True)
    args=parser.parse_args()
    engine=Shadow(args.ledger,json.loads(Path(args.config).read_text()))
    try:
        for line in sys.stdin:
            try:
                frame=json.loads(line)
                result=engine.step(frame,now=time.time(),signal=frame.get('signal'))
            except (ValueError,KeyError,TypeError):
                result={'action':'rejected_observation','execution_enabled':False}
            print(json.dumps(result),flush=True)
    finally:engine.close()


if __name__=='__main__':main()
