"""Pure Demo source decoding. No network, broker, signal or order interface."""
from decimal import Decimal
import math,uuid

MAX_LEVELS=2048
class SourceFault(ValueError):pass

def integer(x):return type(x) is int and x>=1

def number(x):
    if not isinstance(x,str):raise SourceFault("non_text_decimal")
    try:v=Decimal(x)
    except ArithmeticError:raise SourceFault("invalid_decimal") from None
    if not v.is_finite():raise SourceFault("nonfinite_decimal")
    return v

def levels(rows):
    if not isinstance(rows,list) or len(rows)>MAX_LEVELS:raise SourceFault("book_level_capacity")
    result={}
    for row in rows:
        if not isinstance(row,list) or len(row)!=2:raise SourceFault("invalid_level")
        price,size=map(number,row)
        if not 0<price<1 or not 0<=size<=1000000000 or price in result:raise SourceFault("invalid_or_duplicate_level")
        if size:result[price]=size
    return result

class Books:
    def __init__(self,tickers):
        if not 1<=len(tickers)<=8 or len(set(tickers))!=len(tickers):raise SourceFault("target_capacity")
        self.targets=set(tickers);self.channels={};self.sequence={};self.books={};self.last_received=None;self.poisoned=False
    def apply(self,payload,received_at):
        if self.poisoned:raise SourceFault("connection_invalid_after_fault")
        try:return self._apply(payload,received_at)
        except (SourceFault,KeyError,TypeError,ValueError,ArithmeticError) as exc:
            self.poisoned=True;self.books.clear()
            raise SourceFault(str(exc) if isinstance(exc,SourceFault) else "malformed_source_message") from None
    def _apply(self,p,at):
        if type(at) not in (int,float) or not math.isfinite(at) or self.last_received is not None and at<self.last_received:raise SourceFault("receipt_clock_regression")
        self.last_received=at
        if not isinstance(p,dict) or not isinstance(p.get("msg"),dict):raise SourceFault("invalid_envelope")
        sent=p.get("sending_ts_ms")
        if not integer(sent) or sent/1000>at+2:raise SourceFault("invalid_server_clock")
        kind=p.get("type");msg=p["msg"]
        if kind=="subscribed":
            sid=msg.get("sid");channel=msg.get("channel")
            if not integer(sid) or channel not in {"orderbook_delta","trade"} or sid in self.channels:raise SourceFault("invalid_subscription")
            self.channels[sid]=channel;return dict(kind=kind,channel=channel)
        if kind not in {"orderbook_snapshot","orderbook_delta","trade"}:raise SourceFault("unexpected_market_message")
        sid,seq=p.get("sid"),p.get("seq")
        channel="trade" if kind=="trade" else "orderbook_delta"
        if self.channels.get(sid)!=channel or not integer(seq):raise SourceFault("unacknowledged_or_invalid_sequence")
        prior=self.sequence.get(sid)
        if prior is not None and seq!=prior+1:raise SourceFault("subscription_sequence_gap")
        self.sequence[sid]=seq
        ticker=msg.get("market_ticker")
        if ticker not in self.targets:raise SourceFault("outside_targets")
        if kind=="trade":
            yes,no,size=[number(msg[k]) for k in ("yes_price_dollars","no_price_dollars","count_fp")]
            if not 0<=yes<=1 or yes+no!=1 or not 0<size<=1000000000 or msg.get("taker_side") not in {"yes","no"}:raise SourceFault("invalid_trade_economics")
            if msg.get("taker_outcome_side",msg["taker_side"])!=msg["taker_side"]:raise SourceFault("conflicting_trade_side")
            execution=msg.get("ts_ms");precision="milliseconds"
            if execution is None:
                execution=msg.get("ts");precision="seconds"
                if not integer(execution):raise SourceFault("missing_trade_clock")
                execution*=1000
            if not integer(execution) or execution>sent+2000:raise SourceFault("invalid_trade_clock")
            book=self.books.get(ticker)
            return dict(kind=kind,ticker=ticker,clock_precision=precision,book_available=book is not None,
                        prior_book_receipt_at=book["received_at"] if book else None,
                        prior_book_sending_ts_ms=book["sending_ts_ms"] if book else None,
                        trade_execution_ts_ms=execution,actual_fill_proven=False)
        market_id=msg.get("market_id")
        try:uuid.UUID(market_id)
        except (ValueError,TypeError,AttributeError):raise SourceFault("invalid_market_identity") from None
        old=self.books.get(ticker)
        if old is not None and old["market_id"]!=market_id:raise SourceFault("market_identity_changed")
        if kind=="orderbook_snapshot":
            book=dict(market_id=market_id,yes=levels(msg.get("yes_dollars_fp")),no=levels(msg.get("no_dollars_fp")))
        else:
            if old is None:raise SourceFault("delta_without_snapshot")
            side=msg.get("side")
            if side not in {"yes","no"}:raise SourceFault("invalid_delta_side")
            price,delta=number(msg.get("price_dollars")),number(msg.get("delta_fp"))
            if not 0<price<1 or abs(delta)>1000000000:raise SourceFault("invalid_delta")
            book={**old,side:dict(old[side])};size=book[side].get(price,Decimal(0))+delta
            if not 0<=size<=1000000000:raise SourceFault("negative_or_excessive_depth")
            if size:book[side][price]=size
            else:book[side].pop(price,None)
            if len(book[side])>MAX_LEVELS:raise SourceFault("book_level_capacity")
        book.update(received_at=at,sending_ts_ms=sent,sid=sid,seq=seq);self.books[ticker]=book
        yes,no=[max(book[s],default=None) for s in ("yes","no")]
        usable=yes is not None and no is not None and yes+no<1 and all(any(n>=1 for n in book[s].values()) for s in ("yes","no"))
        return dict(kind=kind,ticker=ticker,usable=usable,levels=sum(len(book[s]) for s in ("yes","no")),sending_lag_ms=at*1000-sent)
