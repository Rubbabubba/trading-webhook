"""Read-only finite-prefix archive audit; no network, orders or DB writes."""
import gzip
import hashlib
import json
from fractions import Fraction
from pathlib import Path
import sqlite3

from opportunity_lab.kalshi_demo_stream_source import Books


def sha(value):return hashlib.sha256(value).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def audit(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
    db.execute('BEGIN')
    try:
        protocol=db.execute('select name from protocol where id=1').fetchone()[0]
        assert protocol in {'demo_stream_archive_v1_20261016','demo_stream_archive_availability_v2_20261018','demo_stream_archive_availability_v3_20261018'}
        availability=protocol!='demo_stream_archive_v1_20261016'
        missing_book=protocol=='demo_stream_archive_availability_v3_20261018'
        epochs={identity:(json.loads(targets),started,reason) for identity,targets,started,reason in db.execute('select id,targets,started,reason from connections')}
        previous='0'*64;decoders={};reference={};sequences={};last_receipt={}
        records=states=trades=overlap=unavailable=0;tickers=set();trade_ids={};missing_ids=0
        for identity,at,raw_sha,compressed_sha,prev,chain,data in db.execute('select connection,received,raw_sha,compressed_sha,previous_sha,chain_sha,data from messages order by id'):
            assert identity in epochs and at>=epochs[identity][1]
            assert len(data)<=262144 and sha(data)==compressed_sha
            raw=gzip.decompress(data)
            assert len(raw)<=262144 and sha(raw)==raw_sha and prev==previous
            assert chain==sha(encoded([prev,identity,at,raw_sha]));previous=chain
            payload=json.loads(raw);msg=payload['msg'];kind=payload['type']
            assert identity not in last_receipt or at>=last_receipt[identity]
            last_receipt[identity]=at
            if identity not in decoders:
                decoders[identity]=Books(epochs[identity][0],allow_unavailable=availability,allow_missing_book=missing_book);reference[identity]={};sequences[identity]={}
            decoder=decoders[identity];decoder.apply(payload,at);records+=1
            if kind=='subscribed':continue
            sid,seq=payload['sid'],payload['seq']
            assert sid not in sequences[identity] or seq==sequences[identity][sid]+1
            sequences[identity][sid]=seq;ticker=msg['market_ticker'];tickers.add(ticker)
            books=reference[identity]
            if availability and kind=='orderbook_snapshot' and set(msg)=={'market_ticker','market_id'} and (missing_book or msg['market_id'] in (None,'')):
                books.pop(ticker,None);unavailable+=1;continue
            if kind=='trade':
                yes,no,size=map(Fraction,[msg['yes_price_dollars'],msg['no_price_dollars'],msg['count_fp']])
                assert yes+no==1 and size>0
                trades+=1;overlap+=ticker in books
                trade_id=msg.get('trade_id')
                if trade_id:
                    fingerprint=sha(encoded(msg))
                    assert trade_id not in trade_ids or trade_ids[trade_id]==fingerprint
                    trade_ids[trade_id]=fingerprint
                else:missing_ids+=1
                continue
            if kind=='orderbook_snapshot':
                books[ticker]={side:{Fraction(p):Fraction(n) for p,n in msg[side+'_dollars_fp'] if Fraction(n)>0} for side in ('yes','no')}
            else:
                assert kind=='orderbook_delta' and ticker in books
                side=msg['side'];price=Fraction(msg['price_dollars'])
                size=books[ticker][side].get(price,Fraction(0))+Fraction(msg['delta_fp'])
                assert size>=0
                if size:books[ticker][side][price]=size
                else:books[ticker][side].pop(price,None)
            for side in ('yes','no'):
                assert books[ticker][side]=={Fraction(p):Fraction(n) for p,n in decoder.books[ticker][side].items()}
            states+=1
        return dict(schema='demo_stream_independent_archive_audit_v1',records=records,
            reconstructed_states=states,epochs=len(epochs),markets=len(tickers),unavailable_source_records=unavailable,
            public_trade_records=trades,unique_trade_ids=len(trade_ids),trade_records_missing_id=missing_ids,
            trade_records_with_prior_book_in_same_epoch=overlap,
            last_chain_sha256=previous,sequence_and_fraction_reconstruction_consistent=True,
            synchronized_execution_clock_verified=False,owned_fills=0,profitability_proven=False,
            immutable_prefix_only=True,read_only=True)
    finally:db.close()


if __name__=='__main__':
    import sys
    print(json.dumps(audit(sys.argv[1]),sort_keys=True))
