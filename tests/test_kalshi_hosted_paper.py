import json,time
from datetime import datetime,timezone
import pytest
from opportunity_lab import kalshi_hosted_paper as host
from opportunity_lab.kalshi_process_lock import acquire


def test_lock_blocks_duplicate_and_releases(tmp_path):
    lock=acquire(tmp_path/'worker.lock')
    try:
        with pytest.raises(RuntimeError):acquire(tmp_path/'worker.lock')
    finally:lock.close()
    acquire(tmp_path/'worker.lock').close()


def test_self_contained_boot_and_persistent_restart(tmp_path,monkeypatch):
    stop=datetime.fromtimestamp(time.time()+86400,timezone.utc).isoformat()
    output=tmp_path/'portfolio';source=tmp_path/'discovery/markets.json'
    market=dict(ticker='T',event_ticker='E',market_type='binary',status='active',rules_primary='rule',close_time=stop)
    series=dict(ticker='S',category='Economics',fee_type='quadratic',fee_multiplier=1)
    class Discovery:
        def __init__(self,path,stop):self.path=path
        def update(self):
            source.parent.mkdir(exist_ok=True)
            source.write_text(json.dumps({'T':dict(market=market,series=series)}))
    class Books:
        def __init__(self,db):pass
        def get(self,kind,ticker,book=False):
            data=dict(market=market) if kind=='markets' else dict(series=series)
            if book:data=dict(orderbook_fp=dict(yes_dollars=[['.39','10']],no_dollars=[['.6','10']]))
            return data,time.time(),str(time.time_ns())
    config=dict(execution_enabled=False,slots=4,total_cash_cents=50000,source_sha256={},output=str(output),
                universe_source=str(source),stop_at=stop,engine=dict(strategy_id='host',take_profit_cents=5,
                stop_loss_cents=10,max_hold_seconds=900,latency_seconds=2,slippage_cents=1))
    path=tmp_path/'config.json';path.write_text(json.dumps(config))
    monkeypatch.setattr(host,'Discovery',Discovery);monkeypatch.setattr(host,'PublicBooks',Books)
    host.run(path,cycles=1);host.run(path,cycles=1)
    status=json.loads((output/'status.json').read_text())
    assert status['cash_cents']==50000 and status['cohort_size']==1 and not status['errors']
