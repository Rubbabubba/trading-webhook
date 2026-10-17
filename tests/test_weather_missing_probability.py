import copy
import json
from datetime import datetime,timezone
import pytest
from opportunity_lab.kalshi_weather_ensemble import score_resolution
from opportunity_lab.kalshi_external_sleeves_worker import open_db,resolve_one_weather,_weather_summary

def row():
    return dict(expiration_time='2026-10-08T14:00:00Z',event_structure='bucket_ladder',fee_and_model_stress=.03,contracts=[dict(ticker='WX',bucket=[80,90],consensus_probability=.8,normalized_market_probability=None,market_mid_probability=0)],signal=None)

@pytest.mark.parametrize('value',[None,float('nan'),float('inf'),-0.1,1.1,'invalid'])
def test_missing_or_invalid_frozen_probability_is_not_fabricated(value):
    r=row();r['contracts'][0]['normalized_market_probability']=value
    with pytest.raises(ValueError,match='frozen_probability'):
        score_resolution(r,None,contract_results={'WX':'yes'})

def test_finalized_missing_probability_is_durable_unscored_and_original_preserved(tmp_path):
    db=open_db(tmp_path/'research.sqlite3');r=row();original=json.dumps(r)
    db.execute('INSERT INTO weather_observations VALUES(?,?,?,?,?,NULL)',('a','a',1,'2026-09-30T00:00:00Z',original))
    class Client:
        calls=0
        def get(self,ticker):
            self.calls+=1
            return {'market':{'ticker':ticker,'status':'finalized','result':'yes','expiration_value':''}},0,0
    client=Client();now=datetime(2026,10,17,tzinfo=timezone.utc)
    result=resolve_one_weather(db,client,now)
    assert result['scored']==0 and result['unscored']==1 and result['probability_exclusions']==1
    detail,resolution=db.execute('SELECT detail,resolution FROM weather_observations').fetchone()
    assert detail==original
    out=json.loads(resolution)
    assert out['unscored_reason']=='missing_frozen_probability'
    assert 'brier_delta_vs_market' not in out and 'cost_stressed_net_dollars' not in out
    assert _weather_summary(db)['complete_observations']==0
    assert resolve_one_weather(db,client,now)['rows_scanned']==0 and client.calls==1
    db.close()

def test_zero_is_a_valid_probability_not_missing():
    r=row();r['contracts'][0]['normalized_market_probability']=0
    assert score_resolution(r,None,contract_results={'WX':'yes'})['market_brier']==1
