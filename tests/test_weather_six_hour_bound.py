from datetime import date,datetime,timezone
import pytest
from research.weather_bound_20261016.evaluate import report,climate_window,interval,projection,select_day,numeric_final,source_agency,station_rules

UTC=timezone.utc
ST=dict(station='KNYC',iem='NYC',standard_utc_offset_hours=-5,
        rules_keywords=['central park','national weather service'])


def metar(raw='KNYC 011751Z AUTO 20005KT 10SM CLR 32/20 A3000 RMK AO2 10322',valid='2026-07-01 17:51'):
    return dict(station='NYC',valid=valid,metar=raw)


def market(**kw):
    return dict(ticker='T',event_ticker='E',market_type='binary',yes_sub_title='86° to 87°',
        strike_type='between',floor_strike=86,cap_strike=87,
        rules_primary='National Weather Service Central Park daily high',
        open_time='2026-06-30T00:00:00Z',close_time='2026-07-02T05:00:00Z',
        status='finalized',result='no',settlement_value_dollars='0.0000',expiration_value='90')|kw


def test_six_hour_group_whole_F_not_current_temperature():
    r=report(metar(),ST)
    assert r['maximum_f']==90 and r['lower_bound_f']==88
    assert datetime.fromtimestamp(r['available_at'],UTC).hour==19
    assert r['publication_verified'] is False


@pytest.mark.parametrize('raw',[metar()['metar']+' COR',metar()['metar']+' $',
    metar()['metar']+' 10300',metar()['metar'].replace('10322','10252'),
    metar()['metar'].replace('KNYC','KLAX'),metar()['metar'].replace('011751','011750')])
def test_bad_or_ambiguous_measurement_is_not_silently_rounded(raw):
    with pytest.raises((ValueError,ArithmeticError)):report(metar(raw),ST)


def test_climate_day_is_fixed_standard_time_during_daylight_saving():
    begin,end=climate_window(date(2026,7,1),ST)
    assert datetime.fromtimestamp(begin,UTC).hour==5 and end-begin==86400
    r=report(metar('KNYC 010551Z AUTO 20005KT 10SM CLR 32/20 A3000 RMK AO2 10322','2026-07-01 05:51'),ST)
    assert r['interval_start']<begin  # preceding local climate day leaks into6hmax
    candidate,_,_=select_day(date(2026,7,1),ST,[r],[projection(market())])
    assert candidate is None


def test_machine_bounds_and_tail_off_by_one_require_exact_text_agreement():
    assert interval(market())==(86,87)
    assert interval(market(strike_type='greater',floor_strike=87,cap_strike=None,yes_sub_title='88° or above'))==(88,None)
    assert interval(market(strike_type='less',cap_strike=86,floor_strike=None,yes_sub_title='85° or below'))==(None,85)
    with pytest.raises(ValueError):interval(market(yes_sub_title='85° to 86°'))


def test_selection_uses_available_bound_and_has_no_outcome_or_price_interface():
    r=report(metar(),ST);m=market()
    candidate,_,_=select_day(date(2026,7,1),ST,[r],[projection(m)])
    assert candidate['side']=='no' and candidate['lower_bound_f']==88
    assert datetime.fromtimestamp(candidate['decision_at'],UTC).hour==19
    assert projection(m)==projection(market(result='yes',expiration_value='40',yes_bid_dollars='.99'))
    with pytest.raises(ValueError,match='unapproved'):
        select_day(date(2026,7,1),ST,[r],[m])


def test_first_decision_no_future_report_and_nearest_crossed_boundary():
    r=report(metar(),ST)
    future={**r,'available_at':r['available_at']+86400,'lower_bound_f':120}
    m1=projection(market(ticker='FAR',floor_strike=80,cap_strike=81,yes_sub_title='80° to 81°'))
    m2=projection(market(ticker='NEAR'))
    candidate,_,_=select_day(date(2026,7,1),ST,[r,future],[m1,m2])
    assert candidate['ticker']=='NEAR' and candidate['lower_bound_f']==88


def test_numeric_final_does_not_substitute_label_for_measurement():
    high,n,faults=numeric_final([market()]);assert (high,n,faults)==(90,1,[])
    high,n,faults=numeric_final([market(result='yes',settlement_value_dollars='1')])
    assert high is None and n==0 and faults
    high,n,faults=numeric_final([market(expiration_value='')])
    assert high is None and n==0


def test_current_weather_company_rules_cannot_be_treated_as_legacy_nws():
    m=market(rules_primary='The maximum temperature at New York City according to The Weather Company')
    assert source_agency(projection(m))=='TWC'
    assert not station_rules(projection(m),ST)


def test_lax_alias_requires_exact_historical_series_and_airport_wording():
    station=dict(station='KLAX',rules_keywords=['los angeles','national weather service'])
    m=projection(market(event_ticker='KXHIGHLAX-26JUL01',rules_primary='Los Angeles Airport, CA National Weather Service Climatological Report Daily'))
    assert station_rules(m,station)
    assert not station_rules({**m,'event_ticker':'OTHER'},station)
    assert not station_rules({**m,'rules_primary':'Los Angeles Downtown CA National Weather Service'},station)
