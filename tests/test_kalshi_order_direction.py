import pytest
from opportunity_lab.kalshi_order_direction import terms, outcome_for_book


@pytest.mark.parametrize('outcome,action,side,price,reducing,direction',[
    ('yes','buy','bid','0.3000',False,'yes'),
    ('yes','sell','ask','0.3000',True,'no'),
    ('no','buy','ask','0.7000',False,'no'),
    ('no','sell','bid','0.7000',True,'yes')])
def test_all_four_intents(outcome,action,side,price,reducing,direction):
    assert terms(outcome,action,30)==dict(side=side,price=price,reduce_only=reducing,outcome_side=direction)


@pytest.mark.parametrize('value',[True,0,100,30.5,'30'])
def test_invalid_price(value):
    with pytest.raises(ValueError):terms('no','buy',value)


def test_invalid_direction():
    with pytest.raises(ValueError):outcome_for_book('sell')
    with pytest.raises(ValueError):terms('maybe','buy',30)
