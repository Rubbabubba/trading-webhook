"""Explicit economic intent to Kalshi V2 YES-price book terms. No transport."""


def outcome_for_book(book_side):
    if book_side not in ('bid', 'ask'):
        raise ValueError('invalid_book_side')
    return 'yes' if book_side == 'bid' else 'no'


def terms(outcome, action, price_cents):
    if outcome not in ('yes', 'no') or action not in ('buy', 'sell'):
        raise ValueError('invalid_economic_intent')
    if type(price_cents) is not int or not 1 <= price_cents <= 99:
        raise ValueError('invalid_price')
    book_side = 'bid' if (outcome == 'yes') == (action == 'buy') else 'ask'
    yes_price = price_cents if outcome == 'yes' else 100-price_cents
    return dict(side=book_side, price=f'{yes_price / 100:.4f}',
                reduce_only=action == 'sell', outcome_side=outcome_for_book(book_side))
