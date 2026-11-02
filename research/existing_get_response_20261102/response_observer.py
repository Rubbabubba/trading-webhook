"""Offline response admission component. Never calls a transport or broker.

Consumes bytes already returned by an existing GET. requests.content is the
decoded HTTP entity, not proof of original wire compression or TLS identity.
No headers, credentials, request bodies, or exception text are persisted.
"""
from decimal import Decimal, InvalidOperation
from receipt import BASE, build

SCHEMA = 'existing_get_response_admission_component_v1_20261102'


def observe_existing(response, archive, *, ticker, requested_at, received_at):
    """Admit only exact unredirected Demo metadata responses; no retry/fetch.

    The caller must obtain real clocks around its existing request. A fake
    Response can pass these structural checks; venue authenticity stays false.
    Missing HTTP Age is unknown, never silently converted to zero.
    """
    expected = BASE + ticker
    if response.request.method != 'GET':
        raise ValueError('existing_get_required')
    if response.url != expected or response.request.url != expected:
        raise ValueError('exact_demo_endpoint_required')
    if response.history:
        raise ValueError('redirected_response_forbidden')
    raw_age = response.headers.get('Age')
    if not isinstance(raw_age, str) or len(raw_age) > 32:
        raise ValueError('http_age_unknown')
    try:
        age = Decimal(raw_age)
        if not age.is_finite() or not 0 <= age <= 2:
            raise ValueError('http_age_out_of_range')
    except InvalidOperation:
        raise ValueError('invalid_http_age') from None
    # No reserialization: retain the exact entity bytes supplied by Response.
    receipt = build(response.content, ticker=ticker, url=expected,
                    requested_at=requested_at, received_at=received_at,
                    http_status=response.status_code,
                    http_age_seconds=float(age))
    return archive.observe(receipt, at=received_at)
