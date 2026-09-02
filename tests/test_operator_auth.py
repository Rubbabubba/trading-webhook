import base64

from operator_auth import operator_authorized, supplied_operator_secret


def test_operator_secret_header_authentication():
    assert operator_authorized({"x-admin-secret": "correct"}, "correct")
    assert not operator_authorized({"x-admin-secret": "wrong"}, "correct")


def test_browser_basic_auth_uses_password_only():
    token = base64.b64encode(b"operator:correct").decode("ascii")
    assert supplied_operator_secret({"authorization": f"Basic {token}"}) == "correct"
    assert operator_authorized({"authorization": f"Basic {token}"}, "correct")


def test_missing_or_malformed_auth_fails_closed():
    assert not operator_authorized({}, "correct")
    assert not operator_authorized({"authorization": "Basic !!!"}, "correct")
    assert not operator_authorized({"x-admin-secret": "anything"}, "")
