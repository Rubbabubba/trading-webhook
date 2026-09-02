"""Small, testable authentication helpers for operator-only HTTP surfaces."""

from __future__ import annotations

import base64
import secrets
from collections.abc import Mapping


def supplied_operator_secret(headers: Mapping[str, str]) -> str:
    supplied = str(headers.get("x-admin-secret", "") or "").strip()
    auth = str(headers.get("authorization", "") or "")
    if supplied or not auth.lower().startswith("basic "):
        return supplied
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1], validate=True).decode("utf-8")
        return decoded.split(":", 1)[1] if ":" in decoded else ""
    except Exception:
        return ""


def operator_authorized(headers: Mapping[str, str], configured_secret: str) -> bool:
    expected = str(configured_secret or "").strip()
    supplied = supplied_operator_secret(headers)
    return bool(expected and secrets.compare_digest(supplied, expected))
