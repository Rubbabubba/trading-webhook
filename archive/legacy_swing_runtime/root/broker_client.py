"""Broker-facing helpers for the trading webhook.

This module is intentionally side-effect light. It centralizes small broker/env
utilities before the larger Alpaca order and position code is moved out of
app.py in later cleanup patches.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


BROKER_CLIENT_MODULE_VERSION = "patch-392-broker-market-module-split-prep"


@dataclass(frozen=True)
class BrokerCredentials:
    key_id: str
    secret_key: str
    paper: bool


def getenv_any(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def env_bool_any(*names: str, default: str | bool = "false") -> bool:
    value = getenv_any(*names, default=str(default))
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def broker_credentials_from_env() -> BrokerCredentials:
    return BrokerCredentials(
        key_id=getenv_any("APCA_API_KEY_ID", "ALPACA_KEY_ID", "ALPACA_API_KEY_ID", default=""),
        secret_key=getenv_any("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET_KEY", default=""),
        paper=env_bool_any("APCA_PAPER", "ALPACA_PAPER", default=True),
    )


def alpaca_trading_base_url(paper: bool) -> str:
    return "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def normalize_symbols(symbols: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for symbol in symbols or []:
        normalized = normalize_symbol(symbol)
        if not normalized or normalized in seen:
            continue
        out.append(normalized)
        seen.add(normalized)
    return out


def broker_module_status() -> dict:
    creds = broker_credentials_from_env()
    return {
        "ok": True,
        "module": "broker_client",
        "module_version": BROKER_CLIENT_MODULE_VERSION,
        "credentials_configured": bool(creds.key_id and creds.secret_key),
        "paper": bool(creds.paper),
        "base_url": alpaca_trading_base_url(creds.paper),
    }