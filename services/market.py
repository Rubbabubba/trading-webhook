# services/market.py
# Version: 2025-09-15-ENV-FALLBACK
# - Accepts both ALPACA_* and APCA_* env var names
# - Never hard-fails if one alias is present (robust fallback)
# - Centralizes headers/session + simple diagnostics

from __future__ import annotations

import os
import json
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional

import requests


def _pick_env(*names: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first non-empty env var among names (stripped), else default."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


@dataclass
class Market:
    # Credentials
    api_key: str
    secret_key: str

    # Endpoints & config
    trading_base: str
    data_base: str
    feed: str = "iex"

    # Optional toggles
    market_gate_on: bool = True

    # ---------- Construction ----------

    @classmethod
    def from_env(cls) -> "Market":
        """
        Load from environment with robust fallbacks:
          Keys:
            - ALPACA_API_KEY, ALPACA_API_KEY_ID, APCA_API_KEY_ID
            - ALPACA_SECRET_KEY, ALPACA_API_SECRET, ALPACA_API_SECRET_KEY, APCA_API_SECRET_KEY
          Hosts:
            - Trading: ALPACA_TRADING_BASE_URL, ALPACA_BASE_URL, APCA_API_BASE_URL
            - Data:    ALPACA_DATA_BASE_URL, APCA_DATA_BASE_URL
          Feed:
            - ALPACA_FEED (default: 'iex')
          Market gate:
            - ALPACA_MARKET_GATE (on/off, default: on)
        """
        api_key = _pick_env(
            "ALPACA_API_KEY",
            "ALPACA_API_KEY_ID",
            "APCA_API_KEY_ID",
        )
        secret_key = _pick_env(
            "ALPACA_SECRET_KEY",
            "ALPACA_API_SECRET",
            "ALPACA_API_SECRET_KEY",
            "APCA_API_SECRET_KEY",
        )

        # Endpoints (defaults are safe for paper + data)
        trading_base = _pick_env(
            "ALPACA_TRADING_BASE_URL",
            "ALPACA_BASE_URL",
            "APCA_API_BASE_URL",
            default="https://paper-api.alpaca.markets",
        )
        data_base = _pick_env(
            "ALPACA_DATA_BASE_URL",
            "APCA_DATA_BASE_URL",
            default="https://data.alpaca.markets",
        )

        feed = _pick_env("ALPACA_FEED", default="iex")
        gate_raw = (_pick_env("ALPACA_MARKET_GATE", default="on") or "").lower()
        market_gate_on = gate_raw not in ("off", "0", "false", "no")

        missing = []
        if not api_key:
            missing.append("ALPACA_API_KEY|ALPACA_API_KEY_ID|APCA_API_KEY_ID")
        if not secret_key:
            missing.append("ALPACA_SECRET_KEY|ALPACA_API_SECRET|ALPACA_API_SECRET_KEY|APCA_API_SECRET_KEY")
        if missing:
            # Explain exactly what we looked for (helps when scanning Render env page)
            msg = (
                "Missing required Alpaca credentials.\n"
                f"Looked for: {', '.join(missing)}\n"
                "Add one of the listed variables (aliases are accepted) and redeploy."
            )
            print("\nError:", msg, "\n")
            raise SystemExit(1)

        # Light diag to logs (no secrets)
        print(
            json.dumps(
                {
                    "event": "market_env_loaded",
                    "trading_base": trading_base,
                    "data_base": data_base,
                    "feed": feed,
                    "market_gate": "on" if market_gate_on else "off",
                }
            )
        )

        return cls(
            api_key=api_key,
            secret_key=secret_key,
            trading_base=trading_base,
            data_base=data_base,
            feed=feed,
            market_gate_on=market_gate_on,
        )

    # ---------- HTTP helpers ----------

    @property
    def headers(self) -> Dict[str, str]:
        # Alpaca v2 auth header names:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "equities-webhook/2025-09",
        }

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        r = requests.get(url, headers=self.headers, params=params, timeout=30)
        r.raise_for_status()
        return r

    def _post(self, url: str, payload: Dict[str, Any]) -> requests.Response:
        r = requests.post(url, headers=self.headers, json=payload, timeout=30)
        r.raise_for_status()
        return r

    # ---------- Minimal API ----------

    def get_clock(self) -> Dict[str, Any]:
        """Return Alpaca trading clock (via trading endpoint)."""
        url = f"{self.trading_base.rstrip('/')}/v2/clock"
        return self._get(url).json()

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "1Min",
        limit: int = 400,
        adjustment: str = "raw",
        feed: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch bars from the data endpoint."""
        url = f"{self.data_base.rstrip('/')}/v2/stocks/{symbol}/bars"
        params = {
            "timeframe": timeframe,
            "limit": limit,
            "adjustment": adjustment,
            "feed": (feed or self.feed),
        }
        return self._get(url, params=params).json()

    # Simple diagnostics used by app routes
    def diag(self) -> Dict[str, Any]:
        return {
            "trading_base": self.trading_base,
            "data_base": self.data_base,
            "feed": self.feed,
            "market_gate": "on" if self.market_gate_on else "off",
        }
