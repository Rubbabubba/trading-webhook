# strategies/book.py
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd

# ---- Defaults (kept consistent with your current behavior) ----
DEFAULT_MIN_ATR_PCT = float(os.getenv('MIN_ATR_PCT', '0.08'))  # default 8% for 5m
DEFAULT_BOOK_TOPK = int(float(os.getenv("BOOK_TOPK", "2")))
DEFAULT_BOOK_MIN_SCORE = float(os.getenv("BOOK_MIN_SCORE", "0.10"))
DEFAULT_ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULT", "1.5"))
DEFAULT_MTF_CONFIRM = (str(os.getenv("MTF_CONFIRM", "true")).lower() in ("1","true","yes","on"))

# NEW: minimum bar counts (global defaults; can be overridden via env)
DEFAULT_MIN_BARS_1M = int(float(os.getenv("BOOK_MIN_BARS_1M", "50")))
DEFAULT_MIN_BARS_5M = int(float(os.getenv("BOOK_MIN_BARS_5M", "50")))

# ====== utilities ======
def _roll_mean(a, n): return pd.Series(a).rolling(n).mean().to_numpy()
def _roll_std(a, n):  return pd.Series(a).rolling(n).std(ddof=0).to_numpy()

def _zscore(x, n):
    s = pd.Series(x)
    m = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0).replace(0, np.nan)
    return ((s - m) / sd).to_numpy()

def _atr(high, low, close, n=14):
    h, l, c = map(pd.Series, (high, low, close))
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean().to_numpy()

def _rsi(values, n=14):
    s = pd.Series(values)
    d = s.diff()
    up = d.clip(lower=0).rolling(n).mean()
    dn = (-d.clip(upper=0)).rolling(n).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).to_numpy()

def _roc(values, n=12):
    v = pd.Series(values)
    return (v / v.shift(n) - 1.0).to_numpy()

# ====== small config helpers (per-strategy override -> global -> default) ======
def _cfg_bool(key: str, strat: Optional[str], default: bool) -> bool:
    if strat:
        v = os.getenv(f"{strat.upper()}_{key}")
        if v is not None:
            return str(v).lower() in ("1", "true", "yes", "on")
    v = os.getenv(key)
    if v is not None:
        return str(v).lower() in ("1", "true", "yes", "on")
    return default

def _cfg_float(key: str, strat: Optional[str], default: float) -> float:
    if strat:
        v = os.getenv(f"{strat.upper()}_{key}")
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    v = os.getenv(key)
    if v is not None:
        try:
            return float(v)
        except Exception:
            pass
    return default

def _cfg_int(key: str, strat: Optional[str], default: int) -> int:
    if strat:
        v = os.getenv(f"{strat.upper()}_{key}")
        if v is not None:
            try:
                return int(float(v))
            except Exception:
                pass
    v = os.getenv(key)
    if v is not None:
        try:
            return int(float(v))
        except Exception:
            pass
    return default

# ====== data classes ======
@dataclass
class ScanRequest:
    strat: str
    timeframe: str
    limit: int
    topk: int
    min_score: float
    notional: float

@dataclass
class ScanResult:
    symbol: str
    action: str
    reason: str
    score: float
    atr: float
    atr_pct: float
    qty: float
    notional: float
    selected: bool

# ====== regime computation ======
@dataclass
class Regimes:
    trend_z: float
    atr: float
    atr_pct: float
    sma_fast: float
    sma_slow: float

def compute_regimes(close, high, low) -> Regimes:
    sma_f = _roll_mean(close, 20)
    sma_s = _roll_mean(close, 60)
    trend_z = _zscore(sma_f - sma_s, 60)
    atr = _atr(high, low, close, 14)
    # Percentile of ATR over 200 bars, used as a "volatility percentile"
    atr_pct = pd.Series(atr).rolling(200).rank(pct=True).to_numpy()
    i = len(close) - 1
    def last(x): return float(x[i]) if len(x) else float("nan")
    return Regimes(
        trend_z=last(trend_z),
        atr=last(atr),
        atr_pct=last(atr_pct),
        sma_fast=last(sma_f),
        sma_slow=last(sma_s),
    )

# ====== raw signals ======
def sig_c1_adaptive_rsi(close, regimes: Regimes,
                        rsi_len=14, band_lookback=100, k=0.7, min_atr_pct=DEFAULT_MIN_ATR_PCT):
    r = _rsi(close, rsi_len)
    s = pd.Series(r).rolling(max(30, band_lookback)).std(ddof=0).to_numpy()
    i = len(close) - 1
    lower = 50 - k * (s[i] if np.isfinite(s[i]) else 5)
    upper = 50 + k * (s[i] if np.isfinite(s[i]) else 5)
    action, score, reason = "flat", 0.0, "no_raw_signal"
            action, score, reason = "flat", 0.0, "no_raw_signal"
            elif s == "e1":
                min_dev_atr = float(os.getenv("E1_MIN_DEV_ATR", "1.5"))
                bos_lb      = int(os.getenv("E1_BOS_LOOKBACK", "10"))
                vol_sma_n   = int(os.getenv("E1_VOL_SMA_N", "9"))
                vol_mult    = float(os.getenv("E1_VOL_MULT", "1.0"))
                retest_lb   = int(os.getenv("E1_RETEST_LOOKBACK", "4"))
                retest_tol  = float(os.getenv("E1_RETEST_TOL_ATR", "0.15"))
                action, score, reason = sig_e1_vwap_bos(
                    one, five,
                    min_dev_atr=min_dev_atr,
                    bos_lookback=bos_lb,
                    vol_sma_n=vol_sma_n,
                    vol_mult=vol_mult,
                    retest_lookback=retest_lb,
                    retest_tol_atr=retest_tol,
                )
            else:
                action, score, reason = "flat", 0.0, "unknown_strategy"
                action, score, reason = "flat", 0.0, "filt_mtf_disagree"