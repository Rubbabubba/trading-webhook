import os
import logging
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Any, Type

from flask import Flask, request, jsonify, make_response

# --- Optional/defensive imports for existing systems (S1/S2) ---
S1_VERSION = None
S2_VERSION = None
try:
    from strategies.s1 import __version__ as S1_VERSION  # noqa: F401
except Exception:
    pass
try:
    from strategies.s2 import __version__ as S2_VERSION  # noqa: F401
except Exception:
    pass

# --- New systems (S3/S4) ---
from strategies.s3 import S3OpeningRange, __version__ as S3_VERSION
from strategies.s4 import S4RSIPullback, __version__ as S4_VERSION

# Shared services (already in your repo)
from services.market import Market
from services.alpaca_exec import execute_orderplan_batch

# --------------------------------------------------------------------------------------
# App setup
# --------------------------------------------------------------------------------------
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("webhook-app")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _to_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _symbols_for(prefix: str) -> list[str]:
    # Prefix symbols override global EQUITY_SYMBOLS
    raw = os.getenv(f"{prefix}_SYMBOLS") or os.getenv("EQUITY_SYMBOLS", "SPY")
    return [s.strip() for s in raw.split(",") if s.strip()]


def _respond(payload: Dict[str, Any], version_header: str | None = None):
    # Attach a request-id for correlation and optional version header
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    resp = make_response(jsonify(payload))
    resp.headers["X-Request-ID"] = rid
    if version_header:
        resp.headers["X-Strategy-Version"] = version_header
    return resp


def _run_strategy(
    *,
    strategy_cls: Type,
    system_name: str,
    system_version: str,
    env_prefix: str,
) -> Any:
    """
    Shared execution path for /scan/* endpoints.
    Respects ?dry= param or {PREFIX}_DRY env default.
    """
    dry_default = _to_bool(os.getenv(f"{env_prefix}_DRY", "1"), default=True)
    dry = _to_bool(request.args.get("dry"), default=dry_default)
    symbols = _symbols_for(env_prefix)

    mkt = Market()
    strat = strategy_cls(mkt)
    plans = strat.scan(mkt.now(), mkt, symbols)

    if dry:
        payload = {
            "system": system_name,
            "version": system_version,
            "dry": True,
            "symbols": symbols,
            "plans": [p.__dict__ for p in plans],
        }
        return _respond(payload, version_header=f"{env_prefix}/{system_version}")

    # Live (paper/real) execution via Alpaca
    results = execute_orderplan_batch(plans, system=system_name)
    payload = {
        "system": system_name,
        "version": system_version,
        "dry": False,
        "symbols": symbols,
        "results": results,
    }
    return _respond(payload, version_header=f"{env_prefix}/{system_version}")


# --------------------------------------------------------------------------------------
# Health & diagnostics
# --------------------------------------------------------------------------------------
@app.get("/health/ping")
def health_ping():
    return _respond({"ok": True, "now_utc": datetime.now(timezone.utc).isoformat()})


@app.get("/health/versions")
def health_versions():
    payload = {
        "S1": S1_VERSION,
        "S2": S2_VERSION,
        "S3_OPENING_RANGE": S3_VERSION,
        "S4_RSI_PB": S4_VERSION,
    }
    return _respond(payload)


# --------------------------------------------------------------------------------------
# Scan routes
#   (S1/S2 registered only if present in the environment; S3/S4 always available here)
# --------------------------------------------------------------------------------------
# If you want S1/S2 here as well, you can mirror the pattern used for S3/S4.

@app.post("/scan/s3")
def scan_s3():
    return _run_strategy(
        strategy_cls=S3OpeningRange,
        system_name="OPENING_RANGE",
        system_version=S3_VERSION,
        env_prefix="S3",
    )


@app.post("/scan/s4")
def scan_s4():
    return _run_strategy(
        strategy_cls=S4RSIPullback,
        system_name="RSI_PB",
        system_version=S4_VERSION,
        env_prefix="S4",
    )


# --------------------------------------------------------------------------------------
# Error handling
# --------------------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(e):
    return _respond({"error": "not_found", "message": "Route does not exist."}), 404


@app.errorhandler(Exception)
def on_exception(e):
    log.exception("Unhandled error")
    return _respond({"error": "internal_error", "message": str(e)}), 500


# --------------------------------------------------------------------------------------
# Entrypoint (local dev)
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
