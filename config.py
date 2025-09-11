import os
import logging
import requests

APP_VERSION = os.getenv("APP_VERSION", "2025-09-10-05")

# API keys
APCA_API_KEY_ID     = os.getenv("APCA_API_KEY_ID", "")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY", "")

# Alpaca base URLs
ALPACA_BASE = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets/v2")
DATA_BASE   = os.getenv("APCA_DATA_BASE_URL", "https://data.alpaca.markets/v2")
DATA_FEED   = os.getenv("APCA_DATA_FEED", "iex")  # iex (paper) or sip (live)

# Safety caps
MAX_OPEN_POSITIONS         = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
MAX_POSITIONS_PER_SYMBOL   = int(os.getenv("MAX_POSITIONS_PER_SYMBOL", "1"))
MAX_POSITIONS_PER_STRATEGY = int(os.getenv("MAX_POSITIONS_PER_STRATEGY", "3"))

# Order behavior
CANCEL_OPEN_ORDERS_BEFORE_PLAIN = os.getenv("CANCEL_OPEN_ORDERS_BEFORE_PLAIN", "true").lower() == "true"
ATTACH_OCO_ON_PLAIN             = os.getenv("ATTACH_OCO_ON_PLAIN", "true").lower() == "true"
OCO_TIF                         = os.getenv("OCO_TIF", "gtc")
OCO_REQUIRE_POSITION            = os.getenv("OCO_REQUIRE_POSITION", "true").lower() == "true"

# Scanner/strategy defaults
S2_WHITELIST       = os.getenv("S2_WHITELIST", "SPY,QQQ,TSLA,NVDA,COIN,GOOGL,META,MSFT,AMZN,AAPL")
S2_TF_DEFAULT      = os.getenv("S2_TF_DEFAULT", "60")
S2_MODE_DEFAULT    = os.getenv("S2_MODE_DEFAULT", "either")
S2_USE_RTH_DEFAULT = os.getenv("S2_USE_RTH_DEFAULT", "true")
S2_USE_VOL_DEFAULT = os.getenv("S2_USE_VOL_DEFAULT", "false")

# Strategy display
CURRENT_STRATEGIES = [s.strip() for s in os.getenv(
    "CURRENT_STRATEGIES", "SPY_VWAP_EMA20,SMA10D_MACD"
).split(",") if s.strip()]

# HTTP session
SESSION = requests.Session()
HEADERS = {
    "APCA-API-KEY-ID": APCA_API_KEY_ID,
    "APCA-API-SECRET-KEY": APCA_API_SECRET_KEY,
    "Content-Type": "application/json",
}

# Budgets
MAX_DAYS_PERF       = int(os.getenv("MAX_DAYS_PERF", "45"))
MAX_ACTIVITY_DAYS   = int(os.getenv("MAX_ACTIVITY_DAYS", "60"))
MAX_ORDER_LOOKUPS   = int(os.getenv("MAX_ORDER_LOOKUPS", "60"))
LOOKUP_BUDGET_SEC   = float(os.getenv("LOOKUP_BUDGET_SEC", "5.0"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("equities-webhook")