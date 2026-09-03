"""Local UI preview using synthetic data only; never imports the trading runtime.

Run: python tools/preview_intraday_dashboard.py
"""
import sys
from pathlib import Path
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from regime_intraday_dashboard import render_intraday_dashboard


class Preview(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        page = render_intraday_dashboard(
            scan={"ts_utc": datetime.now(timezone.utc).isoformat(),
                  "regime": {"name": "transition", "direction": None},
                  "config": {"mean_reversion_enabled": True, "trade_symbols": ["SPY"], "symbols": ["SPY", "QQQ"]},
                  "sleeves": {"spy_mean_reversion": {"setup_proximity": [{"symbol": "SPY", "data_ready": True, "regime_ready": True, "stretch_ready": False, "reversal_ready": True, "vwap_distance_atr": -0.72, "required_vwap_atr_band": [1, 2.75], "distance_to_nearest_band_edge_atr": .28, "next_gate": "needs more VWAP stretch"}]}, "dia_mean_reversion": {"setup_proximity": [{"symbol": "DIA", "data_ready": True, "regime_ready": True, "stretch_ready": True, "reversal_ready": False, "vwap_distance_atr": 1.34, "required_vwap_atr_band": [1, 2.75], "distance_to_nearest_band_edge_atr": 0, "next_gate": "waiting for reversal bar confirmation"}]}},
                  "features": {symbol: {"freshness": "fresh", "bars": 130} for symbol in ("SPY", "QQQ", "DIA")},
                  "paper_auto_submit_enabled": True},
            ledger={}, readiness={"paper_ready": True}, scanner={},
            view=query.get("view", ["overview"])[0])
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 8765), Preview).serve_forever()
