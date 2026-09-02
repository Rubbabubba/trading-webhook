"""Generate the categorized endpoint manifest from FastAPI decorators."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "SYSTEM_ENDPOINTS.md"
BASE = "https://trading-webhook-q4d5.onrender.com"


def main() -> None:
    sources = [ROOT / "intraday_app.py", ROOT / "regime_intraday_api.py"]
    found = []
    for source in sources:
        text = source.read_text(encoding="utf-8")
        found.extend(re.findall(r'@(app|router)\.(get|post|put|patch|delete)\(\s*["\']([^"\']+)', text))
    found = [(method, path) for _owner, method, path in found]
    groups: dict[str, list[tuple[str, str]]] = {"Operator dashboards": [], "Regime intraday": [], "Runtime workers and controls": [], "Swing active diagnostics": [], "Research and deprecation candidates": [], "Shared and other": []}
    for method, path in sorted(set(found), key=lambda row: row[1]):
        if path.startswith("/dashboard"):
            group = "Operator dashboards"
        elif "regime_intraday" in path:
            group = "Regime intraday"
        elif path.startswith("/worker") or path in {"/kill", "/unkill", "/webhook"} or path.startswith("/admin"):
            group = "Runtime workers and controls"
        elif any(token in path for token in ("lab", "simulation", "tuning", "audit", "forensics", "replay")):
            group = "Research and deprecation candidates"
        elif path.startswith("/diagnostics/swing") or path.startswith("/diagnostics/live") or path.startswith("/diagnostics/position"):
            group = "Swing active diagnostics"
        else:
            group = "Shared and other"
        groups[group].append((method.upper(), path))
    lines = ["# System endpoint manifest", "", "Generated from the intraday-only application by `tools/generate_endpoint_manifest.py`. Do not edit endpoint rows manually.", "", f"Total application routes: **{len(set(found))}**.", "", "Dashboard and diagnostic detail require operator authentication. Worker routes require the worker secret.", ""]
    for heading, rows in groups.items():
        lines += [f"## {heading}", "", "| Method | Endpoint |", "|---|---|"]
        lines += [f"| `{method}` | [{path}]({BASE}{path}) |" for method, path in rows]
        lines.append("")
    OUTPUT.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
