"""Run the fail-closed intraday qualification report from a saved ledger."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from regime_intraday_ledger import load_ledger
from regime_intraday_qualification import qualification_report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", default="regime_intraday_ledger.json")
    parser.add_argument("--output", default="intraday_qualification_report.json")
    parser.add_argument("--trials", type=int, default=2000)
    args = parser.parse_args()
    report = qualification_report(load_ledger(args.ledger), trials=args.trials)
    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("paper_production_qualified", "paper_blockers", "live_capital_qualified", "live_blockers")}, indent=2))
    return 0 if report["paper_production_qualified"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
