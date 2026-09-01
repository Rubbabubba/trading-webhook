from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from swing_performance_reports import build_replay_promotion_gate_report

PATCH_VERSION = "patch-704-out-of-sample-replay-promotion-gate"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _default_input_dirs() -> list[Path]:
    home = Path.home()
    return [
        home / "TradingDiagnostics" / "swing_minute_replay",
        home / "TradingDiagnostics" / "swing_two_week_replay",
    ]


def _parse_input_dirs(values: list[str] | None) -> list[Path]:
    raw: list[str] = []
    for value in list(values or []):
        raw.extend(part for part in str(value or "").split(";") if part.strip())
    env_value = os.getenv("SWING_REPLAY_PROMOTION_INPUT_DIRS", "")
    raw.extend(part for part in env_value.split(";") if part.strip())
    return [Path(part).expanduser() for part in raw] if raw else _default_input_dirs()


def _collect_replay_inputs(input_dirs: list[Path]) -> list[dict]:
    replay_inputs: list[dict] = []
    for input_dir in input_dirs:
        if not input_dir.exists():
            continue
        label = input_dir.name
        matrix = _load_json(input_dir / "latest_scenario_matrix.json")
        if matrix is not None:
            replay_inputs.append({
                "window": f"{label}:scenario_matrix",
                "payload": matrix,
                "source_path": str(input_dir / "latest_scenario_matrix.json"),
            })
        summary = _load_json(input_dir / "latest_summary.json")
        if summary is not None:
            replay_inputs.append({
                "window": f"{label}:summary",
                "payload": summary,
                "source_path": str(input_dir / "latest_summary.json"),
            })
    return replay_inputs


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only replay promotion gate report from local replay artifacts.")
    parser.add_argument("--input-dir", action="append", default=None, help="Replay output directory. Can be passed multiple times or separated by semicolons.")
    parser.add_argument("--output-dir", default=str(Path.home() / "TradingDiagnostics" / "replay_promotion_gate"))
    parser.add_argument("--min-trades", type=int, default=_env_int("REPLAY_PROMOTION_GATE_MIN_TRADES", 10))
    parser.add_argument("--min-total-pnl", type=float, default=_env_float("REPLAY_PROMOTION_GATE_MIN_TOTAL_PNL", 0.0))
    parser.add_argument("--min-avg-r", type=float, default=_env_float("REPLAY_PROMOTION_GATE_MIN_AVG_R", 0.05))
    parser.add_argument("--min-win-rate", type=float, default=_env_float("REPLAY_PROMOTION_GATE_MIN_WIN_RATE", 0.5))
    parser.add_argument("--max-drawdown", type=float, default=_env_float("REPLAY_PROMOTION_GATE_MAX_DRAWDOWN", 0.0))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    input_dirs = _parse_input_dirs(args.input_dir)
    replay_inputs = _collect_replay_inputs(input_dirs)
    report = build_replay_promotion_gate_report(
        patch_version=PATCH_VERSION,
        replay_inputs=replay_inputs,
        min_trades=args.min_trades,
        min_total_pnl=args.min_total_pnl,
        min_avg_r=args.min_avg_r,
        min_win_rate=args.min_win_rate,
        max_drawdown=args.max_drawdown,
        limit=args.limit,
    )
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["input_dirs"] = [str(path) for path in input_dirs]
    report["input_count"] = len(replay_inputs)
    report["snapshot_target_hint"] = "Set REPLAY_PROMOTION_GATE_SNAPSHOT_PATH to latest_replay_promotion_gate.json if you want Render to expose this snapshot."

    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    latest_json = output_dir / "latest_replay_promotion_gate.json"
    latest_csv = output_dir / "latest_replay_promotion_gate.csv"
    run_json = run_dir / "replay_promotion_gate.json"
    run_csv = run_dir / "replay_promotion_gate.csv"
    text = json.dumps(report, indent=2, default=str)
    latest_json.write_text(text, encoding="utf-8")
    run_json.write_text(text, encoding="utf-8")
    rows = [dict(row or {}) for row in list(report.get("scenario_rows") or []) if isinstance(row, dict)]
    _write_csv(latest_csv, rows)
    _write_csv(run_csv, rows)

    print(json.dumps({
        "ok": True,
        "patch_version": PATCH_VERSION,
        "output_json": str(latest_json),
        "output_csv": str(latest_csv),
        "scenario_count": report.get("scenario_count"),
        "promotion_ready_count": report.get("promotion_ready_count"),
        "recommended_action": report.get("recommended_action"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
