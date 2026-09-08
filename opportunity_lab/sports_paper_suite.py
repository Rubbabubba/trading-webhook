"""Prepare verified game mappings and supervise a bounded week of paper tests."""
import argparse
from datetime import timedelta
import json
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

from .college_football_paper import lock_process, timestamp, utcnow, write_json
from .sports_paper_v2 import DEFAULTS, SPORTS, VERSION, fetch

ALIASES = {"mlb": {"ARI": "AZ", "CHW": "CWS"}, "nfl": {"JAX": "JAC", "WSH": "WAS"}}


def match_event(event, markets, league):
    competition = event["competitions"][0]
    teams = {c["homeAway"]: c for c in competition["competitors"]}
    codes = {s: ALIASES[league].get(c["team"]["abbreviation"], c["team"]["abbreviation"]) for s, c in teams.items()}
    eastern = timestamp(event["date"]).astimezone(ZoneInfo("America/New_York"))
    date_code = eastern.strftime("%y%b%d").upper()
    time_code = eastern.strftime("%H%M") if league == "mlb" else ""
    stem = SPORTS[league][1] + "-" + date_code + time_code + codes["away"] + codes["home"]
    mapped = {side: stem + "-" + code for side, code in codes.items()}
    # Exact date/time and both team tickers prevent fuzzy or doubleheader matches.
    if not all(ticker in markets and markets[ticker].get("event_ticker") == stem for ticker in mapped.values()):
        return None
    return {**DEFAULTS, "league": league, "event_id": str(event["id"]), "game": event["name"],
            "home_team_id": str(teams["home"]["id"]), "away_team_id": str(teams["away"]["id"]),
            "markets": mapped, "market_event": stem, "kickoff": timestamp(event["date"]).isoformat(),
            "stop_at": (timestamp(event["date"]) + timedelta(hours=12)).isoformat(),
            "mapping_evidence": {s: {k: markets[t].get(k) for k in ("ticker", "title", "rules_primary", "rules_secondary")}
                                 for s, t in mapped.items()}}


def prepare(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    manifest = {"version": VERSION, "games": [], "unmatched": []}
    for league, dates in (("mlb", "20260908"), ("nfl", "20260909-20260914")):
        path, series = SPORTS[league]
        schedule = fetch(f"https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard?dates={dates}&limit=100")
        if not schedule["ok"]:
            raise RuntimeError(schedule)
        markets, cursor = {}, ""
        for _ in range(10):
            result = fetch(f"https://external-api.kalshi.com/trade-api/v2/markets?series_ticker={series}&status=open&limit=1000&cursor={cursor}")
            if not result["ok"]:
                raise RuntimeError(result)
            markets.update({m["ticker"]: m for m in result["data"].get("markets", [])})
            cursor = result["data"].get("cursor", "")
            if not cursor:
                break
        if cursor:
            raise RuntimeError("Market pagination incomplete")
        for event in schedule["data"].get("events", []):
            config = match_event(event, markets, league)
            if config is None:
                manifest["unmatched"].append({"league": league, "game": event["name"], "id": event["id"]})
                continue
            slug = league + "_" + str(event["id"])
            path = directory / (slug + ".json")
            if path.exists() and json.loads(path.read_text()) != config:
                raise RuntimeError("Refusing to overwrite existing frozen config: " + str(path))
            write_json(path, config)
            manifest["games"].append({"slug": slug, "config": path.name, "league": league,
                                      "game": config["game"], "kickoff": config["kickoff"], "stop_at": config["stop_at"]})
    manifest["games"].sort(key=lambda g: g["kickoff"])
    manifest_path = directory / "manifest.json"
    if manifest_path.exists() and json.loads(manifest_path.read_text()) != manifest:
        raise RuntimeError("Refusing to overwrite a changed manifest")
    write_json(manifest_path, manifest)
    print(json.dumps({"manifest": str(manifest_path), "games": len(manifest["games"]), "unmatched": manifest["unmatched"]}, indent=2))


def supervise(manifest_path, output):
    manifest_path = Path(manifest_path).resolve()
    manifest = json.loads(manifest_path.read_text())
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock = lock_process(output / "suite.lock")
    processes, last_launch = {}, {}
    try:
        while True:
            now, rows = utcnow(), []
            for game in manifest["games"]:
                folder = output / game["slug"]
                folder.mkdir(exist_ok=True)
                status_path = folder / "status.json"
                try:
                    saved = json.loads(status_path.read_text()) if status_path.exists() else {}
                except (OSError, json.JSONDecodeError):
                    saved = {}
                terminal = saved.get("worker_state") in {"completed", "deadline_reached"}
                expired = now >= timestamp(game["stop_at"])
                # Collect pregame context from two hours before kickoff.
                due = now >= timestamp(game["kickoff"]) - timedelta(hours=2)
                child = processes.get(game["slug"])
                running = child is not None and child.poll() is None
                paused = (output / "PAUSE").exists() or (folder / "PAUSE").exists()
                # If this supervisor restarted, an existing collector holds its
                # own OS lock. Probe that lock before attempting any launch.
                if due and not expired and not terminal and not running and not paused:
                    elapsed = (now - last_launch.get(game["slug"], now - timedelta(minutes=10))).total_seconds()
                    if elapsed >= 120:
                        try:
                            probe = lock_process(folder / "collector.lock")
                            probe.close()
                        except (RuntimeError, OSError):
                            running = True
                        else:
                            with (folder / "worker.log").open("a", encoding="utf-8") as log:
                                child = subprocess.Popen([sys.executable, "-m", "opportunity_lab.sports_paper_v2", "--config",
                                    str(manifest_path.parent / game["config"]), "--output", str(folder)],
                                    cwd=Path(__file__).resolve().parents[1], stdout=log, stderr=log,
                                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                            processes[game["slug"]] = child
                            last_launch[game["slug"]] = now
                            write_json(folder / "process.json", {"pid": child.pid, "started_at": now.isoformat()})
                            running = True
                rows.append({**game, "state": saved.get("worker_state", "expired" if expired else "waiting"),
                             "running": running, "last_sample": saved.get("at"),
                             "account": saved.get("account"), "transport_errors": saved.get("transport_errors", {}),
                             "worker_error": saved.get("error")})
            write_json(output / "status.json", {"at": now.isoformat(), "mode": "paper", "version": VERSION, "games": rows})
            if all(now >= timestamp(g["stop_at"]) or r["state"] in {"completed", "deadline_reached"} for g, r in zip(manifest["games"], rows)):
                break
            time.sleep(30)
    finally:
        lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--directory", default="configs/sports_week_20260908")
    p = sub.add_parser("run")
    p.add_argument("--manifest", default="configs/sports_week_20260908/manifest.json")
    p.add_argument("--output", default="sports_paper/week_20260908")
    args = parser.parse_args()
    prepare(args.directory) if args.command == "prepare" else supervise(args.manifest, args.output)


if __name__ == "__main__":
    main()
