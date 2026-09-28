"""Run the established Demo maker and the isolated sports shadow collector."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import time

from .kalshi_demo_v5_maker_worker import run as run_maker
from .kalshi_external_sleeves_worker import run as run_research_sleeves
from .sports_persistent_passive_worker import run as run_sports


def sports_supervisor(root):
    while True:
        try:
            run_sports(root/'sports-challenger')
        except Exception as exc:
            # The shadow collector has no order path and must never take down
            # reconciliation or the established all-market Demo worker.
            print(json.dumps({
                'at': datetime.now(timezone.utc).isoformat(),
                'event': 'sports_challenger_supervisor_restart',
                'error_type': type(exc).__name__,
            }), flush=True)
            time.sleep(60)


def research_sleeves_supervisor(root):
    while True:
        try:
            run_research_sleeves(root)
        except Exception as exc:
            print(json.dumps({
                'at': datetime.now(timezone.utc).isoformat(),
                'event': 'research_sleeves_supervisor_restart',
                'error_type': type(exc).__name__,
            }), flush=True)
            time.sleep(60)


def run(data_root, cycles=None):
    root = Path(data_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if cycles is None:
        threading.Thread(target=sports_supervisor, args=(root,), daemon=True).start()
        threading.Thread(target=research_sleeves_supervisor, args=(root,), daemon=True).start()
    else:
        # Bounded validation runs exercise both services without a background
        # thread surviving the test process.
        run_sports(root/'sports-challenger', cycles=cycles)
        run_research_sleeves(root, cycles=cycles)
    run_maker(root, cycles=cycles)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root', default='/var/data/kalshi-demo-v9')
    parser.add_argument('--cycles', type=int)
    args = parser.parse_args(argv)
    run(args.data_root, cycles=args.cycles)


if __name__ == '__main__':
    main()
