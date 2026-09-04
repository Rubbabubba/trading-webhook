"""One-shot Opportunity Lab collector command for a Render cron job."""

import json
import os
import urllib.request


def main() -> None:
    base = (os.getenv("OPPORTUNITY_SERVICE_URL") or "").strip().rstrip("/")
    secret = (os.getenv("OPPORTUNITY_WORKER_SECRET") or "").strip()
    if not base or not secret:
        raise RuntimeError("OPPORTUNITY_SERVICE_URL and OPPORTUNITY_WORKER_SECRET are required")
    request = urllib.request.Request(
        f"{base}/worker/opportunity-lab/collect-kalshi",
        data=json.dumps({"worker_secret": secret, "pages": 10, "limit": 200}).encode(),
        method="POST", headers={"Content-Type": "application/json", "User-Agent": "OpportunityLabWorker/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        print(response.read().decode(), flush=True)


if __name__ == "__main__":
    main()
