"""Send a batch of orders to the deployed API at a capped rate and report client-side timings.

The API throttles at 20 requests/s, so the default rate stays below that.

Usage (from the project folder, with the Zscaler bundle in place):
  SSL_CERT_FILE="$HOME/aws-ca-bundle.pem" python3 scripts/load_test.py --api <ApiUrl> --orders 100 --rate 10
"""

import argparse
import json
import os
import random
import ssl
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

SKUS = ["MUG-001", "LAMP-002", "BAG-003"]
REGIONS = ["NORTH", "EAST", "SOUTH", "WEST"]


def make_ssl_context() -> ssl.SSLContext:
    # Certificate checks stay on. Only the strict X.509 flag is relaxed, because
    # the Zscaler root certificate lacks a key-usage extension that Python's
    # OpenSSL rejects. AWS CLI accepts the same certificate.
    context = ssl.create_default_context(cafile=os.environ.get("SSL_CERT_FILE"))
    context.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return context


SSL_CONTEXT = make_ssl_context()


def post_order(api: str, key: str, body: dict) -> tuple[int, float]:
    request = urllib.request.Request(
        f"{api}/orders",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=30, context=SSL_CONTEXT) as response:
            status = response.status
            response.read()
    except urllib.error.HTTPError as err:
        status = err.code
    return status, (time.perf_counter() - started) * 1000


def percentile(sorted_values: list[float], pct: float) -> float:
    rank = max(0, min(len(sorted_values) - 1, round(pct / 100 * len(sorted_values)) - 1))
    return sorted_values[rank]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True, help="ApiUrl from the stack outputs")
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--rate", type=float, default=10, help="target requests per second")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    rng = random.Random(run_id)
    jobs = [
        (
            f"load-{run_id}-{i}",
            {
                "customer_id": f"load-{i}",
                "region": rng.choice(REGIONS),
                "lines": [{"sku": SKUS[i % len(SKUS)], "quantity": 1}],
            },
        )
        for i in range(args.orders)
    ]

    start = time.perf_counter()

    def send(indexed_job: tuple[int, tuple[str, dict]]) -> tuple[int, float]:
        index, (key, body) = indexed_job
        wait = start + index / args.rate - time.perf_counter()
        if wait > 0:
            time.sleep(wait)
        return post_order(args.api, key, body)

    window_start = datetime.now(timezone.utc)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(send, enumerate(jobs)))
    wall_seconds = time.perf_counter() - start
    window_end = datetime.now(timezone.utc)

    statuses = Counter(status for status, _ in results)
    latencies = sorted(ms for _, ms in results)

    print(f"run id:            {run_id}")
    print(f"orders sent:       {args.orders}")
    print(f"status codes:      {dict(statuses)}")
    print(f"wall time:         {wall_seconds:.1f} s  ({args.orders / wall_seconds:.1f} requests/s achieved)")
    print(f"POST latency ms:   p50 {percentile(latencies, 50):.0f}  p95 {percentile(latencies, 95):.0f}  max {latencies[-1]:.0f}")
    print(f"window (UTC):      {window_start.isoformat()}  ->  {window_end.isoformat()}")
    print("Wait 2-3 minutes for metrics to appear, then run scripts/metrics_report.py with these times.")


if __name__ == "__main__":
    main()
