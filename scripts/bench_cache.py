"""Simulate product lookups with and without the LRU cache.

No AWS calls. Uses the real LRUCache with a Zipf-distributed SKU workload,
where a few popular products get most of the traffic, and counts how many
lookups would have reached DynamoDB. Latency numbers come from assumed
constants, so treat them as a model, not a measurement of AWS.

Usage:  python scripts/bench_cache.py
"""

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from orders.ds.lru import LRUCache  # noqa: E402

REQUESTS = 100_000
SKUS = 10_000
ZIPF_EXPONENT = 1.07
DYNAMO_READ_MS = 4.0  # assumed single-digit-ms GetItem latency
MEMORY_HIT_MS = 0.01  # assumed in-process dict lookup
CACHE_SIZES = [0, 64, 256, 1024, 4096]


def zipf_weights(n: int, s: float) -> list[float]:
    return [1 / (rank**s) for rank in range(1, n + 1)]


def run(cache_size: int, skus: list[str], requests: list[str]) -> dict:
    cache: LRUCache[str, dict] = LRUCache(cache_size) if cache_size else None
    db_reads = 0
    for sku in requests:
        if cache is not None and cache.get(sku) is not None:
            continue
        db_reads += 1
        if cache is not None:
            cache.put(sku, {"sku": sku})
    hit_rate = (cache.hits / len(requests)) if cache is not None else 0.0
    avg_ms = (db_reads * DYNAMO_READ_MS + (len(requests) - db_reads) * MEMORY_HIT_MS) / len(requests)
    return {"cache_size": cache_size, "hit_rate": hit_rate, "db_reads": db_reads, "avg_ms": avg_ms}


def main() -> None:
    rng = random.Random(42)
    skus = [f"SKU-{i:05d}" for i in range(SKUS)]
    requests = rng.choices(skus, weights=zipf_weights(SKUS, ZIPF_EXPONENT), k=REQUESTS)

    print(f"{REQUESTS:,} lookups over {SKUS:,} SKUs (Zipf s={ZIPF_EXPONENT})")
    print(f"{'cache size':>10} | {'hit rate':>8} | {'DynamoDB reads':>14} | {'avg lookup ms (model)':>21}")
    print("-" * 64)
    for size in CACHE_SIZES:
        r = run(size, skus, requests)
        print(
            f"{r['cache_size']:>10} | {r['hit_rate']:>7.1%} | {r['db_reads']:>14,} | {r['avg_ms']:>21.3f}"
        )


if __name__ == "__main__":
    main()
