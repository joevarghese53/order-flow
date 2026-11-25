"""Report OrderService custom metrics for a time window from CloudWatch.

Usage:
  python3 scripts/metrics_report.py --start 2026-11-25T10:00:00 --end 2026-11-25T10:10:00
Times are UTC. Use the window printed by load_test.py. Metrics can take a few minutes to appear.

Counts are summed over one-minute datapoints. Latency average is total Sum / total SampleCount,
which stays exact across minute boundaries. Percentiles aren't reported because they can't be
combined across minutes.
"""

import argparse
from datetime import datetime, timezone

import boto3

DIMENSIONS = [{"Name": "Service", "Value": "order-service"}]


def totals(cloudwatch, start: datetime, end: datetime, name: str) -> dict:
    points = cloudwatch.get_metric_statistics(
        Namespace="OrderService",
        MetricName=name,
        Dimensions=DIMENSIONS,
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Sum", "SampleCount", "Maximum"],
    )["Datapoints"]
    return {
        "sum": sum(p["Sum"] for p in points),
        "count": sum(p["SampleCount"] for p in points),
        "max": max((p["Maximum"] for p in points), default=0),
    }


def minute_floor(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="UTC, e.g. 2026-11-25T10:00:00")
    parser.add_argument("--end", required=True, help="UTC, e.g. 2026-11-25T10:10:00")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    start = minute_floor(datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc))
    end = minute_floor(datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc))
    minutes = max(1, int((end - start).total_seconds() // 60) + 1)

    cloudwatch = boto3.client("cloudwatch", region_name=args.region)
    confirmed = totals(cloudwatch, start, end, "OrdersConfirmed")
    rejected = totals(cloudwatch, start, end, "OrdersRejected")
    hits = totals(cloudwatch, start, end, "CacheHit")
    misses = totals(cloudwatch, start, end, "CacheMiss")
    latency = totals(cloudwatch, start, end, "EndToEndLatency")

    lookups = hits["sum"] + misses["sum"]

    print(f"window: {start.isoformat()} -> {end.isoformat()} UTC ({minutes} minute buckets)")
    print(f"confirmed: {confirmed['sum']:.0f}   rejected: {rejected['sum']:.0f}")
    print(f"confirmed per minute over the window: {confirmed['sum'] / minutes:.1f}")
    if latency["count"]:
        print(
            f"end-to-end latency ms (order accepted -> confirmed): "
            f"avg {latency['sum'] / latency['count']:.0f}  max {latency['max']:.0f}  "
            f"(from {latency['count']:.0f} orders)"
        )
    else:
        print("end-to-end latency: no data yet, wait a few minutes and rerun")
    if lookups:
        print(f"cache: {hits['sum']:.0f} hits / {lookups:.0f} lookups = {hits['sum'] / lookups:.1%} hit rate")
    else:
        print("cache: no lookups found in this window")


if __name__ == "__main__":
    main()
