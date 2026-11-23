"""Custom CloudWatch metrics via the Embedded Metric Format (EMF).

Writing a JSON line to stdout is enough: CloudWatch Logs extracts the metric
without an extra API call or SDK dependency.
"""

import json
import time

from orders.config import METRICS_NAMESPACE


def emit(name: str, value: float, unit: str = "Count") -> None:
    record = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": METRICS_NAMESPACE,
                    "Dimensions": [["Service"]],
                    "Metrics": [{"Name": name, "Unit": unit}],
                }
            ],
        },
        "Service": "order-service",
        name: value,
    }
    print(json.dumps(record))
