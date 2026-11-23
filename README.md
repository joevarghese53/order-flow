# Serverless order processing

An online-store backend. Customers place orders through API Gateway. The API
stores each order and enqueues it in SQS, then returns right away. A Lambda
worker reserves stock in DynamoDB with conditional writes, confirms or rejects
the order, and publishes the result to SNS. Messages that keep failing go to a
dead-letter queue, and a CloudWatch alarm fires when that happens.

Stack: API Gateway (HTTP API), Lambda (Python 3.12, arm64), DynamoDB, SQS, SNS,
CloudWatch, IAM. Deployed with AWS SAM.

## Architecture

```mermaid
flowchart LR
    C[Customer] -->|POST /orders + Idempotency-Key| APIGW[API Gateway]
    APIGW --> PL[Lambda: place-order]
    PL -->|1. validate, product lookup via LRU| PT[(Products)]
    PL -->|2. atomic: order + idempotency record| OT[(Orders)]
    PL -->|3. enqueue order_id| Q[[SQS orders]]
    Q -->|batch of 10| W[Lambda: worker]
    W -->|earliest deadline first| W
    W -->|4. TransactWrite: stock decrement + CONFIRMED| IT[(Inventory)]
    W --> OT
    W -->|5. status event| SNS[[SNS order-events]]
    Q -.->|after 3 failed receives| DLQ[[SQS DLQ]]
    DLQ -->|message count > 0| AL[CloudWatch alarm] --> OPS[[SNS ops-alerts]]
    C -.->|GET /orders/{id}| APIGW
```

### Order lifecycle

```
POST /orders  -> PENDING  -> (worker) -> CONFIRMED  (stock reserved at one warehouse)
                                      -> REJECTED   (UNKNOWN_SKU or OUT_OF_STOCK everywhere)
```

### Tables

| Table | Key | Purpose |
| --- | --- | --- |
| Orders | `pk` = `ORDER#<id>` or `IDEM#<key>` | Order documents, plus idempotency records (`ttl` expires them after 24h) |
| Products | `sku` | Name and price, read through the LRU cache |
| Inventory | `sku` + `warehouse_id` | `qty` per warehouse |

## Data structures

| Where | Structure | Why |
| --- | --- | --- |
| `src/orders/ds/lru.py` | LRU cache: dict + doubly linked list | Product lookups in the API and worker skip DynamoDB for popular SKUs. get and put are O(1). |
| `src/orders/ds/deadline_heap.py` | Binary min-heap keyed by `ship_by` | The worker handles the most urgent order in each batch first. FIFO tie-break. |
| `src/orders/ds/graph.py` + `topology.py` | Weighted graph + Dijkstra | Finds the warehouse closest to the customer's region, then tries the next one if stock runs out. |

**LRU use case.** A few SKUs take most of the traffic (a Zipf distribution).
Keeping them in memory across warm Lambda invocations avoids most reads. The
`scripts/bench_cache.py` model below shows the effect. Misses are not cached,
so a product added later shows up on its next lookup.

**Dijkstra use case.** Warehouse selection. The graph's nodes are regions and
warehouses, and its edges are transit hours. The worker runs Dijkstra from the
customer's region and tries warehouses in order of distance.

## Correctness

**No overselling.** Stock is reserved with one `TransactWriteItems` call. It
holds one `UpdateItem` per line, each with `ConditionExpression: qty >= :requested`,
plus the order update `status = PENDING -> CONFIRMED`. Either everything
commits or nothing does. Two orders racing for the last unit: one commits, the
other is cancelled with `ConditionalCheckFailed` and falls through to the next warehouse.

**Idempotency.** Clients send an `Idempotency-Key` header. Each request body is hashed.
- Same key, same body: the original order is returned. No second order is created or reserved.
- Same key, different body: `422`.
- The order and the idempotency record are written in one transaction, so a crash can't leave one without the other.
- If the enqueue fails after the write, a retry re-sends the message while the order is still `PENDING`. This is safe because of the status condition below.

**Safe redelivery.** SQS is at-least-once. The reservation transaction also
requires `status = PENDING`, so a second delivery of the same message gets
`ALREADY_PROCESSED` and reserves nothing.

**Retries and DLQ.** The worker reports per-message failures
(`ReportBatchItemFailures`), so one bad message doesn't retry the whole batch.
After 3 receives the message moves to the DLQ, and the alarm fires.

**Notifications** are sent after the database commit, so they are at-least-once.
If the publish fails, the retry finds the order already processed and publishes
the stored status again. Subscribers should dedupe on `order_id` + `status`.

## Design trade-offs

- **Priority inside a batch only.** SQS standard queues don't guarantee order,
  so the heap orders the messages the worker has in hand (up to 10). Strict
  global priority would need one queue per deadline bucket, or a scheduler such
  as EventBridge Scheduler.
- **Single warehouse per order.** Splitting an order across warehouses adds
  partial-fulfilment states. This version picks one warehouse that can ship everything.
- **Cache staleness.** The LRU has no TTL. Prices changed in DynamoDB may be
  stale until eviction. Add a TTL if prices change often.
- **Reservation is pessimistic.** Stock is reserved at confirm time, not at
  checkout. A payment step would sit between PENDING and CONFIRMED in production.
- **Auth is out of scope.** The HTTP API is open. Add a JWT authorizer (Cognito)
  before production.
- **Single-table idempotency records** share the Orders table. This keeps one
  transaction across order and key, at the cost of mixed item shapes.
- **Throttling** is set to 20 requests/s with a burst of 50, a cost guard.

## Measurements

**Cache hit rate.** `python scripts/bench_cache.py` replays 100,000 lookups over
10,000 SKUs (Zipf s=1.07) through the real `LRUCache`. It counts how many would
reach DynamoDB. Latencies are assumed (4 ms per DynamoDB read, 0.01 ms per
memory hit), so these are model numbers, not AWS measurements:

| Cache size | Hit rate | DynamoDB reads | Avg lookup, ms (model) |
| ---: | ---: | ---: | ---: |
| 0 (none) | 0.0% | 100,000 | 4.000 |
| 64 | 43.2% | 56,812 | 2.277 |
| 256 | 59.6% | 40,431 | 1.623 |
| 1024 | 75.1% | 24,916 | 1.004 |
| 4096 | 88.8% | 11,249 | 0.459 |

**Live metrics.** Each function writes Embedded Metric Format logs, which
CloudWatch turns into the `OrderService` namespace:

- `OrdersReceived`, `DuplicateOrderRequests`
- `OrdersConfirmed`, `OrdersRejected`
- `EndToEndLatency` (ms, `created_at` to the final status)
- `CacheHit`, `CacheMiss` (cache hit rate = hits / (hits + misses))

Build a dashboard from these, and record numbers before and after the cache
change in your demo. The model above only shows the expected effect.

## Layout

```
template.yaml            SAM template: tables, queues, topics, alarm, functions, IAM
src/orders/
  handlers.py            Lambda entry points (thin)
  deps.py                builds clients and services once per container
  placement.py           validation, idempotency, enqueue
  worker.py              deadline-ordered batch processing, reservation, notification
  repository.py          DynamoDB transactions and conditional writes
  catalog.py             read-through LRU cache for products
  topology.py            shipping graph and warehouse ranking
  messaging.py           SQS and SNS wrappers
  metrics.py             EMF metrics
  ds/                    LRU cache, deadline heap, weighted graph (pure Python)
tests/                   45 tests; in-memory fakes for the AWS-facing parts
scripts/seed_catalog.py  loads demo products and stock
scripts/bench_cache.py   cache hit-rate model
```

## Run the tests

No AWS account needed.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install boto3 pytest pyflakes
pytest -q
```

## Deploy

Prerequisites: AWS CLI with credentials, AWS SAM CLI.

```bash
sam build
sam deploy --guided --stack-name order-service     # first time: pick region and optionally an alert email
```

Then seed demo data with the table names from the stack outputs:

```bash
python scripts/seed_catalog.py --products-table <ProductsTableName> --inventory-table <InventoryTableName>
```

Place an order and check it:

```bash
API=<ApiUrl output>
curl -s -X POST "$API/orders" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-0001' \
  -d '{"customer_id":"c-42","region":"north","lines":[{"sku":"MUG-001","quantity":2}]}'

curl -s "$API/orders/<order_id from above>"
```

Retrying the same `curl` returns the same `order_id`.

## Cost and cleanup

Expected cost is inside the free tier for light use: on-demand DynamoDB, SQS, SNS,
Lambda, and HTTP API. Set a billing alarm before you deploy. It needs
"Receive Billing Alerts" turned on in the Billing console first, and the
metric only exists in `us-east-1`:

```bash
aws cloudwatch put-metric-alarm --region us-east-1 \
  --alarm-name billing-over-10-usd --namespace AWS/Billing --metric-name EstimatedCharges \
  --dimensions Name=Currency,Value=USD --statistic Maximum --period 21600 \
  --evaluation-periods 1 --threshold 10 --comparison-operator GreaterThanThreshold \
  --alarm-actions <an SNS topic ARN you own>
```

When you're finished:

```bash
sam delete --stack-name order-service
```

This removes the tables, queues, topics, and functions. Delete the
`/aws/lambda/*` log groups separately, since CloudWatch keeps them after the stack is gone.

## Status

- Unit tests (45) pass locally. They cover the data structures, placement
  rules, worker behaviour (ordering, fallback, oversell, redelivery, retry), and
  the DynamoDB transaction error handling.
- The SAM template and the AWS wiring have not been deployed or validated against
  AWS from here. Run `sam validate` and a deploy to a test account before relying on them.
