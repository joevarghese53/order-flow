"""Lambda entry points. Keep these thin: parse input, call a service, shape the response."""

import base64
import json

from orders import deps
from orders.placement import ValidationError


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _json_body(event: dict) -> object:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as err:
        raise ValidationError(f"body is not valid JSON: {err.msg}") from err


def place_order(event, context):
    headers = {name.lower(): value for name, value in (event.get("headers") or {}).items()}
    try:
        body = _json_body(event)
        result = deps.order_placer().place(body, headers.get("idempotency-key", ""))
    except ValidationError as err:
        return _response(400, {"error": str(err)})
    return _response(result.status_code, result.body)


def get_order(event, context):
    order_id = (event.get("pathParameters") or {}).get("order_id")
    order = deps.order_repo().get_order(order_id) if order_id else None
    if order is None:
        return _response(404, {"error": "order not found"})
    return _response(200, order.to_dict())


def process_orders(event, context):
    return deps.order_worker().handle_batch(event["Records"])
