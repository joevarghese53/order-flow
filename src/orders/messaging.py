import json

from orders.models import Order


class OrderQueue:
    def __init__(self, sqs_client, queue_url: str) -> None:
        self._sqs = sqs_client
        self._queue_url = queue_url

    def send(self, order_id: str) -> None:
        # Only the id travels in the message. The worker reads the order from
        # DynamoDB, which stays the single source of truth.
        self._sqs.send_message(QueueUrl=self._queue_url, MessageBody=json.dumps({"order_id": order_id}))


class OrderNotifier:
    def __init__(self, sns_client, topic_arn: str) -> None:
        self._sns = sns_client
        self._topic_arn = topic_arn

    def publish(self, order: Order) -> None:
        # Subscribers can filter on the `status` message attribute.
        self._sns.publish(
            TopicArn=self._topic_arn,
            Message=json.dumps(
                {
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "status": order.status.value,
                    "warehouse_id": order.warehouse_id,
                    "reason": order.reason,
                }
            ),
            MessageAttributes={"status": {"DataType": "String", "StringValue": order.status.value}},
        )
