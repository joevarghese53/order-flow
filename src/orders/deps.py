"""Composition root: builds clients and services once per warm Lambda container."""

from functools import lru_cache

import boto3

from orders import config
from orders.catalog import ProductCatalog
from orders.ds.lru import LRUCache
from orders.messaging import OrderNotifier, OrderQueue
from orders.placement import OrderPlacer
from orders.repository import DynamoOrderRepository, DynamoProductRepository
from orders.topology import WarehouseRanker, build_shipping_graph
from orders.worker import OrderWorker


@lru_cache(maxsize=None)
def _dynamodb():
    return boto3.client("dynamodb")


@lru_cache(maxsize=None)
def order_repo() -> DynamoOrderRepository:
    return DynamoOrderRepository(_dynamodb(), config.ORDERS_TABLE, config.INVENTORY_TABLE)


@lru_cache(maxsize=None)
def catalog() -> ProductCatalog:
    products = DynamoProductRepository(_dynamodb(), config.PRODUCTS_TABLE)
    return ProductCatalog(products.get, LRUCache(config.CATALOG_CACHE_SIZE))


@lru_cache(maxsize=None)
def order_placer() -> OrderPlacer:
    return OrderPlacer(order_repo(), catalog(), OrderQueue(boto3.client("sqs"), config.ORDERS_QUEUE_URL))


@lru_cache(maxsize=None)
def order_worker() -> OrderWorker:
    return OrderWorker(
        order_repo(),
        catalog(),
        OrderNotifier(boto3.client("sns"), config.ORDER_EVENTS_TOPIC_ARN),
        WarehouseRanker(build_shipping_graph()),
    )
