from collections.abc import Callable

from orders.ds.lru import LRUCache
from orders.metrics import emit


class ProductCatalog:
    """Read-through LRU cache in front of the Products table.

    Popular SKUs stay in memory across warm Lambda invocations, so most
    lookups skip DynamoDB. Misses are not cached, so a product created later
    is picked up on its next lookup.
    """

    def __init__(self, loader: Callable[[str], dict | None], cache: LRUCache[str, dict]) -> None:
        self._loader = loader
        self._cache = cache

    def get(self, sku: str) -> dict | None:
        product = self._cache.get(sku)
        if product is not None:
            emit("CacheHit", 1)
            return product
        emit("CacheMiss", 1)
        product = self._loader(sku)
        if product is not None:
            self._cache.put(sku, product)
        return product
