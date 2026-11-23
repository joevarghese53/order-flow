"""Load demo products and per-warehouse stock into DynamoDB.

Usage:
  python scripts/seed_catalog.py --products-table <name> --inventory-table <name>
The table names are in the SAM stack outputs (ProductsTableName, InventoryTableName).
"""

import argparse

import boto3

from orders.topology import WAREHOUSES

DEMO_PRODUCTS = [
    {"sku": "MUG-001", "name": "Ceramic mug", "price_cents": 1200},
    {"sku": "LAMP-002", "name": "Desk lamp", "price_cents": 4500},
    {"sku": "BAG-003", "name": "Canvas tote", "price_cents": 2200},
]
STOCK_PER_WAREHOUSE = 25


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--products-table", required=True)
    parser.add_argument("--inventory-table", required=True)
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb")
    products = dynamodb.Table(args.products_table)
    inventory = dynamodb.Table(args.inventory_table)

    for product in DEMO_PRODUCTS:
        products.put_item(Item=product)
        for warehouse in WAREHOUSES:
            inventory.put_item(Item={"sku": product["sku"], "warehouse_id": warehouse, "qty": STOCK_PER_WAREHOUSE})
        print(f"seeded {product['sku']} at {len(WAREHOUSES)} warehouses")


if __name__ == "__main__":
    main()
