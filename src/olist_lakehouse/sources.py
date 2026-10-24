"""Source file contracts for the Olist Brazilian e-commerce dataset.

Bronze keeps every column as a string (schema-on-read, nothing is lost).
Silver applies the typed schema declared here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    name: str  # bronze/silver table name
    file: str  # CSV file name in the raw landing zone
    columns: dict[str, str]  # column -> Spark SQL type used in silver
    keys: tuple[str, ...]  # business key (used for dedup + uniqueness checks)


SOURCES: dict[str, Source] = {
    s.name: s
    for s in [
        Source(
            name="customers",
            file="olist_customers_dataset.csv",
            columns={
                "customer_id": "string",
                "customer_unique_id": "string",
                "customer_zip_code_prefix": "string",
                "customer_city": "string",
                "customer_state": "string",
            },
            keys=("customer_id",),
        ),
        Source(
            name="orders",
            file="olist_orders_dataset.csv",
            columns={
                "order_id": "string",
                "customer_id": "string",
                "order_status": "string",
                "order_purchase_timestamp": "timestamp",
                "order_approved_at": "timestamp",
                "order_delivered_carrier_date": "timestamp",
                "order_delivered_customer_date": "timestamp",
                "order_estimated_delivery_date": "timestamp",
            },
            keys=("order_id",),
        ),
        Source(
            name="order_items",
            file="olist_order_items_dataset.csv",
            columns={
                "order_id": "string",
                "order_item_id": "int",
                "product_id": "string",
                "seller_id": "string",
                "shipping_limit_date": "timestamp",
                "price": "decimal(10,2)",
                "freight_value": "decimal(10,2)",
            },
            keys=("order_id", "order_item_id"),
        ),
        Source(
            name="order_payments",
            file="olist_order_payments_dataset.csv",
            columns={
                "order_id": "string",
                "payment_sequential": "int",
                "payment_type": "string",
                "payment_installments": "int",
                "payment_value": "decimal(10,2)",
            },
            keys=("order_id", "payment_sequential"),
        ),
        Source(
            name="order_reviews",
            file="olist_order_reviews_dataset.csv",
            columns={
                "review_id": "string",
                "order_id": "string",
                "review_score": "int",
                "review_comment_title": "string",
                "review_comment_message": "string",
                "review_creation_date": "timestamp",
                "review_answer_timestamp": "timestamp",
            },
            keys=("review_id", "order_id"),
        ),
        Source(
            name="products",
            file="olist_products_dataset.csv",
            columns={
                "product_id": "string",
                "product_category_name": "string",
                # the original dataset misspells "length" - keep source names in bronze
                "product_name_lenght": "int",
                "product_description_lenght": "int",
                "product_photos_qty": "int",
                "product_weight_g": "int",
                "product_length_cm": "int",
                "product_height_cm": "int",
                "product_width_cm": "int",
            },
            keys=("product_id",),
        ),
        Source(
            name="sellers",
            file="olist_sellers_dataset.csv",
            columns={
                "seller_id": "string",
                "seller_zip_code_prefix": "string",
                "seller_city": "string",
                "seller_state": "string",
            },
            keys=("seller_id",),
        ),
        Source(
            name="category_translation",
            file="product_category_name_translation.csv",
            columns={
                "product_category_name": "string",
                "product_category_name_english": "string",
            },
            keys=("product_category_name",),
        ),
    ]
}

ORDER_STATUSES = (
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "canceled",
    "unavailable",
)
PAYMENT_TYPES = ("credit_card", "boleto", "voucher", "debit_card", "not_defined")
BRAZIL_STATES = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]
