"""Generate a small, deterministic sample of the Olist dataset for local runs and CI.

The files use exactly the same names and columns as the real Kaggle dataset, so
the pipeline runs unchanged on either. A few deliberately dirty rows are mixed in
(duplicates, bad codes, negative prices, unparseable numbers, out-of-range
scores) to exercise the silver cleaning and quarantine logic.

    python scripts/generate_sample_data.py --out data/raw --orders 2000
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path

STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE", "CE", "ES"]
CITIES = {
    "SP": ["sao paulo", "campinas", "santos"],
    "RJ": ["rio de janeiro", "niteroi"],
    "MG": ["belo horizonte", "uberlandia"],
    "RS": ["porto alegre", "caxias do sul"],
    "PR": ["curitiba", "londrina"],
    "SC": ["florianopolis", "joinville"],
    "BA": ["salvador"],
    "DF": ["brasilia"],
    "GO": ["goiania"],
    "PE": ["recife"],
    "CE": ["fortaleza"],
    "ES": ["vitoria"],
}
CATEGORIES = {
    "beleza_saude": "health_beauty",
    "informatica_acessorios": "computers_accessories",
    "moveis_decoracao": "furniture_decor",
    "esporte_lazer": "sports_leisure",
    "cama_mesa_banho": "bed_bath_table",
    "utilidades_domesticas": "housewares",
    "relogios_presentes": "watches_gifts",
    "telefonia": "telephony",
    "brinquedos": "toys",
    "automotivo": "auto",
}
PAYMENT_TYPES = ["credit_card"] * 7 + ["boleto"] * 2 + ["voucher", "debit_card"]
TS = "%Y-%m-%d %H:%M:%S"


def uid(*parts: object) -> str:
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()


def write(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)


def generate(out: Path, n_orders: int, seed: int) -> None:
    rnd = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)

    # sellers & products
    sellers = []
    for i in range(max(20, n_orders // 25)):
        state = rnd.choice(STATES)
        sellers.append(
            [uid("seller", i), f"{rnd.randint(1000, 99999):05d}", rnd.choice(CITIES[state]), state]
        )
    products = []
    for i in range(max(50, n_orders // 7)):
        products.append(
            [
                uid("product", i),
                rnd.choice(list(CATEGORIES)),
                rnd.randint(20, 60),
                rnd.randint(100, 2000),
                rnd.randint(1, 6),
                rnd.randint(100, 20000),
                rnd.randint(10, 80),
                rnd.randint(2, 60),
                rnd.randint(10, 60),
            ]
        )
    products.append(["", "brinquedos", 30, 300, 1, 500, 20, 10, 15])  # dirty: missing key

    customers, orders, items, payments, reviews = [], [], [], [], []
    start = datetime(2017, 1, 1)
    for i in range(n_orders):
        state = rnd.choice(STATES)
        cust_id = uid("customer", i)
        customers.append(
            [
                cust_id,
                uid("person", rnd.randint(0, int(n_orders * 0.9))),
                f"{rnd.randint(1000, 99999):05d}",
                rnd.choice(CITIES[state]),
                state,
            ]
        )

        purchased = start + timedelta(minutes=rnd.randint(0, 60 * 24 * 600))
        status = rnd.choices(["delivered", "shipped", "canceled", "invoiced"], [92, 4, 2, 2])[0]
        approved = purchased + timedelta(hours=rnd.randint(0, 48))
        carrier = approved + timedelta(days=rnd.randint(1, 5))
        estimated = purchased + timedelta(days=rnd.randint(15, 35))
        delivered = carrier + timedelta(days=rnd.randint(2, 30)) if status == "delivered" else None
        order_id = uid("order", i)
        orders.append(
            [
                order_id,
                cust_id,
                status,
                purchased.strftime(TS),
                approved.strftime(TS),
                carrier.strftime(TS) if status in {"delivered", "shipped"} else "",
                delivered.strftime(TS) if delivered else "",
                estimated.strftime("%Y-%m-%d 00:00:00"),
            ]
        )

        total = 0.0
        for n in range(1, rnd.choices([1, 2, 3], [85, 12, 3])[0] + 1):
            product = rnd.choice(products[:-1])
            price = round(rnd.uniform(10, 500), 2)
            freight = round(rnd.uniform(5, 60), 2)
            total += price + freight
            items.append(
                [
                    order_id,
                    n,
                    product[0],
                    rnd.choice(sellers)[0],
                    (purchased + timedelta(days=3)).strftime(TS),
                    f"{price:.2f}",
                    f"{freight:.2f}",
                ]
            )

        if rnd.random() < 0.05:  # split payment: voucher + card
            voucher = round(min(total, rnd.uniform(5, 50)), 2)
            payments.append([order_id, 1, "voucher", 1, f"{voucher:.2f}"])
            payments.append([order_id, 2, "credit_card", rnd.randint(1, 10), f"{total - voucher:.2f}"])
        else:
            ptype = rnd.choice(PAYMENT_TYPES)
            payments.append(
                [order_id, 1, ptype, rnd.randint(1, 10) if ptype == "credit_card" else 1, f"{total:.2f}"]
            )

        if status == "delivered" and rnd.random() < 0.9:
            score = rnd.choices([1, 2, 3, 4, 5], [10, 3, 8, 20, 59])[0]
            comment = rnd.choice(
                ["", "Produto chegou rapido, recomendo!", "Entrega atrasou.\nMas o produto e bom.", ""]
            )
            reviews.append(
                [
                    uid("review", i),
                    order_id,
                    score,
                    "",
                    comment,
                    (delivered + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"),
                    (delivered + timedelta(days=2)).strftime(TS),
                ]
            )

    # ---- deliberately dirty rows -------------------------------------------------
    customers.extend(customers[:5])  # exact duplicates -> deduplicated
    customers[10][4] = "  sp "  # messy code -> standardised to SP
    customers[11][4] = "XX"  # invalid state -> quarantined
    customers[12][4] = "XX"
    orders[20][2] = " Delivered "  # messy status -> standardised
    orders[21][2] = "lost"  # unknown status -> quarantined
    orders[22][2] = "lost"
    items[30][5] = "-15.00"  # negative price -> quarantined
    items[31][5] = "abc"  # unparseable -> quarantined
    if reviews:
        reviews[5][2] = 7  # score out of range -> quarantined

    write(
        out / "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        customers,
    )
    write(
        out / "olist_orders_dataset.csv",
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        orders,
    )
    write(
        out / "olist_order_items_dataset.csv",
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        items,
    )
    write(
        out / "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
        payments,
    )
    write(
        out / "olist_order_reviews_dataset.csv",
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        reviews,
    )
    write(
        out / "olist_products_dataset.csv",
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        products,
    )
    write(
        out / "olist_sellers_dataset.csv",
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        sellers,
    )
    write(
        out / "product_category_name_translation.csv",
        ["product_category_name", "product_category_name_english"],
        [[k, v] for k, v in CATEGORIES.items()],
    )
    print(f"Wrote sample Olist data ({n_orders} orders) to {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw")
    p.add_argument("--orders", type=int, default=2000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    generate(Path(a.out), a.orders, a.seed)
