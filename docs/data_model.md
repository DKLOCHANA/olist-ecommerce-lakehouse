# Gold data model

Kimball-style star schema. Surrogate keys are `xxhash64` of the natural key, so they are
deterministic across reloads. Every dimension has an **unknown member** (`key = -1`) so a
fact row is never dropped when its dimension row is missing or was quarantined.

```mermaid
erDiagram
    dim_date ||--o{ fact_order_items : "purchase_date_key"
    dim_customer ||--o{ fact_order_items : customer_key
    dim_product ||--o{ fact_order_items : product_key
    dim_seller ||--o{ fact_order_items : seller_key
    dim_customer ||--o{ fact_payments : customer_key
    dim_date ||--o{ fact_payments : "purchase_date_key"

    fact_order_items {
        bigint order_item_key PK
        string order_id
        int order_item_id
        int purchase_date_key FK
        bigint customer_key FK
        bigint product_key FK
        bigint seller_key FK
        string order_status
        decimal price
        decimal freight_value
        decimal item_total
        int delivery_days
        int delay_days
        boolean delivered_on_time
        double review_score
    }
    fact_payments {
        bigint payment_key PK
        string order_id
        int payment_sequential
        int purchase_date_key FK
        bigint customer_key FK
        string payment_type
        int payment_installments
        decimal payment_value
    }
    dim_customer {
        bigint customer_key PK
        string customer_id
        string customer_unique_id
        string city
        string state
    }
    dim_product {
        bigint product_key PK
        string product_id
        string category
        int weight_g
        int volume_cm3
    }
    dim_seller {
        bigint seller_key PK
        string seller_id
        string city
        string state
    }
    dim_date {
        int date_key PK
        date date
        int year
        int quarter
        int month
        boolean is_weekend
    }
```

## Grain

| Table | One row per |
|---|---|
| `fact_order_items` | item sold (order_id + order_item_id) |
| `fact_payments` | payment transaction (order_id + payment_sequential) |
| `mart_daily_sales` | date × customer state × product category (excludes canceled/unavailable orders) |

## Notes on the source data

- In Olist, `customer_id` is generated **per order**; `customer_unique_id` identifies the
  real person. Customer analytics (RFM) therefore groups by `customer_unique_id`.
- An order can have several payments (for example a voucher plus a credit card), which is
  why payments are a separate fact rather than a column on the order.
- Product category names are Portuguese in the source and translated with
  `product_category_name_translation.csv`.
- The source misspells two columns (`product_name_lenght`, `product_description_lenght`);
  bronze keeps the original names on purpose.
