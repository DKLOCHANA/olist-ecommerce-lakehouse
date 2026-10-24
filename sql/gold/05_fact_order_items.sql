-- Grain: one row per item sold. Foreign keys resolve to the unknown member (-1)
-- when the dimension row is missing (e.g. it was quarantined in silver).
WITH reviews AS (
  SELECT order_id, avg(review_score) AS review_score
  FROM {{ silver.order_reviews }}
  GROUP BY order_id
)
SELECT
  xxhash64(i.order_id, i.order_item_id)                         AS order_item_key,
  i.order_id,
  i.order_item_id,
  CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS purchase_date_key,
  coalesce(c.customer_key, -1)                                  AS customer_key,
  coalesce(p.product_key, -1)                                   AS product_key,
  coalesce(s.seller_key, -1)                                    AS seller_key,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_delivered_customer_date,
  i.price,
  i.freight_value,
  i.price + coalesce(i.freight_value, 0)                        AS item_total,
  datediff(o.order_delivered_customer_date, o.order_purchase_timestamp)       AS delivery_days,
  datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date)  AS delay_days,
  CASE WHEN o.order_delivered_customer_date IS NULL THEN NULL
       ELSE o.order_delivered_customer_date <= o.order_estimated_delivery_date
  END                                                           AS delivered_on_time,
  r.review_score
FROM {{ silver.order_items }} i
JOIN {{ silver.orders }} o            ON i.order_id = o.order_id
LEFT JOIN {{ gold.dim_customer }} c   ON o.customer_id = c.customer_id
LEFT JOIN {{ gold.dim_product }} p    ON i.product_id = p.product_id
LEFT JOIN {{ gold.dim_seller }} s     ON i.seller_id = s.seller_id
LEFT JOIN reviews r                   ON i.order_id = r.order_id
