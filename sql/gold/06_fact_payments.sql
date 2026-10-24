-- Grain: one row per payment transaction on an order.
SELECT
  xxhash64(p.order_id, p.payment_sequential)                       AS payment_key,
  p.order_id,
  p.payment_sequential,
  CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS purchase_date_key,
  coalesce(c.customer_key, -1)                                     AS customer_key,
  p.payment_type,
  p.payment_installments,
  p.payment_value
FROM {{ silver.order_payments }} p
JOIN {{ silver.orders }} o           ON p.order_id = o.order_id
LEFT JOIN {{ gold.dim_customer }} c  ON o.customer_id = c.customer_id
