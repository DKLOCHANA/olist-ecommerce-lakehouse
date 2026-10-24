-- Business-ready aggregate for dashboards (Power BI / Databricks SQL).
SELECT
  d.date,
  d.year,
  d.month,
  c.state                                   AS customer_state,
  p.category,
  count(DISTINCT f.order_id)                AS orders,
  count(*)                                  AS items_sold,
  CAST(sum(f.price) AS DECIMAL(14,2))       AS product_revenue,
  CAST(sum(f.freight_value) AS DECIMAL(14,2)) AS freight_revenue,
  CAST(sum(f.item_total) AS DECIMAL(14,2))  AS gross_revenue,
  round(avg(f.review_score), 2)             AS avg_review_score
FROM {{ gold.fact_order_items }} f
JOIN {{ gold.dim_date }} d      ON f.purchase_date_key = d.date_key
JOIN {{ gold.dim_customer }} c  ON f.customer_key = c.customer_key
JOIN {{ gold.dim_product }} p   ON f.product_key = p.product_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.date, d.year, d.month, c.state, p.category
