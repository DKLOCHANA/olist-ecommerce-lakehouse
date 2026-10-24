-- Monthly revenue with month-over-month growth (window function).
WITH monthly AS (
  SELECT date_trunc('month', order_purchase_timestamp) AS month,
         count(DISTINCT order_id)                       AS orders,
         CAST(sum(item_total) AS DECIMAL(14,2))         AS revenue
  FROM {{ gold.fact_order_items }}
  WHERE order_status NOT IN ('canceled', 'unavailable')
  GROUP BY 1
)
SELECT
  date_format(month, 'yyyy-MM')                                         AS month,
  orders,
  revenue,
  round(100 * (revenue - lag(revenue) OVER (ORDER BY month))
        / lag(revenue) OVER (ORDER BY month), 1)                        AS mom_growth_pct,
  round(revenue / orders, 2)                                            AS avg_order_value
FROM monthly
ORDER BY month
