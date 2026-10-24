-- RFM segmentation (Recency, Frequency, Monetary) on real customers (customer_unique_id).
WITH base AS (
  SELECT c.customer_unique_id,
         max(f.order_purchase_timestamp)        AS last_order,
         count(DISTINCT f.order_id)             AS frequency,
         sum(f.item_total)                      AS monetary
  FROM {{ gold.fact_order_items }} f
  JOIN {{ gold.dim_customer }} c ON f.customer_key = c.customer_key
  WHERE f.order_status = 'delivered' AND c.customer_key <> -1
  GROUP BY c.customer_unique_id
),
scored AS (
  SELECT *,
         ntile(4) OVER (ORDER BY last_order)  AS r,
         ntile(4) OVER (ORDER BY frequency)   AS f,
         ntile(4) OVER (ORDER BY monetary)    AS m
  FROM base
)
SELECT
  CASE WHEN r = 4 AND f >= 3 AND m >= 3 THEN 'Champions'
       WHEN r >= 3 AND m >= 3            THEN 'Loyal / high value'
       WHEN r >= 3                       THEN 'Recent, low spend'
       WHEN m >= 3                       THEN 'At risk (high value)'
       ELSE                                   'Hibernating' END AS segment,
  count(*)                       AS customers,
  round(avg(monetary), 2)        AS avg_spend
FROM scored
GROUP BY 1
ORDER BY customers DESC
