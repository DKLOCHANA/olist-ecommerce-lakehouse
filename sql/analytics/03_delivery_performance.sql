-- Does late delivery hurt review scores?
SELECT
  CASE WHEN delay_days <= -7 THEN '1. 7+ days early'
       WHEN delay_days <= 0  THEN '2. on time'
       WHEN delay_days <= 7  THEN '3. up to 1 week late'
       ELSE                       '4. more than 1 week late' END  AS delivery_bucket,
  count(DISTINCT order_id)                                        AS orders,
  round(avg(delivery_days), 1)                                    AS avg_delivery_days,
  round(avg(review_score), 2)                                     AS avg_review_score
FROM {{ gold.fact_order_items }}
WHERE order_status = 'delivered' AND delay_days IS NOT NULL
GROUP BY 1
ORDER BY 1
