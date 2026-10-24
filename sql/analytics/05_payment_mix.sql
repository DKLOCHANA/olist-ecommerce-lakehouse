-- Payment method mix and average instalments.
SELECT
  payment_type,
  count(DISTINCT order_id)                                    AS orders,
  sum(payment_value)                                          AS total_paid,
  round(100 * sum(payment_value) / sum(sum(payment_value)) OVER (), 1) AS share_pct,
  round(avg(payment_installments), 1)                         AS avg_installments
FROM {{ gold.fact_payments }}
GROUP BY payment_type
ORDER BY total_paid DESC
