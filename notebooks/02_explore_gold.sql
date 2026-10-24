-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Explore the gold layer
-- MAGIC Business questions answered from the star schema built by the pipeline.

-- COMMAND ----------

-- Monthly revenue and month-over-month growth
WITH monthly AS (
  SELECT date_trunc('month', order_purchase_timestamp) AS month,
         count(DISTINCT order_id) AS orders,
         CAST(sum(item_total) AS DECIMAL(14,2)) AS revenue
  FROM workspace.gold.fact_order_items
  WHERE order_status NOT IN ('canceled', 'unavailable')
  GROUP BY 1
)
SELECT date_format(month, 'yyyy-MM') AS month, orders, revenue,
       round(100 * (revenue - lag(revenue) OVER (ORDER BY month)) / lag(revenue) OVER (ORDER BY month), 1) AS mom_growth_pct
FROM monthly ORDER BY month

-- COMMAND ----------

-- Late deliveries vs review score
SELECT CASE WHEN delay_days <= -7 THEN '1. 7+ days early'
            WHEN delay_days <= 0  THEN '2. on time'
            WHEN delay_days <= 7  THEN '3. up to 1 week late'
            ELSE '4. more than 1 week late' END AS delivery_bucket,
       count(DISTINCT order_id) AS orders,
       round(avg(review_score), 2) AS avg_review_score
FROM workspace.gold.fact_order_items
WHERE order_status = 'delivered' AND delay_days IS NOT NULL
GROUP BY 1 ORDER BY 1

-- COMMAND ----------

-- Revenue by customer state (map-ready)
SELECT customer_state, sum(gross_revenue) AS revenue, sum(orders) AS orders
FROM workspace.gold.mart_daily_sales
GROUP BY customer_state ORDER BY revenue DESC

-- COMMAND ----------

-- Latest data quality run
SELECT * FROM workspace.gold.dq_results
WHERE run_at = (SELECT max(run_at) FROM workspace.gold.dq_results)
ORDER BY passed, severity, table
