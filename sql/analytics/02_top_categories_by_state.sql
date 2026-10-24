-- Top 3 product categories by revenue in each of the 5 biggest states.
WITH by_state AS (
  SELECT customer_state, category, sum(gross_revenue) AS revenue
  FROM {{ gold.mart_daily_sales }}
  GROUP BY customer_state, category
),
top_states AS (
  SELECT customer_state FROM by_state
  GROUP BY customer_state ORDER BY sum(revenue) DESC LIMIT 5
),
ranked AS (
  SELECT b.*, dense_rank() OVER (PARTITION BY b.customer_state ORDER BY revenue DESC) AS rnk
  FROM by_state b JOIN top_states t USING (customer_state)
)
SELECT customer_state, rnk, category, revenue
FROM ranked
WHERE rnk <= 3
ORDER BY customer_state, rnk
