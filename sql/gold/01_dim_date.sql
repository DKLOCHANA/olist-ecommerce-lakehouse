-- Calendar dimension covering every purchase date in the data.
WITH bounds AS (
  SELECT to_date(min(order_purchase_timestamp)) AS start_date,
         to_date(max(order_purchase_timestamp)) AS end_date
  FROM {{ silver.orders }}
),
days AS (
  SELECT explode(sequence(start_date, end_date, INTERVAL 1 DAY)) AS date
  FROM bounds
)
SELECT
  CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
  date,
  year(date)                                 AS year,
  quarter(date)                              AS quarter,
  month(date)                                AS month,
  date_format(date, 'MMMM')                  AS month_name,
  weekofyear(date)                           AS week_of_year,
  dayofmonth(date)                           AS day_of_month,
  date_format(date, 'EEEE')                  AS day_name,
  dayofweek(date) IN (1, 7)                  AS is_weekend
FROM days
