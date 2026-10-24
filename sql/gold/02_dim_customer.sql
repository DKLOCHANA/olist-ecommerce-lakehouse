-- Customer dimension. customer_id is per order in Olist; customer_unique_id is the real person.
-- Row -1 is the "unknown member" so facts never point at a missing customer.
SELECT
  xxhash64(customer_id)      AS customer_key,
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix   AS zip_code_prefix,
  customer_city              AS city,
  customer_state             AS state
FROM {{ silver.customers }}
UNION ALL
SELECT -1, 'unknown', 'unknown', NULL, 'Unknown', 'NA'
