SELECT
  xxhash64(seller_id)      AS seller_key,
  seller_id,
  seller_zip_code_prefix   AS zip_code_prefix,
  seller_city              AS city,
  seller_state             AS state
FROM {{ silver.sellers }}
UNION ALL
SELECT -1, 'unknown', NULL, 'Unknown', 'NA'
