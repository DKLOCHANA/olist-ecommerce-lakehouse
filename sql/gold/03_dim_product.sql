-- Product dimension with English category names and derived volume.
SELECT
  xxhash64(p.product_id)                                            AS product_key,
  p.product_id,
  coalesce(t.product_category_name_english,
           p.product_category_name, 'unknown')                      AS category,
  p.product_photos_qty                                              AS photos_qty,
  p.product_weight_g                                                AS weight_g,
  p.product_length_cm * p.product_height_cm * p.product_width_cm    AS volume_cm3
FROM {{ silver.products }} p
LEFT JOIN {{ silver.category_translation }} t
  ON p.product_category_name = t.product_category_name
UNION ALL
SELECT -1, 'unknown', 'unknown', NULL, NULL, NULL
