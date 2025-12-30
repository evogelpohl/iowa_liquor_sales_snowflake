-- Store location view with corrected coordinates for known outliers
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

CREATE OR REPLACE VIEW dim_store_location_v AS
WITH ranked AS (
  SELECT
    store_number,
    store_name,
    store_location,
    ROW_NUMBER() OVER (
      PARTITION BY store_number
      ORDER BY CASE WHEN store_location IS NOT NULL THEN 0 ELSE 1 END,
               sale_date DESC
    ) AS rn
  FROM EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES
)
SELECT
  store_number,
  store_name,
  CASE WHEN store_number IN (10505, 10129, 10663) THEN TRUE ELSE FALSE END AS correctedTF,
  CASE
    WHEN store_number = 10505 THEN OBJECT_CONSTRUCT('type','Point','coordinates', ARRAY_CONSTRUCT(-95.14875::double, 43.18232::double))
    WHEN store_number = 10129 THEN OBJECT_CONSTRUCT('type','Point','coordinates', ARRAY_CONSTRUCT(-91.6823925::double, 42.2899905::double))
    WHEN store_number = 10663 THEN OBJECT_CONSTRUCT('type','Point','coordinates', ARRAY_CONSTRUCT(-92.67254::double, 43.07529::double))
    ELSE store_location
  END AS store_location,
  CASE
    WHEN store_number = 10505 THEN -95.14875::double
    WHEN store_number = 10129 THEN -91.6823925::double
    WHEN store_number = 10663 THEN -92.67254::double
    ELSE store_location:"coordinates"[0]::double
  END AS longitude,
  CASE
    WHEN store_number = 10505 THEN 43.18232::double
    WHEN store_number = 10129 THEN 42.2899905::double
    WHEN store_number = 10663 THEN 43.07529::double
    ELSE store_location:"coordinates"[1]::double
  END AS latitude
FROM ranked
WHERE rn = 1
  AND store_location IS NOT NULL;
