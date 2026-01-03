-- Current-year sales by liquor category (descending)
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH params AS (
  SELECT YEAR(CURRENT_DATE()) AS yr
)
SELECT
  INITCAP(liquor_category) AS liquor_category,
  SUM(sale_dollars) AS sales
FROM IOWA_LIQUOR_SALES
WHERE sale_year = (SELECT yr FROM params)
GROUP BY liquor_category
ORDER BY sales DESC;
