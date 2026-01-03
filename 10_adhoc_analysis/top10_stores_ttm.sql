-- Top 10 stores by sales trailing twelve months
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH bounds AS (
  SELECT
    DATEADD(day, -1, DATE_TRUNC(month, CURRENT_DATE())) AS last_full_month_end,
    DATEADD(month, -12, DATE_TRUNC(month, CURRENT_DATE())) AS start_month
)
SELECT
  store_number,
  store_name,
  city,
  county,
  SUM(sale_dollars) AS sales
FROM IOWA_LIQUOR_SALES
WHERE sale_date >= (SELECT start_month FROM bounds)
  AND sale_date <= (SELECT last_full_month_end FROM bounds)
GROUP BY store_number, store_name, city, county
ORDER BY sales DESC
LIMIT 10;
