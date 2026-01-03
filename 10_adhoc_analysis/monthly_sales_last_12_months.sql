-- Sales trend for the last 12 full months (aggregated by month)
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH bounds AS (
  SELECT
    DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE())) AS last_full_month_end,
    DATEADD(month, -12, DATE_TRUNC(month, CURRENT_DATE())) AS start_month
)
SELECT
  DATE_TRUNC(month, sale_date) AS month,
  SUM(sale_dollars) AS sales
FROM IOWA_LIQUOR_SALES
WHERE sale_date >= (SELECT start_month FROM bounds)
  AND sale_date <= (SELECT last_full_month_end FROM bounds)
GROUP BY month
ORDER BY month;
