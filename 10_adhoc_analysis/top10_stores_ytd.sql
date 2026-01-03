-- Top 10 stores by sales year-to-date
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH params AS (
  SELECT DATE_TRUNC(year, CURRENT_DATE()) AS start_of_year
)
SELECT
  store_number,
  store_name,
  city,
  county,
  SUM(sale_dollars) AS sales
FROM IOWA_LIQUOR_SALES
WHERE sale_date >= (SELECT start_of_year FROM params)
GROUP BY store_number, store_name, city, county
ORDER BY sales DESC
LIMIT 10;
