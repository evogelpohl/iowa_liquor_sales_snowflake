USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

WITH max_month AS (
  SELECT
      DATE_TRUNC('MONTH', MAX(SALE_DATE)) AS max_sale_month
  FROM IOWA_LIQUOR_SALES
),

ttm_sales AS (
  SELECT
      INITCAP(liquor_category) AS liquor_category,
      SUM(SALE_DOLLARS)        AS ttm_sales
  FROM IOWA_LIQUOR_SALES
  CROSS JOIN max_month
  WHERE SALE_DATE >= DATEADD(
          MONTH, -11, max_month.max_sale_month
        )
    AND SALE_DATE < DATEADD(
          MONTH,  1, max_month.max_sale_month
        )
  GROUP BY 1
)

SELECT
    liquor_category        AS "Liquor Category",
    round(ttm_sales,0)              AS "TTM Sales"
FROM ttm_sales
ORDER BY "TTM Sales" DESC;