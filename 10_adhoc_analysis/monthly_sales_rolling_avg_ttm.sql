USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH params AS (
  SELECT
    -- visible range you want to chart (example: last 36 months ending current month)
    DATEADD(MONTH, -36, DATE_TRUNC('MONTH', CURRENT_DATE())) AS visible_start,
    DATE_TRUNC('MONTH', CURRENT_DATE())                      AS visible_end
),

bounds AS (
  SELECT
    DATEADD(MONTH, -11, (SELECT visible_start FROM params)) AS scan_start,
    (SELECT visible_end FROM params)                        AS scan_end
),

monthly AS (
  SELECT
      DATE_TRUNC('MONTH', SALE_DATE) AS sale_month,
      SUM(SALE_DOLLARS)              AS monthly_sales
  FROM IOWA_LIQUOR_SALES
  WHERE SALE_DATE >= (SELECT scan_start FROM bounds)
    AND SALE_DATE <  DATEADD(MONTH, 1, (SELECT scan_end FROM bounds))  -- include full scan_end month
  GROUP BY 1
),

calc AS (
  SELECT
      sale_month,
      monthly_sales,
      AVG(monthly_sales) OVER (
        ORDER BY sale_month
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
      ) AS ttm_avg_sales
  FROM monthly
)

SELECT
    sale_month AS sale_date,
    ROUND(monthly_sales, 0)  AS monthly_sales,
    ROUND(ttm_avg_sales, 0)  AS ttm_avg_sales
FROM calc
WHERE sale_month BETWEEN (SELECT visible_start FROM params)
                     AND (SELECT visible_end FROM params)
ORDER BY sale_month;