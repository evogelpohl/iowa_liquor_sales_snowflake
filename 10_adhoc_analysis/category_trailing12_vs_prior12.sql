-- Trailing 12 vs prior 12 months over time (one row per month, no category split)
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

/* TTM Sales + TTM % Change (YoY) by YYYYMM */

WITH monthly AS (
  SELECT
      DATE_TRUNC('MONTH', SALE_DATE)                    AS month_start,
      TO_CHAR(DATE_TRUNC('MONTH', SALE_DATE), 'YYYYMM') AS yyyymm,
      SUM(SALE_DOLLARS)                                 AS monthly_sales
  FROM IOWA_LIQUOR_SALES
  WHERE SALE_DATE IS NOT NULL
  GROUP BY 1, 2
),

ttm AS (
  SELECT
      month_start,
      yyyymm,
      SUM(monthly_sales) OVER (
        ORDER BY month_start
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
      ) AS ttm_sales
  FROM monthly
)

SELECT
    yyyymm,
    ttm_sales,
    /* YoY % change of the TTM metric */
    (ttm_sales / NULLIF(LAG(ttm_sales, 12) OVER (ORDER BY month_start), 0)) - 1
      AS ttm_pct_change
FROM ttm
WHERE month_start >= DATEADD(MONTH, -36, DATE_TRUNC('MONTH', CURRENT_DATE()))
ORDER BY yyyymm;