-- Dynamic N-day moving average (defaults to 90) + linear trend line over the moving average
USE WAREHOUSE IOWA_WH;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

WITH params AS (
    SELECT
        DATE '2023-01-01' AS visible_start,
        DATE '9999-12-31' AS visible_end,
        90 AS window_days
),

-- limit scan to what's needed for the moving window prior to visible_start
source AS (
    SELECT sale_date, sale_dollars
    FROM EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES
    WHERE sale_date >= DATEADD(day, -1 * ((SELECT window_days FROM params) - 1), (SELECT visible_start FROM params))
      AND sale_date <= (SELECT visible_end FROM params)
),

-- aggregate to daily totals (required for a correct daily moving average)
daily AS (
    SELECT
        sale_date,
        SUM(sale_dollars) AS sale_dollars
    FROM source
    GROUP BY sale_date
),

-- dynamic calendar-day moving average using a correlated subquery
ma AS (
    SELECT
        d.sale_date,
        d.sale_dollars,
        (
          SELECT AVG(d2.sale_dollars)
          FROM daily d2
          CROSS JOIN params p
          WHERE d2.sale_date BETWEEN DATEADD(day, -(p.window_days - 1), d.sale_date) AND d.sale_date
        ) AS moving_avg
    FROM daily d
),

xprep AS (
    SELECT
        sale_date,
        moving_avg,
        DATEDIFF(day, MIN(sale_date) OVER (), sale_date) AS x
    FROM ma
    WHERE moving_avg IS NOT NULL
      AND sale_date BETWEEN (SELECT visible_start FROM params) AND (SELECT visible_end FROM params)
),

reg AS (
    SELECT
        REGR_SLOPE(moving_avg, x)     AS slope,
        REGR_INTERCEPT(moving_avg, x) AS intercept
    FROM xprep
)

SELECT
    x.sale_date,
    x.moving_avg,
    reg.intercept + reg.slope * x.x AS trend_line
FROM xprep x
CROSS JOIN reg
ORDER BY x.sale_date;