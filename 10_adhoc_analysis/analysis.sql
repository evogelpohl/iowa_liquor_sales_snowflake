-- N-day moving average and trend line (default 90-day) for sale_dollars
-- Adjust visible_start/visible_end/window_days as needed.
WITH params AS (
    SELECT
        DATE '2022-01-01' AS visible_start,
        DATE '9999-12-31' AS visible_end,
        90 AS window_days  -- rolling window in data rows
),
source AS (
    SELECT sale_date, sale_dollars
    FROM EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES
    WHERE sale_date >= DATEADD(day, -1 * ((SELECT window_days FROM params) - 1), (SELECT visible_start FROM params))
      AND sale_date <= (SELECT visible_end FROM params)
),
ma AS (
    SELECT
        sale_date,
        SUM(sale_dollars) AS sale_dollars,
        AVG(SUM(sale_dollars)) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW   -- window_days - 1
        ) AS moving_avg
    FROM source
    GROUP BY sale_date
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
