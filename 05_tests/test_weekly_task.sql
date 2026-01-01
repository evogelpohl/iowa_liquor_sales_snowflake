-- Manual test for WEEKLY_IOWA_LOAD (replay last full month)
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

SET last_full_month_start = (
  SELECT DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE()))
);
SET last_full_month_end = (
  SELECT DATEADD(day, -1, DATEADD(month, 1, $last_full_month_start))
);

-- Baseline count for last full month
SELECT COUNT(*) AS rows_before
FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN $last_full_month_start AND $last_full_month_end;

-- Delete the last full month to force a reload
DELETE FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN $last_full_month_start AND $last_full_month_end;

-- Confirm deletion
SELECT COUNT(*) AS rows_after_delete
FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN $last_full_month_start AND $last_full_month_end;

-- Run the weekly task once
EXECUTE TASK WEEKLY_IOWA_LOAD;

-- Recheck after the task run
SELECT COUNT(*) AS rows_after_reload
FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN $last_full_month_start AND $last_full_month_end;

-- Inspect a few rows
SELECT * FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN $last_full_month_start AND DATEADD(day, 4, $last_full_month_start)
ORDER BY sale_date, invoice_and_item_number
LIMIT 20;
