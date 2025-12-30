-- Manual test for WEEKLY_IOWA_LOAD
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

-- Optional: capture baseline counts
SELECT COUNT(*) AS rows_before FROM IOWA_LIQUOR_SALES WHERE sale_date BETWEEN '2025-11-01' AND '2025-11-30';

-- Delete the test month
DELETE FROM IOWA_LIQUOR_SALES WHERE sale_date BETWEEN '2025-11-01' AND '2025-11-30';

-- Confirm deletion
SELECT COUNT(*) AS rows_after_delete FROM IOWA_LIQUOR_SALES WHERE sale_date BETWEEN '2025-11-01' AND '2025-11-30';

-- Run the weekly task once
EXECUTE TASK WEEKLY_IOWA_LOAD;

-- Wait for completion, then recheck
SELECT COUNT(*) AS rows_after_reload FROM IOWA_LIQUOR_SALES WHERE sale_date BETWEEN '2025-11-01' AND '2025-11-30';

-- Inspect a few rows
SELECT * FROM IOWA_LIQUOR_SALES
WHERE sale_date BETWEEN '2025-11-01' AND '2025-11-05'
ORDER BY sale_date, invoice_and_item_number
LIMIT 20;
