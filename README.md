## Iowa Liquor Sales on Snowflake — Quickstart

This repo is ordered so you can build the pipeline from scratch and rerun pieces cleanly.

### Execution Order
Use Snow CLI (`snow sql -f ...`) with your profile.  
1) `01_env/warehouse_schema.sql` – create/ensure DB, schema, warehouse, stage, file format, raw + silver tables.  
2) `01_env/network_access.sql` – network rule + external access integration for the Socrata API.  
3) `01_env/02_file_format_iowa_json.sql` – canonical JSON file format in the project schema.  
4) `03_procs/` – create all stored procedures (`SP_LOAD_IOWA`, `SP_FETCH_IOWA_TO_STAGE`, `SP_LOAD_IOWA_FROM_STAGE`, `SP_LOAD_IOWA_LATEST`).  
5) `04_tasks/task_weekly_load.sql` – create/enable the weekly Task that calls `SP_LOAD_IOWA_LATEST`.  
6) (Optional) `06_streamlit/streamlit_setup.sql` – compute pool/warehouse/app DB for Streamlit.  
7) (Optional) `05_tests/test_stage_load.sql` and `05_tests/test_weekly_task.sql` – manual checks.  
8) Analysis/views: `02_objects/views.sql`, `02_objects/analysis.sql`.

### Stored procedure pattern (stage-first)
- `SP_FETCH_IOWA_TO_STAGE(years ARRAY, months ARRAY)`: pulls from API to `RAW_STAGE` (JSONL). Months use `'YYYY-MM'`; if both args null, defaults to last full year/month.  
- `SP_LOAD_IOWA_FROM_STAGE(years ARRAY, months ARRAY)`: COPYs matching stage files into `RAW_IOWA`, transforms/dedups, MERGEs into `IOWA_LIQUOR_SALES`.  
- `SP_LOAD_IOWA_LATEST()`: finds the next missing full month after `MAX(SALE_DATE)`, fetches that month to stage, then loads from stage.
- Legacy direct path: `SP_LOAD_IOWA(years ARRAY)` fetches API → table directly (bypasses stage).

### Backfill / Incremental examples
- Stage backfill by years: `CALL IOWA_LIQUOR_SALES.SP_FETCH_IOWA_TO_STAGE(ARRAY_CONSTRUCT(2022,2023), NULL);` then `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_FROM_STAGE(ARRAY_CONSTRUCT(2022,2023), NULL);`
- Stage targeted months: `CALL ...SP_FETCH_IOWA_TO_STAGE(NULL, ARRAY_CONSTRUCT('2024-11','2024-12'));` then `CALL ...SP_LOAD_IOWA_FROM_STAGE(NULL, ARRAY_CONSTRUCT('2024-11','2024-12'));`
- Incremental: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_LATEST();` (weekly task calls this).  
- Direct legacy: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA(NULL);` (incremental) or pass years to backfill.

### Reset
- `start_from_scratch.sql` drops/recreates tasks, procs, views, and drops tables while preserving stage files.

### Validate
- Row counts: `SELECT COUNT(*) FROM RAW_IOWA;` and `...FROM IOWA_LIQUOR_SALES;`
- Sample data: `SELECT * FROM IOWA_LIQUOR_SALES ORDER BY SALE_DATE DESC LIMIT 5;`
- Task history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))`.

### Layout
- `01_env/` — environment (DB/schema/warehouse/stage/file formats)
- `02_objects/` — tables, views, analysis
- `03_procs/` — stored procedures
- `04_tasks/` — tasks
- `05_tests/` — ad hoc test scripts
- `06_streamlit/` — Streamlit app setup
- `start_from_scratch.sql` — teardown/reseed helper
