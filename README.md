## Iowa Liquor Sales on Snowflake — Quickstart

This repo is ordered so you can build the pipeline from scratch and rerun pieces cleanly.

### Execution Order
1) `01_env/warehouse_schema.sql` – create/ensure DB, schema, warehouse, stage, file format, raw + silver tables.  
2) `01_env/network_access.sql` – network rule + external access integration for the Socrata API.  
3) `03_procs/` – create all stored procedures (`SP_LOAD_IOWA`, `SP_FETCH_IOWA_TO_STAGE`, `SP_LOAD_IOWA_FROM_STAGE`, `SP_LOAD_IOWA_LATEST`).  
4) `04_tasks/task_weekly_load.sql` – create/enable the weekly Task that calls `SP_LOAD_IOWA_LATEST`.  
5) (Optional) `06_streamlit/streamlit_setup.sql` – compute pool/warehouse/app DB for Streamlit.  
6) (Optional) `05_tests/test_stage_load.sql` and `05_tests/test_weekly_task.sql` – manual checks.  
7) Analysis/views: `02_objects/views.sql`, `02_objects/analysis.sql`.

### Backfill / Incremental
- Direct fetch/merge: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA(ARRAY_CONSTRUCT(2025));` or `NULL` for incremental.  
- Stage-based: `CALL ...SP_FETCH_IOWA_TO_STAGE(<years>);` then `CALL ...SP_LOAD_IOWA_FROM_STAGE(<years>);`.
- Weekly Task: `WEEKLY_IOWA_LOAD` calls `SP_LOAD_IOWA_LATEST` (incremental last full month).

### Reset
- `start_from_scratch.sql` drops/recreates tasks, procs, views, and truncates tables while preserving stage files.

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

