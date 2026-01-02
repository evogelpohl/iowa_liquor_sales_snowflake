## Iowa Liquor Sales on Snowflake — Quickstart

This repo is ordered so you can build the pipeline from scratch and rerun pieces cleanly.

### Execution Order
Use Snow CLI (`snow sql -f ...`) with your profile.  
1) `01_env/warehouse_schema.sql` – create/ensure DB, schema, warehouse, stage, raw + silver tables, and stream on RAW_IOWA.  
2) `01_env/file_format_iowa_json.sql` – canonical JSON file format in the project schema.  
3) `01_env/network_access.sql` – network rule + external access integration for the Socrata API.  
4) `03_procs/` – create stored procedures (`SP_FETCH_IOWA_TO_STAGE`, `SP_LOAD_IOWA_FROM_STAGE`, `SP_LOAD_IOWA_LATEST`).  
5) `04_tasks/task_weekly_load.sql` – create/enable the weekly Task that calls `SP_LOAD_IOWA_LATEST`.  
6) (Optional) `06_streamlit/streamlit_setup.sql` – compute pool/warehouse/app DB for Streamlit; deploy app from `06_streamlit/app.py` via Snowsight “Add App” or with CLI helper `06_streamlit/deploy_SLit-app_via_snow-cli.sql`.  
7) (Optional) `05_tests/test_stage_load.sql` and `05_tests/test_weekly_task.sql` – manual checks.  
8) Views/analysis: `02_views/views.sql`; ad hoc SQL lives in `10_adhoc_analysis/`.

### Stored procedure pattern (stage-first)
- `SP_FETCH_IOWA_TO_STAGE(years ARRAY, months ARRAY)`: pulls from API to `RAW_STAGE` (JSONL). Months use `'YYYY-MM'`; if both args null, defaults to last full year/month.  
- `SP_LOAD_IOWA_FROM_STAGE(years ARRAY, months ARRAY)`: COPYs matching stage files into `RAW_IOWA` with `FORCE=FALSE`, then reads the `RAW_IOWA_STREAM` (append-only) to transform/dedup and MERGE only new rows into `IOWA_LIQUOR_SALES` (fast/no full scan).  
- `SP_LOAD_IOWA_LATEST()`: finds the next missing full month after `MAX(SALE_DATE)`, fetches that month to stage, then loads from stage.

### Backfill / Incremental examples
- Stage backfill by years: `CALL IOWA_LIQUOR_SALES.SP_FETCH_IOWA_TO_STAGE(ARRAY_CONSTRUCT(2022,2023), NULL);` then `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_FROM_STAGE(ARRAY_CONSTRUCT(2022,2023), NULL);`
- Stage targeted months: `CALL ...SP_FETCH_IOWA_TO_STAGE(NULL, ARRAY_CONSTRUCT('2024-11','2024-12'));` then `CALL ...SP_LOAD_IOWA_FROM_STAGE(NULL, ARRAY_CONSTRUCT('2024-11','2024-12'));`
- Incremental: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_LATEST();` (weekly task calls this).  

### Reset
- `01_env/start_from_scratch.sql` drops/recreates tasks, procs, views, stream, and drops tables while preserving stage files. Re-run env + proc + task files after reset.

### Validate
- Row counts: `SELECT COUNT(*) FROM RAW_IOWA;` and `...FROM IOWA_LIQUOR_SALES;`
- Sample data: `SELECT * FROM IOWA_LIQUOR_SALES ORDER BY SALE_DATE DESC LIMIT 5;`
- Task history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))`.

### Layout
- `01_env/` — environment (DB/schema/warehouse/stage/file formats)
- `02_views/` — views
- `03_procs/` — stored procedures
- `04_tasks/` — tasks
- `05_tests/` — ad hoc test scripts
- `06_streamlit/` — Streamlit app setup and `app.py` entrypoint
- `10_adhoc_analysis/` — analysis SQL
- `01_env/start_from_scratch.sql` — teardown/reseed helper

### Notes on recent renames
- Views now live in `02_views/` (was `02_objects/`).
- File format script is `01_env/file_format_iowa_json.sql` (dropped numeric prefix).
- Reset script sits in `01_env/start_from_scratch.sql` (no top-level copy).

### Streamlit app
- Entry file: `06_streamlit/app.py` (uses active Snowflake session; queries `EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES`).
- Deployment options:
  - Snowsight: run `06_streamlit/streamlit_setup.sql`, then Projects -> Streamlit -> “Add App” pointing to `06_streamlit/app.py` and use `IOWA_STREAMLIT_WH` (or `IOWA_WH`).
  - Snow CLI: `snow stage copy 06_streamlit/app.py @IOWA_STREAMLIT.PUBLIC.APP_STAGE --overwrite` then `snow sql -f 06_streamlit/deploy_SLit-app_via_snow-cli.sql`; grab URL with `snow streamlit get-url IOWA_STREAMLIT.PUBLIC.IOWA_LIQUOR_APP --open`. (Deploy script uses fully qualified stage `@IOWA_STREAMLIT.PUBLIC.APP_STAGE`.)
- Current view: bar chart of 2025 sale dollars by liquor category.
