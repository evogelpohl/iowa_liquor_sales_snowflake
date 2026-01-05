## Iowa Liquor Sales on Snowflake — Quickstart

This repo is ordered so you can build the pipeline from scratch and rerun pieces cleanly.

### The Project
- End-to-end Snowflake demo for the Iowa State Liquor Sales dataset (public, item-level retail sales from Iowa state liquor stores). Each row is an item on a sales invoice, with store, vendor, product, volumes, costs, and sales dollars. We normalize categories, roll up to liquor families, and include store geospatial points for mapping.
- Creates the full landing-to-analytics stack: database/schema/warehouse/stage, file formats, ingest procs, weekly task, core views (including geospatial store view), and ad hoc dashboard starter SQL.
- Adds a Cortex Analyst semantic view plus a Cortex Agent for natural-language SQL and Snowflake Intelligence integration, so you can ask NL questions about revenue, volume, gross margin, categories/families, items, and geography.

### Execution Order
Use Snow CLI (`snow sql -f ...`) with your profile.  
1) `01_env/warehouse_schema.sql` – create/ensure DB, schema, warehouse, stage, raw + silver tables, and stream on RAW_IOWA.  
2) `01_env/file_format_iowa_json.sql` – canonical JSON file format in the project schema.  
3) `01_env/network_access.sql` – network rule + external access integration for the Socrata API.  
4) `01_env/date_dimension_load.sql` – create/load DATE_DIM from staged CSV (`@RAW_STAGE/date_dim/datedimension.csv`). Source: https://github.com/wysiwys/datedim_with_holidays (CSV generated externally; Python not included here).  
5) `01_env/views.sql` – create required views (e.g., `dim_store_location_v`).  
6) `03_procs/` – create stored procedures (`SP_FETCH_IOWA_TO_STAGE`, `SP_LOAD_IOWA_FROM_STAGE`, `SP_LOAD_IOWA_LATEST`).  
7) `04_tasks/task_weekly_load.sql` – create/enable the weekly Task that calls `SP_LOAD_IOWA_LATEST`.  
8) (Optional) `05_tests/test_stage_load.sql` and `05_tests/test_weekly_task.sql` – manual checks.  
9) Ad hoc/dashboard seeds: `10_adhoc_analysis/` (starter SQL for Snowsight dashboards).

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
- `01_env/` — environment (DB/schema/warehouse/stage/file formats/views)
- `03_procs/` — stored procedures
- `04_tasks/` — tasks
- `05_tests/` — ad hoc test scripts
- `06_cortex_ai/` — Cortex Analyst semantic view YAML + apply script, and Cortex Agent creation script
- `09_sample_data/` — sample CSV (20 rows)
- `10_adhoc_analysis/` — starter SQL for Snowsight dashboards
- `01_env/start_from_scratch.sql` — teardown/reseed helper

### Notes on recent renames
- Views now live in `01_env/views.sql` (folder `02_views/` removed).
- File format script is `01_env/file_format_iowa_json.sql` (dropped numeric prefix).
- Reset script sits in `01_env/start_from_scratch.sql` (no top-level copy).

### Cortex AI artifacts (06_cortex_ai)
- `semantic_view.yaml` — semantic model for Analyst (renamed dimensions: Iowa category = `CATEGORY_NAME` with synonyms, normalized rollup `LIQUOR_FAMILY`, item label `ITEM` with synonyms).
- `semantic_view_create.sql` — verifies then (re)creates the semantic view from the YAML and copies grants.
- `agent_create.sql` — builds `AGENT_IOWA_LIQUOR_SALES` with profile, goal, and Cortex Analyst tool bound to `SV_IOWA_LIQUOR_SALES`; uses orchestration model `openai-gpt-5` (adjust as needed). Grants USAGE to PUBLIC by default.
