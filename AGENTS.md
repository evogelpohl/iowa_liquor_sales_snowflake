# Repository Guidelines

## Project Structure & Module Organization
- SQL-first layout aligned to Snowflake objects: `01_env/` (databases/warehouses/stages), `02_objects/` (tables/views/analysis), `03_procs/` (stored procedures), `04_tasks/` (scheduled tasks), `05_tests/` (ad hoc validation SQL), `06_streamlit/` (optional app setup), and `start_from_scratch.sql` for resets.
- Files are numbered to reflect execution order; keep new scripts in that order to preserve the build pipeline narrative.

## Build, Test, and Development Commands
- Use Snow CLI: `snow sql -f <file.sql> -c <profile>`. Shell globs don’t bind to `-f`; run multiple files with multiple `-f` flags or a loop.
- Create/refresh core objects: `snow sql -f 01_env/warehouse_schema.sql`.
- Enable network access for the Socrata API: `snow sql -f 01_env/network_access.sql`.
- Canonical file format: `snow sql -f 01_env/02_file_format_iowa_json.sql`.
- Deploy logic: `snow sql -f 03_procs/sp_fetch_to_stage.sql`, `sp_load_from_stage.sql`, `sp_load_iowa.sql`, `sp_load_iowa_latest.sql`; then `snow sql -f 04_tasks/task_weekly_load.sql`.
- Optional app setup: `snow sql -f 06_streamlit/streamlit_setup.sql`.
- Reset environment: `snow sql -f start_from_scratch.sql` (then rerun env/procs/tasks).
- Use your Snowflake profile flag (`-c my_profile`) or env vars; avoid embedding credentials.

## Coding Style & Naming Conventions
- SQL keywords uppercase; identifiers snake_case; boolean flags and task names uppercase (e.g., `WEEKLY_IOWA_LOAD`).
- Stored procedures use `SP_<ACTION>_<SCOPE>`; tables/views keep the `RAW_` vs business layer distinction.
- Keep numbered directories and file prefixes stable to indicate run order; new scripts should follow the numeric prefixing pattern.
- Keep transactions explicit when needed; set `exit_on_error=true` in tooling to surface failures early.

## Testing Guidelines
- Manual checks live in `05_tests/`; run with `snow sql -f 05_tests/test_stage_load.sql` and `snow sql -f 05_tests/test_weekly_task.sql`.
- Add new validations as `test_<area>.sql` alongside existing files; include assertions (e.g., `COUNT(*)` comparisons) and comments on expected results.
- For data sanity, verify row counts (`SELECT COUNT(*) FROM RAW_IOWA;`) and recency (`ORDER BY SALE_DATE DESC LIMIT 5;`) after loads.

## Stored Procedure Pattern (stage-first)
- Fetch to stage: `CALL IOWA_LIQUOR_SALES.SP_FETCH_IOWA_TO_STAGE(years ARRAY, months ARRAY)`; months use `'YYYY-MM'`. Null args default to the last full month/year.
- Load from stage: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_FROM_STAGE(years ARRAY, months ARRAY)`; dedups by `INVOICE_AND_ITEM_NUMBER` then MERGEs into `IOWA_LIQUOR_SALES`.
- Incremental latest: `CALL IOWA_LIQUOR_SALES.SP_LOAD_IOWA_LATEST();` (task `WEEKLY_IOWA_LOAD` invokes this).
- Legacy direct path: `SP_LOAD_IOWA(years ARRAY)` fetches API→table without using stage.

## Commit & Pull Request Guidelines
- Follow the repo history: short, lower-case, imperative subjects (e.g., `add weekly task guard`), no trailing punctuation.
- PRs should describe what changed, how to run the affected SQL (including any required profile/role), and note validation results (tests run, row counts, task status).
- Link issues or tickets when available; include screenshots only for UI/Streamlit changes.

## Security & Configuration Tips
- Never commit Snowflake credentials or private keys; rely on local `~/.snowsql/config` or environment variables.
- Validate roles/warehouses before running scripts to avoid modifying the wrong account; prefer read-only roles when executing tests.
- Review `network_access.sql` changes carefully—external access integrations and network rules should be least-privilege and peer reviewed.
