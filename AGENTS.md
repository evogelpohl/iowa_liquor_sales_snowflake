# Repository Guidelines

## Project Structure & Module Organization
- SQL-first layout aligned to Snowflake objects: `01_env/` (databases/warehouses/stages), `02_objects/` (tables/views/analysis), `03_procs/` (stored procedures), `04_tasks/` (scheduled tasks), `05_tests/` (ad hoc validation SQL), `06_streamlit/` (optional app setup), and `start_from_scratch.sql` for resets.
- Files are numbered to reflect execution order; keep new scripts in that order to preserve the build pipeline narrative.

## Build, Test, and Development Commands
- Create/refresh core objects: `snowsql -f 01_env/warehouse_schema.sql -o exit_on_error=true`.
- Enable network access for the Socrata API: `snowsql -f 01_env/network_access.sql`.
- Deploy logic: `snowsql -f 03_procs/all_procs.sql` (or individual files) then `snowsql -f 04_tasks/task_weekly_load.sql`.
- Optional app setup: `snowsql -f 06_streamlit/streamlit_setup.sql`.
- Reset environment: `snowsql -f start_from_scratch.sql`.
- Use your Snowflake profile (e.g., `-c my_profile`) to avoid embedding credentials; prefer `SNOWSQL_ACCOUNT/USER/ROLE/WAREHOUSE/DB/SCHEMA` env vars when scripting.

## Coding Style & Naming Conventions
- SQL keywords uppercase; identifiers snake_case; boolean flags and task names uppercase (e.g., `WEEKLY_IOWA_LOAD`).
- Stored procedures use `SP_<ACTION>_<SCOPE>`; tables/views keep the `RAW_` vs business layer distinction.
- Keep numbered directories and file prefixes stable to indicate run order; new scripts should follow the numeric prefixing pattern.
- Keep transactions explicit when needed; set `exit_on_error=true` in tooling to surface failures early.

## Testing Guidelines
- Manual checks live in `05_tests/`; run with `snowsql -f 05_tests/test_stage_load.sql` and `snowsql -f 05_tests/test_weekly_task.sql`.
- Add new validations as `test_<area>.sql` alongside existing files; include assertions (e.g., `COUNT(*)` comparisons) and comments on expected results.
- For data sanity, verify row counts (`SELECT COUNT(*) FROM RAW_IOWA;`) and recency (`ORDER BY SALE_DATE DESC LIMIT 5;`) after loads.

## Commit & Pull Request Guidelines
- Follow the repo history: short, lower-case, imperative subjects (e.g., `add weekly task guard`), no trailing punctuation.
- PRs should describe what changed, how to run the affected SQL (including any required profile/role), and note validation results (tests run, row counts, task status).
- Link issues or tickets when available; include screenshots only for UI/Streamlit changes.

## Security & Configuration Tips
- Never commit Snowflake credentials or private keys; rely on local `~/.snowsql/config` or environment variables.
- Validate roles/warehouses before running scripts to avoid modifying the wrong account; prefer read-only roles when executing tests.
- Review `network_access.sql` changes carefully—external access integrations and network rules should be least-privilege and peer reviewed.
