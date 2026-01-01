-- Reset script: preserve staged JSONL files, but drop/recreate procs, task, view; drop tables.
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

-- Remove git repo + secret so tutorial restarts cleanly (recreate via 01_env/git_repo_setup.sql)
DROP GIT REPOSITORY IF EXISTS IOWA_LIQUOR_SALES_REPO;
DROP SECRET IF EXISTS GITHUB_PAT_EVOGELPOHL;

-- Disable and drop task
ALTER TASK IF EXISTS WEEKLY_IOWA_LOAD SUSPEND;
DROP TASK IF EXISTS WEEKLY_IOWA_LOAD;

-- Drop streams
DROP STREAM IF EXISTS RAW_IOWA_STREAM;

-- Drop procs
DROP PROCEDURE IF EXISTS SP_LOAD_IOWA_LATEST();
DROP PROCEDURE IF EXISTS SP_LOAD_IOWA_FROM_STAGE(ARRAY, ARRAY);
DROP PROCEDURE IF EXISTS SP_FETCH_IOWA_TO_STAGE(ARRAY, ARRAY);

-- Drop view
DROP VIEW IF EXISTS dim_store_location_v;

-- Drop tables (keep stage files intact)
DROP TABLE IF EXISTS RAW_IOWA;
DROP TABLE IF EXISTS IOWA_LIQUOR_SALES;

-- Recreate procs, task, view by rerunning:
-- 01_env/*.sql (env)
-- 03_procs/*.sql (procs)
-- 04_tasks/task_weekly_load.sql (task)
-- 02_views/views.sql (view)
