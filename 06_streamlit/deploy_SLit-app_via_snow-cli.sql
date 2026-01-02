-- Deploy Streamlit app using Snow CLI + SQL
-- Prereq: Upload the app file to the stage (from repo root):
--   snow stage copy 06_streamlit/app.py @IOWA_STREAMLIT.PUBLIC.APP_STAGE --overwrite

USE DATABASE IOWA_STREAMLIT;
USE SCHEMA PUBLIC;

-- Ensure the app stage exists
CREATE STAGE IF NOT EXISTS APP_STAGE;

-- Create/replace the Streamlit app pointing at the staged file
CREATE OR REPLACE STREAMLIT IOWA_LIQUOR_APP
  ROOT_LOCATION='@IOWA_STREAMLIT.PUBLIC.APP_STAGE'
  MAIN_FILE='app.py'
  QUERY_WAREHOUSE=IOWA_STREAMLIT_WH;

-- Get the URL after deploying:
--   snow streamlit get-url IOWA_STREAMLIT.PUBLIC.IOWA_LIQUOR_APP --open
