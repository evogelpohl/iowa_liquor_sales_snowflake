-- GitHub-backed git repository setup for Snowsight/CLI workflows.
-- Update the placeholders before running (GitHub username + PAT). Do not commit real secrets.
USE ROLE ACCOUNTADMIN;
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

-- 1) Create/update the secret (replace values before execution).
-- Example: username = 'evogelpohl', password = '<PAT_FROM_GITHUB>'
CREATE OR REPLACE SECRET GITHUB_PAT_EVOGELPOHL
  TYPE = PASSWORD
  USERNAME = '<<GITHUB_USERNAME>>'
  PASSWORD = '<<GITHUB_PAT>>';

-- 2) Allow Snowflake to reach the repo with the PAT.
CREATE OR REPLACE API INTEGRATION GITHUB_PUBLIC_GIT
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = (
    'https://github.com/evogelpohl/iowa_liquor_sales_snowflake.git',
    'https://github.com/evogelpohl/iowa_liquor_sales_snowflake',
    'https://github.com/evogelpohl/iowa_liquor_sales_snowflake/'
  )
  ALLOWED_AUTHENTICATION_SECRETS = (GITHUB_PAT_EVOGELPOHL)
  ENABLED = TRUE;

-- 3) Create or refresh the git repository object.
CREATE OR REPLACE GIT REPOSITORY IOWA_LIQUOR_SALES_REPO
  API_INTEGRATION = GITHUB_PUBLIC_GIT
  ORIGIN = 'https://github.com/evogelpohl/iowa_liquor_sales_snowflake.git';

-- 4) Attach credentials and fetch the latest commit.
ALTER GIT REPOSITORY IOWA_LIQUOR_SALES_REPO
  SET GIT_CREDENTIALS = GITHUB_PAT_EVOGELPOHL;

ALTER GIT REPOSITORY IOWA_LIQUOR_SALES_REPO
  FETCH;
