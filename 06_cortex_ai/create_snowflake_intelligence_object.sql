-- https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence#tools
-- You can only have on Snowflake Intelligence object per account
-- This was done already on Jan 4, 2026. It works, probablyl needs roles added later. No need to do this now for demos.
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- Creating the Iowa Liquor Sales Cortex Analsyst. 
-- First up, create the semantic view. (DOC HERE: https://docs.snowflake.com/en/user-guide/views-semantic/overview)
-- We'll use sql to create it via snow cli sql commands: https://docs.snowflake.com/en/user-guide/views-semantic/sql
-- We'll use the CREATE SEMANTIC VIEW FROM YAML. DOCS HERE: https://docs.snowflake.com/en/sql-reference/stored-procedures/system_create_semantic_view_from_yaml
-- The YAML file is stored in the 10_adhoc_analysis folder as iowa_liquor_sales_semantic_view.yaml as we'll consider it managed code
-- We'll follow best practices for semantic view (sv) creation: https://docs.snowflake.cn/en/user-guide/views-semantic/best-practices-dev

-- My assumption is that the yaml file is in a stage folder or is sent along with the command. I don't know. You need to research that for snow cli programmablility. For now, the YAML file is here: ./10_adhoc_analysis/iowa_liquor_sales_semantic_view.yaml

-- The official semantic view page is here: https://docs.snowflake.com/en/user-guide/views-semantic/overview

-- NOTE: From the snowsight UI, it appears Snowflake has set "semantic models" to legacy. We'll create a semantic view(s) instead.

SHOW SEMANTIC VIEWS;

-- Drop any prior attempt
DROP SEMANTIC VIEW IF EXISTS LIQUOR_SALES_SV;

-- Create the semantic view from YAML (YAML is under repo at 10_adhoc_analysis/iowa_liquor_sales_semantic_view.yaml)
-- Adjust @stage/path if you stage the YAML; here we embed it via literal.
DECLARE
  yaml STRING := $$
semantic_models:
  - name: iowa_liquor_sales
    description: Iowa liquor sales fact with date and store context
    database: EVO_DEMO
    schema: IOWA_LIQUOR_SALES
    table: IOWA_LIQUOR_SALES
    primary_key:
      - INVOICE_AND_ITEM_NUMBER
    dimensions:
      - name: sale_date
        column: SALE_DATE
        type: date
      - name: sale_year
        column: SALE_YEAR
        type: number
      - name: sale_month
        column: SALE_MONTH
        type: number
      - name: store_number
        column: STORE_NUMBER
        type: number
      - name: store_name
        column: STORE_NAME
        type: text
      - name: city
        column: CITY
        type: text
      - name: county
        column: COUNTY
        type: text
      - name: liquor_category
        column: LIQUOR_CATEGORY
        type: text
      - name: vendor_name
        column: VENDOR_NAME
        type: text
      - name: item_description
        column: ITEM_DESCRIPTION
        type: text
    measures:
      - name: sale_dollars
        expr: SUM(SALE_DOLLARS)
        type: number
      - name: bottles_sold
        expr: SUM(BOTTLES_SOLD)
        type: number
      - name: volume_sold_liters
        expr: SUM(VOLUME_SOLD_LITERS)
        type: number
      - name: volume_sold_gallons
        expr: SUM(VOLUME_SOLD_GALLONS)
        type: number
$$;

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  semantic_view_name => 'LIQUOR_SALES_SV',
  database_name => 'EVO_DEMO',
  schema_name => 'IOWA_LIQUOR_SALES',
  yaml_def => :yaml
);
