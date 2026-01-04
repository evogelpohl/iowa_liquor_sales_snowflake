-- Create a Cortex Agent that uses the SV_IOWA_LIQUOR_SALES semantic view as a Cortex Analyst tool.
-- Run with: snow sql -f 06_cortex_ai/create_cortex_agent.sql
-- Notes:
--   - Uses CREATE AGENT ... FROM SPECIFICATION syntax (see docs: https://docs.snowflake.com/en/sql-reference/sql/create-agent).
--   - Requires Cortex Agent feature + SNOWFLAKE.CORTEX_USER (or broader Covered AI role).
--   - Adjust model, budget, and instructions as needed.

USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

-- Drop existing agent to cleanly recreate
DROP AGENT IF EXISTS AGENT_IOWA_LIQUOR_SALES;

-- Create the agent with Cortex Analyst (text-to-SQL) bound to the semantic view
CREATE OR REPLACE AGENT AGENT_IOWA_LIQUOR_SALES
  COMMENT = 'CFO/Revenue analyst for Iowa liquor sales with Analyst tool'
  PROFILE = '{"display_name": "Iowa Liquor Sales Analyst", "color": "blue"}'
  FROM SPECIFICATION
$$
models:
  orchestration: openai-gpt-5

orchestration:
  budget:
    seconds: 30
    tokens: 16000

instructions:
  system: |
    You are a reasoning agent acting as the CFO, Sales Forecaster, Chief Revenue Officer, and Data Analyst teams for the State of Iowa. Answer questions about Iowa liquor sales performance, items, volumes, and locations. Prefer concise answers with dollar formatting and clear rankings when relevant.
  response: |
    Be concise and factual. Include both monthly values and trailing averages when helpful. Cite liquor family and item names clearly.

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: analyst
      description: Use the semantic view for Iowa liquor sales to answer numeric and slice/dice questions.

tool_resources:
  analyst:
    semantic_view: EVO_DEMO.IOWA_LIQUOR_SALES.SV_IOWA_LIQUOR_SALES
$$;

-- Optional: grant usage to a role (adjust role as needed)
GRANT USAGE ON AGENT AGENT_IOWA_LIQUOR_SALES TO ROLE PUBLIC;

-- Example invocation (uncomment to test in Snowsight worksheet):
-- SELECT SNOWFLAKE.CORTEX.COMPLETE(
--   AGENT_NAME => 'AGENT_IOWA_LIQUOR_SALES',
--   MESSAGES => [
--     OBJECT_CONSTRUCT('role', 'user', 'content', 'What were total sales last month and the top 3 liquor families?')
--   ]
-- );
