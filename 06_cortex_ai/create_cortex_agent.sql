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
  COMMENT = 'Financial analyst for Iowa liquor sales performance. Answers questions on revenue, volume, gross margin, trends, and category/item mix using the semantic view.'
  PROFILE = '{"display_name": "Iowa Liquor Sales Analyst", "color": "blue"}'
  FROM SPECIFICATION
$$
models:
  orchestration: auto

orchestration:
  budget:
    seconds: 300
    tokens: 16000

instructions:
  system: |
    You are a reasoning agent acting as the CFO, Sales Forecaster, Chief Revenue Officer, and Data Analyst teams for the State of Iowa. Answer questions about Iowa liquor sales performance, items, volumes, and locations. Prefer concise answers with dollar formatting and clear rankings when relevant.
  response: |
    Be concise and factual. Include both monthly values and trailing averages when helpful. Cite liquor family and item names clearly.
  sample_questions:
    - question: "Compare total sales for the latest full month vs the same month last year; show dollar and percent change and the top 3 liquor families driving the delta."
    - question: "Rank the top 10 items by trailing 3-month sales and include the month-over-month percent change for the latest month."
    - question: "Plot the past 26 weeks of sales with a linear trendline (slope and intercept) and flag any weeks above +2 standard deviations."
    - question: "Which counties have the highest year-over-year growth in bottles sold, and what is the leading liquor family in each?"
    - question: "Show trailing-12-month sales by liquor family with share of total and the change from last month to the prior month."
    - question: "Which liquor families contribute the most gross margin in the latest quarter, and how do they compare to the same quarter last year?"
    - question: "Identify the top 10 items by gross margin dollars over the trailing 3 months and show their gross margin rate alongside the prior 3-month period."
    - question: "For the past 12 months, show monthly gross margin rate and dollars with a trendline; flag any month where margin rate drops by more than 2 percentage points vs prior month."

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: analyst
      description: Use the semantic view for Iowa liquor sales to answer numeric and slice/dice questions.

tool_resources:
  analyst:
    semantic_view: EVO_DEMO.IOWA_LIQUOR_SALES.SV_IOWA_LIQUOR_SALES
$$;

GRANT USAGE ON AGENT AGENT_IOWA_LIQUOR_SALES TO ROLE PUBLIC;

-- Optional: register the agent with Snowflake Intelligence object (requires feature + ALTER on the object)
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
  ADD AGENT EVO_DEMO.IOWA_LIQUOR_SALES.AGENT_IOWA_LIQUOR_SALES;

-- Example invocation (uncomment to test in Snowsight worksheet):
-- SELECT SNOWFLAKE.CORTEX.COMPLETE(
--   AGENT_NAME => 'AGENT_IOWA_LIQUOR_SALES',
--   MESSAGES => [
--     OBJECT_CONSTRUCT('role', 'user', 'content', 'What were total sales last month and the top 3 liquor families?')
--   ]
-- );
