USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

CREATE OR REPLACE PROCEDURE SP_LOAD_IOWA_LATEST()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
  EXECUTE AS CALLER
AS
$$
from datetime import date, timedelta
from snowflake.snowpark import Session

DEDUP_KEY = "INVOICE_AND_ITEM_NUMBER"
TARGET = "EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES"
LOADER_PROC = "EVO_DEMO.IOWA_LIQUOR_SALES.SP_LOAD_IOWA"

def first_day_next_month(d):
    return date(d.year + (d.month == 12), (d.month % 12) + 1, 1)

def last_complete_month_start(today):
    first_of_month = today.replace(day=1)
    return (first_of_month - timedelta(days=1)).replace(day=1)

def run(session: Session) -> str:
    max_date = session.sql(f"SELECT MAX(SALE_DATE) AS D FROM {TARGET}").collect()[0]["D"]
    last_full = last_complete_month_start(date.today())
    start_anchor = last_full if max_date is None else first_day_next_month(max_date)
    if start_anchor > last_full:
        return f"No new full months to ingest. Last full month: {last_full}"
    result = session.sql(f"CALL {LOADER_PROC}(NULL)").collect()[0][0]
    return f"Ran incremental load from {start_anchor} through {last_full}; result={result}"
$$;
