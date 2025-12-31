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

TARGET = "EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES"
FETCH_PROC = "EVO_DEMO.IOWA_LIQUOR_SALES.SP_FETCH_IOWA_TO_STAGE"
LOAD_PROC = "EVO_DEMO.IOWA_LIQUOR_SALES.SP_LOAD_IOWA_FROM_STAGE"
DATASET_START_YEAR = 2012

def first_day_next_month(d):
    return date(d.year + (d.month == 12), (d.month % 12) + 1, 1)

def last_complete_month_start(today):
    first_of_month = today.replace(day=1)
    return (first_of_month - timedelta(days=1)).replace(day=1)

def months_between(start_anchor, last_full):
    months = []
    cursor = date(start_anchor.year, start_anchor.month, 1)
    while cursor <= last_full:
        months.append(cursor.strftime("%Y-%m"))
        cursor = first_day_next_month(cursor)
    return months

def run(session: Session) -> str:
    max_date = session.sql(f"SELECT MAX(SALE_DATE) AS D FROM {TARGET}").collect()[0]["D"]
    last_full = last_complete_month_start(date.today())
    if max_date is None:
        start_anchor = date(DATASET_START_YEAR, 1, 1)
    else:
        start_anchor = first_day_next_month(max_date)
    if start_anchor > last_full:
        return f"No new full months to ingest. Last full month: {last_full}"

    month_labels = months_between(start_anchor, last_full)

    month_array_sql = ",".join(f"'{m}'" for m in month_labels)
    fetch_result = session.sql(
        f"CALL {FETCH_PROC}(NULL, ARRAY_CONSTRUCT({month_array_sql}))"
    ).collect()[0][0]
    load_result = session.sql(
        f"CALL {LOAD_PROC}(NULL, ARRAY_CONSTRUCT({month_array_sql}))"
    ).collect()[0][0]

    return f"Fetched+loaded months {month_labels} through {last_full}; fetch_result={fetch_result}; load_result={load_result}"
$$;
