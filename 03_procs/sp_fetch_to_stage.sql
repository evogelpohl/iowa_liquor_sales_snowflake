USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

CREATE OR REPLACE PROCEDURE SP_FETCH_IOWA_TO_STAGE(years ARRAY, months ARRAY)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (IOWA_HTTP_INT)
  HANDLER = 'run'
  EXECUTE AS CALLER
AS
$$
import io, json, requests, time
from datetime import date, timedelta
from snowflake.snowpark import Session

DATASET_ID = "m3tr-qhgy"
BASE_URL = f"https://data.iowa.gov/resource/{DATASET_ID}.json"
SOQL_COLUMNS = [
    "invoice_line_no","date","store","name","address","city","zipcode","store_location",
    "county_number","county","category","category_name","vendor_no","vendor_name","itemno",
    "im_desc","pack","bottle_volume_ml","state_bottle_cost","state_bottle_retail",
    "sale_bottles","sale_dollars","sale_liters","sale_gallons"
]
SOQL_SELECT = ", ".join(f"`{c}`" for c in SOQL_COLUMNS)
PAGE_SIZE = 50000
DATASET_START_YEAR = 2012
STAGE_BASE = "@IOWA_LIQUOR_SALES.RAW_STAGE/iowa"

def first_day_next_month(d): return date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
def last_complete_month_start(today):
    first_of_month = today.replace(day=1)
    return (first_of_month - timedelta(days=1)).replace(day=1)

def parse_months(months_in, last_full):
    months_out = []
    if not months_in:
        return months_out
    for m in months_in:
        try:
            parts = str(m).split("-")
            if len(parts) != 2:
                continue
            y = int(parts[0])
            mo = int(parts[1])
            if mo < 1 or mo > 12 or y < DATASET_START_YEAR:
                continue
            start = date(y, mo, 1)
            if start > last_full:
                continue
            months_out.append((y, mo))
        except Exception:
            continue
    return sorted(set(months_out))

def fetch_pages(start, end, max_retries=5, backoff=2.0):
    start_str = start.strftime("%Y-%m-%dT00:00:00.000")
    end_str = end.strftime("%Y-%m-%dT23:59:59.999")
    offset = 0
    while True:
        params = {
            "$select": SOQL_SELECT,
            "$where": f"date between '{start_str}' and '{end_str}'",
            "$order": "date",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        resp = None
        for attempt in range(max_retries):
            resp = requests.get(BASE_URL, params=params, timeout=300)
            if resp.status_code in {429,500,502,503,504} and attempt < max_retries - 1:
                time.sleep(backoff * (attempt + 1))
                continue
            resp.raise_for_status()
            break
        batch = resp.json()
        if not batch:
            break
        yield batch
        offset += PAGE_SIZE

def write_page_to_stage(session, records, year_val, month_val, page_idx):
    if not records:
        return 0
    payload = ("\n".join(json.dumps(r) for r in records) + "\n").encode("utf-8")
    stage_path = f"{STAGE_BASE}/year={year_val:04d}/month={month_val:02d}/page_{year_val:04d}{month_val:02d}_{page_idx:05d}.jsonl"
    session.file.put_stream(
        io.BytesIO(payload),
        stage_path,
        overwrite=True,
        auto_compress=False,
        source_compression="NONE",
    )
    print(f"Wrote file {stage_path} rows={len(records)}")
    return len(records)

def run(session: Session, years=None, months=None) -> str:
    year_list = []
    if years:
        try:
            year_list = [int(y) for y in years]
        except Exception:
            year_list = []

    last_full = last_complete_month_start(date.today())
    total_rows = 0
    total_files = 0

    month_pairs = parse_months(months, last_full)

    if month_pairs:
        months_to_run = month_pairs
    else:
        if year_list:
            years_to_run = [y for y in sorted(set(year_list)) if DATASET_START_YEAR <= y <= last_full.year]
        else:
            years_to_run = [last_full.year]
        months_to_run = []
        for y in years_to_run:
            end_month = last_full.month if y == last_full.year else 12
            for m in range(1, end_month + 1):
                months_to_run.append((y, m))

    for y, m in months_to_run:
        month_start = date(y, m, 1)
        next_month = first_day_next_month(month_start)
        month_end = next_month - timedelta(days=1)
        page_idx = 0
        for page in fetch_pages(month_start, month_end):
            page_idx += 1
            rows_written = write_page_to_stage(session, page, y, m, page_idx)
            total_rows += rows_written
            total_files += 1
        print(f"Finished month {y}-{m:02d} pages={page_idx}")

    return f"Wrote {total_rows} rows across {total_files} files to stage {STAGE_BASE} through {last_full}"
$$;
