USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

CREATE OR REPLACE PROCEDURE SP_LOAD_IOWA(years ARRAY)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  EXTERNAL_ACCESS_INTEGRATIONS = (IOWA_HTTP_INT)
  HANDLER = 'run'
  EXECUTE AS CALLER
AS
$$
import requests, time
from datetime import date, timedelta
from snowflake.snowpark import Session, functions as F, types as T

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
DEDUP_KEY = "INVOICE_AND_ITEM_NUMBER"
TARGET = "IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES"
DATASET_START_YEAR = 2012

def first_day_next_month(d):
    return date(d.year + (d.month == 12), (d.month % 12) + 1, 1)

def last_complete_month_start(today):
    first_of_month = today.replace(day=1)
    return (first_of_month - timedelta(days=1)).replace(day=1)

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

def transform_df(session, rows):
    df = session.create_dataframe(rows, schema=T.StructType([
        T.StructField("invoice_line_no", T.StringType()),
        T.StructField("date", T.StringType()),
        T.StructField("store", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("address", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("zipcode", T.StringType()),
        T.StructField("store_location", T.VariantType()),
        T.StructField("county_number", T.StringType()),
        T.StructField("county", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("category_name", T.StringType()),
        T.StructField("vendor_no", T.StringType()),
        T.StructField("vendor_name", T.StringType()),
        T.StructField("itemno", T.StringType()),
        T.StructField("im_desc", T.StringType()),
        T.StructField("pack", T.StringType()),
        T.StructField("bottle_volume_ml", T.StringType()),
        T.StructField("state_bottle_cost", T.StringType()),
        T.StructField("state_bottle_retail", T.StringType()),
        T.StructField("sale_bottles", T.StringType()),
        T.StructField("sale_dollars", T.StringType()),
        T.StructField("sale_liters", T.StringType()),
        T.StructField("sale_gallons", T.StringType()),
    ]))

    df = (
        df.with_column("INVOICE_AND_ITEM_NUMBER", F.col("invoice_line_no"))
          .with_column("SALE_DATE", F.to_date(F.col("date")))
          .with_column("SALE_YEAR", F.year("SALE_DATE"))
          .with_column("SALE_MONTH", F.month("SALE_DATE"))
          .with_column("STORE_NUMBER", F.col("store").cast(T.IntegerType()))
          .with_column("STORE_NAME", F.col("name"))
          .with_column("ADDRESS", F.col("address"))
          .with_column("CITY", F.col("city"))
          .with_column("ZIP_CODE", F.col("zipcode"))
          .with_column(
              "STORE_LOCATION",
              F.coalesce(
                  F.try_parse_json(F.col("store_location")),
                  F.to_variant(F.col("store_location")),
              ),
          )
          .with_column("COUNTY_NUMBER", F.col("county_number").cast(T.IntegerType()))
          .with_column("COUNTY", F.col("county"))
          .with_column("CATEGORY", F.col("category").cast(T.IntegerType()))
          .with_column("CATEGORY_NAME", F.col("category_name"))
          .with_column("VENDOR_NUMBER", F.col("vendor_no").cast(T.IntegerType()))
          .with_column("VENDOR_NAME", F.col("vendor_name"))
          .with_column("ITEM_NUMBER", F.col("itemno").cast(T.IntegerType()))
          .with_column("ITEM_DESCRIPTION", F.col("im_desc"))
          .with_column("PACK", F.col("pack").cast(T.IntegerType()))
          .with_column("BOTTLE_VOLUME_ML", F.col("bottle_volume_ml").cast(T.IntegerType()))
          .with_column("STATE_BOTTLE_COST", F.col("state_bottle_cost").cast(T.DoubleType()))
          .with_column("STATE_BOTTLE_RETAIL", F.col("state_bottle_retail").cast(T.DoubleType()))
          .with_column("BOTTLES_SOLD", F.col("sale_bottles").cast(T.IntegerType()))
          .with_column("SALE_DOLLARS", F.col("sale_dollars").cast(T.DoubleType()))
          .with_column("VOLUME_SOLD_LITERS", F.col("sale_liters").cast(T.DoubleType()))
          .with_column("VOLUME_SOLD_GALLONS", F.col("sale_gallons").cast(T.DoubleType()))
    )

    lc = F.lower(F.col("CATEGORY_NAME"))
    df = df.with_column("LIQUOR_CATEGORY", F.when(
        lc.like("%whisk%") | lc.like("%bourbon%") | lc.like("%scotch%") | lc.like("%rye%") |
        lc.like("%irish whisk%") | lc.like("%canadian whisk%") | lc.like("%tennessee whisk%"),
        F.lit("WHISKIES"))
        .when(lc.like("%brandi%"), F.lit("BRANDIES"))
        .when(lc.like("%vodka%"), F.lit("VODKAS"))
        .when(lc.like("%wine%"), F.lit("WINES"))
        .when(lc.like("%gin%"), F.lit("GINS"))
        .when(lc.like("%liqueur%") | lc.like("%triple sec%") | lc.like("%cordial%"), F.lit("LIQUEURS"))
        .when(lc.like("%tequila%"), F.lit("TEQUILAS"))
        .when(lc.like("%schnapps%"), F.lit("SCHNAPPS"))
        .when(lc.like("%rum%"), F.lit("RUMS"))
        .when(lc.like("%mezcal%"), F.lit("MEZCALS"))
        .when(lc.like("%beer%"), F.lit("BEERS"))
        .when(lc.like("%champagne%") | lc.like("%sparkling%"), F.lit("CHAMPAGNE & SPARKLING"))
        .when(lc.like("%sake%"), F.lit("SAKE"))
        .when(lc.like("%absinthe%"), F.lit("ABSINTHE"))
        .when(lc.like("%bitters%"), F.lit("BITTERS"))
        .when(lc.like("%vermouth%"), F.lit("VERMOUTH"))
        .when(lc.like("%cocktail%"), F.lit("COCKTAIL MIXERS"))
        .when(lc.like("%neutral grain spirits%"), F.lit("NEUTRAL SPIRITS"))
        .when(lc.like("%distilled spirits specialty%") | lc.like("%temporary & specialty%")
              | lc.like("%special order items%"), F.lit("SPECIALTY SPIRITS"))
        .otherwise(F.lit("OTHER"))
    )

    return df.select(
        "INVOICE_AND_ITEM_NUMBER","SALE_DATE","SALE_YEAR","SALE_MONTH",
        "STORE_NUMBER","STORE_NAME","ADDRESS","CITY","ZIP_CODE","STORE_LOCATION",
        "COUNTY_NUMBER","COUNTY","CATEGORY","CATEGORY_NAME","LIQUOR_CATEGORY",
        "VENDOR_NUMBER","VENDOR_NAME","ITEM_NUMBER","ITEM_DESCRIPTION","PACK",
        "BOTTLE_VOLUME_ML","STATE_BOTTLE_COST","STATE_BOTTLE_RETAIL","BOTTLES_SOLD",
        "SALE_DOLLARS","VOLUME_SOLD_LITERS","VOLUME_SOLD_GALLONS"
    ).drop_duplicates([DEDUP_KEY])

def merge_target(session, df):
    df.create_or_replace_temp_view("SRC_IOWA")
    session.sql(f"""
        MERGE INTO {TARGET} t
        USING SRC_IOWA s
        ON t.{DEDUP_KEY} = s.{DEDUP_KEY}
        WHEN MATCHED THEN UPDATE SET
            INVOICE_AND_ITEM_NUMBER = s.INVOICE_AND_ITEM_NUMBER,
            SALE_DATE = s.SALE_DATE,
            SALE_YEAR = s.SALE_YEAR,
            SALE_MONTH = s.SALE_MONTH,
            STORE_NUMBER = s.STORE_NUMBER,
            STORE_NAME = s.STORE_NAME,
            ADDRESS = s.ADDRESS,
            CITY = s.CITY,
            ZIP_CODE = s.ZIP_CODE,
            STORE_LOCATION = s.STORE_LOCATION,
            COUNTY_NUMBER = s.COUNTY_NUMBER,
            COUNTY = s.COUNTY,
            CATEGORY = s.CATEGORY,
            CATEGORY_NAME = s.CATEGORY_NAME,
            LIQUOR_CATEGORY = s.LIQUOR_CATEGORY,
            VENDOR_NUMBER = s.VENDOR_NUMBER,
            VENDOR_NAME = s.VENDOR_NAME,
            ITEM_NUMBER = s.ITEM_NUMBER,
            ITEM_DESCRIPTION = s.ITEM_DESCRIPTION,
            PACK = s.PACK,
            BOTTLE_VOLUME_ML = s.BOTTLE_VOLUME_ML,
            STATE_BOTTLE_COST = s.STATE_BOTTLE_COST,
            STATE_BOTTLE_RETAIL = s.STATE_BOTTLE_RETAIL,
            BOTTLES_SOLD = s.BOTTLES_SOLD,
            SALE_DOLLARS = s.SALE_DOLLARS,
            VOLUME_SOLD_LITERS = s.VOLUME_SOLD_LITERS,
            VOLUME_SOLD_GALLONS = s.VOLUME_SOLD_GALLONS
        WHEN NOT MATCHED THEN INSERT (
            INVOICE_AND_ITEM_NUMBER, SALE_DATE, SALE_YEAR, SALE_MONTH,
            STORE_NUMBER, STORE_NAME, ADDRESS, CITY, ZIP_CODE, STORE_LOCATION,
            COUNTY_NUMBER, COUNTY, CATEGORY, CATEGORY_NAME, LIQUOR_CATEGORY,
            VENDOR_NUMBER, VENDOR_NAME, ITEM_NUMBER, ITEM_DESCRIPTION, PACK,
            BOTTLE_VOLUME_ML, STATE_BOTTLE_COST, STATE_BOTTLE_RETAIL, BOTTLES_SOLD,
            SALE_DOLLARS, VOLUME_SOLD_LITERS, VOLUME_SOLD_GALLONS
        ) VALUES (
            s.INVOICE_AND_ITEM_NUMBER, s.SALE_DATE, s.SALE_YEAR, s.SALE_MONTH,
            s.STORE_NUMBER, s.STORE_NAME, s.ADDRESS, s.CITY, s.ZIP_CODE, s.STORE_LOCATION,
            s.COUNTY_NUMBER, s.COUNTY, s.CATEGORY, s.CATEGORY_NAME, s.LIQUOR_CATEGORY,
            s.VENDOR_NUMBER, s.VENDOR_NAME, s.ITEM_NUMBER, s.ITEM_DESCRIPTION, s.PACK,
            s.BOTTLE_VOLUME_ML, s.STATE_BOTTLE_COST, s.STATE_BOTTLE_RETAIL, s.BOTTLES_SOLD,
            s.SALE_DOLLARS, s.VOLUME_SOLD_LITERS, s.VOLUME_SOLD_GALLONS
        )
    """).collect()

def run(session: Session, years=None) -> str:
    # Normalize years array from Snowflake to Python list of ints
    year_list = []
    if years:
        try:
            year_list = [int(y) for y in years]
        except Exception:
            year_list = []
    last_full = last_complete_month_start(date.today())
    max_date = session.sql(f"SELECT MAX(SALE_DATE) AS D FROM {TARGET}").collect()[0]["D"]
    total_rows = 0

    def ingest_month(year_val, month_val):
        nonlocal total_rows
        if year_val < DATASET_START_YEAR:
            return
        start = date(year_val, month_val, 1)
        if start > last_full:
            return
        next_month = first_day_next_month(start)
        month_rows = []
        for page in fetch_pages(start, next_month - timedelta(days=1)):
            month_rows.extend(page)
        if month_rows:
            df = transform_df(session, month_rows)
            merge_target(session, df)
            total_rows += df.count()

    if year_list:
        # Explicit backfill for provided years
        for y in sorted(set(year_list)):
            if y > last_full.year:
                continue
            end_month = last_full.month if y == last_full.year else 12
            for m in range(1, end_month + 1):
                ingest_month(y, m)
        return f"Ingested {total_rows} rows for years {sorted(set(year_list))} through {last_full}"

    # Incremental path (no years passed)
    start_anchor = last_complete_month_start(date.today()) if max_date is None else first_day_next_month(max_date)
    if start_anchor > last_full:
        return "No new full months to ingest."
    cursor = start_anchor
    while cursor <= last_full:
        ingest_month(cursor.year, cursor.month)
        cursor = first_day_next_month(cursor)
    return f"Ingested {total_rows} rows through {last_full}"
$$;
