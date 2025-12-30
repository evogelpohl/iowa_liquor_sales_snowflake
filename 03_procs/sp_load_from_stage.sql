USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

CREATE OR REPLACE PROCEDURE SP_LOAD_IOWA_FROM_STAGE(years ARRAY)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
  EXECUTE AS CALLER
AS
$$
from datetime import date, timedelta
from snowflake.snowpark import Session, functions as F, types as T

DB = "EVO_DEMO"
SCHEMA = "IOWA_LIQUOR_SALES"
DEDUP_KEY = "INVOICE_AND_ITEM_NUMBER"
TARGET = f"{DB}.{SCHEMA}.IOWA_LIQUOR_SALES"
RAW_TABLE = f"{DB}.{SCHEMA}.RAW_IOWA"
STAGE_BASE = f"@{DB}.{SCHEMA}.RAW_STAGE/iowa"
DATASET_START_YEAR = 2012

def first_day_next_month(d): return date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
def last_complete_month_start(today):
    first_of_month = today.replace(day=1)
    return (first_of_month - timedelta(days=1)).replace(day=1)

def build_stage_pattern(years):
    if not years:
        return ".*\\.jsonl"
    parts = [f".*year={y}/.*\\.jsonl" for y in sorted(set(years))]
    return "|".join(parts)

def transform_variant_df(df):
    df = df.select(
        F.col("RAW")["invoice_line_no"].as_("invoice_line_no"),
        F.col("RAW")["date"].as_("date"),
        F.col("RAW")["store"].as_("store"),
        F.col("RAW")["name"].as_("name"),
        F.col("RAW")["address"].as_("address"),
        F.col("RAW")["city"].as_("city"),
        F.col("RAW")["zipcode"].as_("zipcode"),
        F.col("RAW")["store_location"].as_("store_location"),
        F.col("RAW")["county_number"].as_("county_number"),
        F.col("RAW")["county"].as_("county"),
        F.col("RAW")["category"].as_("category"),
        F.col("RAW")["category_name"].as_("category_name"),
        F.col("RAW")["vendor_no"].as_("vendor_no"),
        F.col("RAW")["vendor_name"].as_("vendor_name"),
        F.col("RAW")["itemno"].as_("itemno"),
        F.col("RAW")["im_desc"].as_("im_desc"),
        F.col("RAW")["pack"].as_("pack"),
        F.col("RAW")["bottle_volume_ml"].as_("bottle_volume_ml"),
        F.col("RAW")["state_bottle_cost"].as_("state_bottle_cost"),
        F.col("RAW")["state_bottle_retail"].as_("state_bottle_retail"),
        F.col("RAW")["sale_bottles"].as_("sale_bottles"),
        F.col("RAW")["sale_dollars"].as_("sale_dollars"),
        F.col("RAW")["sale_liters"].as_("sale_liters"),
        F.col("RAW")["sale_gallons"].as_("sale_gallons"),
    )

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
    df.create_or_replace_temp_view("SRC_IOWA_STAGE")
    session.sql(f"""
        MERGE INTO {TARGET} t
        USING SRC_IOWA_STAGE s
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
    year_list = []
    if years:
        try:
            year_list = [int(y) for y in years]
        except Exception:
            year_list = []

    last_full = last_complete_month_start(date.today())
    if year_list:
        year_list = [y for y in year_list if DATASET_START_YEAR <= y <= last_full.year]

    pattern = build_stage_pattern(year_list)

    copy_sql = f"""
        COPY INTO {RAW_TABLE} (RAW, FILE_NAME)
        FROM (
          SELECT $1, METADATA$FILENAME
          FROM {STAGE_BASE}
        )
        FILE_FORMAT=(FORMAT_NAME={DB}.{SCHEMA}.IOWA_JSON_FORMAT)
        PATTERN='{pattern}'
        ON_ERROR='CONTINUE'
        FORCE=TRUE
    """
    copy_result = session.sql(copy_sql).collect()

    raw_df = session.table(RAW_TABLE)

    raw_count = raw_df.count()
    if raw_count == 0:
        return f"No rows loaded from stage; pattern='{pattern}'; copy_result={copy_result}; raw_count={raw_count}"

    prepared = transform_variant_df(raw_df)
    prepared_count = prepared.count()
    merge_target(session, prepared)
    return f"Loaded and merged rows from stage pattern '{pattern}' through {last_full}; raw_count={raw_count}; prepared_count={prepared_count}; copy_result={copy_result}"
$$;
