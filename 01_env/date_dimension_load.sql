USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;
USE WAREHOUSE IOWA_WH;

-- Create the date dimension table to load the staged CSV at:
-- @"EVO_DEMO"."IOWA_LIQUOR_SALES"."RAW_STAGE"/date_dim/datedimension.csv
CREATE OR REPLACE TABLE DATE_DIM (
  datekey INT,
  date DATE,
  dayofweek STRING,
  dayofweek_short STRING,
  month STRING,
  year INT,
  yearmonthnum INT,
  monthyear STRING,
  daynuminweek INT,
  daynuminmonth INT,
  daynuminyear INT,
  monthnuminyear INT,
  iso_year INT,
  iso_weeknuminyear INT,
  is_last_day_in_week BOOLEAN,
  is_last_day_in_month BOOLEAN,
  is_holiday BOOLEAN,
  is_weekday BOOLEAN,
  is_holiday_us BOOLEAN,
  holiday_name_us STRING
);

-- Load the staged CSV into DATE_DIM. Adjust FILE_FORMAT if needed.
COPY INTO DATE_DIM
FROM @RAW_STAGE/date_dim/datedimension.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
