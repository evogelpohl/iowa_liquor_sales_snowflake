-- Apply/verify semantic view for SV_IOWA_LIQUOR_SALES
-- Run with: snow sql -f 06_cortex_ai/apply_semantic_view.sql
-- Uses verify-only first, then create/replace to copy grants.
USE DATABASE EVO_DEMO;
USE SCHEMA IOWA_LIQUOR_SALES;

USE WAREHOUSE IOWA_WH;

-- Drop existing semantic view to ensure a clean recreate
DROP SEMANTIC VIEW IF EXISTS SV_IOWA_LIQUOR_SALES;

-- Verify only (no create) to lint the YAML
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('EVO_DEMO.IOWA_LIQUOR_SALES', $$
name: SV_IOWA_LIQUOR_SALES
description: This semantic view (sv_) connects the iowa liquor sales fact table to its store and location table along with a traditional CY based date dimension joined by sale_date. The Iowa liquor sales table records sales receipts for all state liquor stores at the item level.
module_custom_instructions:
  sql_generation: |
    - When aggregating over months, years, or categories, round currency measures to 0 decimals.
    - For row-level or invoice-level outputs, round currency measures to 2 decimals.
    - Assume monthly sales are complete through the last day of each month.
tables:
  - name: DATE_DIM
    description: The table contains calendar date reference information with standard date parts and classifications. Each record represents a single date with its various temporal components, holiday designations, and common date-based calculations.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: DATE_DIM
    dimensions:
      - name: DATEKEY
        description: A date identifier represented as an eight-digit number in YYYYMMDD format.
        expr: DATEKEY
        data_type: NUMBER(38,0)
        sample_values:
          - '20120222'
          - '20120103'
          - '20120226'
      - name: DAYNUMINMONTH
        description: The numerical day within a month.
        expr: DAYNUMINMONTH
        data_type: NUMBER(38,0)
        sample_values:
          - '29'
          - '1'
          - '2'
      - name: DAYNUMINWEEK
        description: The numerical position of the day within a week starting from Sunday.
        expr: DAYNUMINWEEK
        data_type: NUMBER(38,0)
        sample_values:
          - '1'
          - '2'
          - '7'
      - name: DAYNUMINYEAR
        description: The sequential day number within a year from 1 to 366.
        expr: DAYNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: DAYOFWEEK
        description: The name of the day in the week.
        expr: DAYOFWEEK
        data_type: VARCHAR(16777216)
        sample_values:
          - Sunday
          - Monday
          - Tuesday
      - name: DAYOFWEEK_SHORT
        description: The abbreviated name of the day of the week.
        expr: DAYOFWEEK_SHORT
        data_type: VARCHAR(16777216)
        sample_values:
          - Tue
          - Sun
          - Mon
      - name: HOLIDAY_NAME_US
        description: The name of the United States federal or widely observed holiday.
        expr: HOLIDAY_NAME_US
        data_type: VARCHAR(16777216)
        sample_values:
          - New Year's Day
          - New Year's Day (observed)
      - name: IS_HOLIDAY
        description: Indicates whether the date is a recognized holiday.
        expr: IS_HOLIDAY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_HOLIDAY_US
        description: Indicates whether the date is a United States holiday.
        expr: IS_HOLIDAY_US
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_LAST_DAY_IN_MONTH
        description: Indicator of whether the date falls on the last day of its month.
        expr: IS_LAST_DAY_IN_MONTH
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_LAST_DAY_IN_WEEK
        description: Indicator of whether the date falls on the last day of the week.
        expr: IS_LAST_DAY_IN_WEEK
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_WEEKDAY
        description: Indicates whether the date falls on a weekday.
        expr: IS_WEEKDAY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: ISO_WEEKNUMINYEAR
        description: The week number within the year based on the ISO 8601 standard.
        expr: ISO_WEEKNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '29'
          - '1'
          - '2'
      - name: ISO_YEAR
        description: The year represented in International Organization for Standardization format.
        expr: ISO_YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2013'
          - '2011'
          - '2012'
      - name: MONTH
        description: The name of the month.
        expr: MONTH
        data_type: VARCHAR(16777216)
        sample_values:
          - January
          - March
          - May
      - name: MONTHNUMINYEAR
        description: The numeric representation of the month within a year.
        expr: MONTHNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '1'
          - '2'
          - '3'
      - name: MONTHYEAR
        description: Month and year expressed as a text value.
        expr: MONTHYEAR
        data_type: VARCHAR(16777216)
        sample_values:
          - Jan2012
          - Mar2012
          - Feb2012
      - name: YEAR
        description: The calendar year.
        expr: YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2013'
          - '2012'
          - '2014'
      - name: YEARMONTHNUM
        description: A six-digit number representing the year and month in YYYYMM format.
        expr: YEARMONTHNUM
        data_type: NUMBER(38,0)
        sample_values:
          - '202512'
          - '201508'
          - '201203'
    time_dimensions:
      - name: DATE
        synonyms:
          - Sale Date
        description: The calendar date and the date when the liquor sale transaction occurred as joined from iowa_liquor_sales[sale_date]
        expr: DATE
        data_type: DATE
        sample_values:
          - '2012-06-26'
          - '2012-07-13'
          - '2012-08-10'
    primary_key:
      columns:
        - DATE
  - name: DIM_STORE_LOCATION_V
    description: The table contains records of retail store locations and their geographic details. Each record represents a single store with its physical address, county information, and precise geographic coordinates.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: DIM_STORE_LOCATION_V
    dimensions:
      - name: LATITUDE
        synonyms:
          - Lat
        description: The LATITUDE of the store's location
        expr: LATITUDE
        data_type: FLOAT
      - name: LONGITUDE
        synonyms:
          - Lon
          - Long
        description: The longitude of the store's location in IA
        expr: LONGITUDE
        data_type: FLOAT
      - name: STORE_CITY
        description: City where the liquor store is located.
        expr: STORE_CITY
        data_type: VARCHAR(16777216)
        sample_values:
          - CEDAR RAPIDS
          - BROOKLYN
          - VILLISCA
      - name: STORE_COUNTY
        description: The county in Iowa where the liquor store is located.
        expr: STORE_COUNTY
        data_type: VARCHAR(16777216)
        sample_values:
          - CLINTON
          - MONTGOMERY
          - POTTAWATTAMIE
      - name: STORE_COUNTY_NUMBER
        description: Unique numeric identifier for the county where the store is located.
        expr: STORE_COUNTY_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '34'
          - '13'
          - '74'
      - name: STORE_LOCATION
        description: Physical address of the liquor store.
        expr: STORE_LOCATION
        data_type: OBJECT
        sample_values:
          - |-
            {
              "coordinates": [
                -92.72735,
                41.74124
              ],
              "type": "Point"
            }
          - |-
            {
              "coordinates": [
                -94.681939,
                43.111913
              ],
              "type": "Point"
            }
          - |-
            {
              "coordinates": [
                -91.71731,
                41.94423
              ],
              "type": "Point"
            }
      - name: STORE_NAME
        description: Name of the retail store location.
        expr: STORE_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - 'CASEY''S GENERAL STORE #2803 / VILLISCA'
          - 'KUM & GO #7701 / DES MOINES'
          - MEGA SAVER / CEDAR RAPIDS
      - name: STORE_NUMBER
        description: Unique identifier assigned to each liquor store location.
        expr: STORE_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '5970'
          - '10658'
          - '5864'
      - name: STORE_STATE
        description: Two-letter United States state code where the store is located.
        expr: STORE_STATE
        data_type: VARCHAR(2)
        sample_values:
          - IA
      - name: STORE_STREET_ADDRESS
        description: Physical street address of the retail liquor store.
        expr: STORE_STREET_ADDRESS
        data_type: VARCHAR(16777216)
        sample_values:
          - 2604 16TH AVE SW
          - 309 N U AVE
          - 319 7TH ST  STE 101
      - name: STORE_ZIP
        description: ZIP code of the store location.
        expr: STORE_ZIP
        data_type: VARCHAR(16777216)
        sample_values:
          - '52404'
          - '50112'
          - '50536'
    primary_key:
      columns:
        - STORE_NUMBER
  - name: IOWA_LIQUOR_SALES
    description: The table contains records of liquor sales transactions from retail stores across Iowa. Each record represents an individual sale and includes details about the store location, product information, vendor details, and temporal sale data.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: IOWA_LIQUOR_SALES
    dimensions:
      - name: BOTTLE_VOLUME_ML
        description: DNSThe volume of liquor in each bottle measured in milliliters.
        expr: BOTTLE_VOLUME_ML
        data_type: NUMBER(38,0)
        sample_values:
          - '750'
          - '1000'
          - '1750'
      - name: CATEGORY
        description: Product category identification number for liquor classification.
        expr: CATEGORY
        data_type: NUMBER(38,0)
        sample_values:
          - '1031100'
          - '1092100'
          - '1031200'
      - name: CATEGORY_NAME
        synonyms:
          - Category Name
          - Iowa Category
          - State Category
        description: Iowa-provided mid-level category name for the product.
        expr: CATEGORY_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - AMERICAN CORDIALS & LIQUEURS
          - NEUTRAL GRAIN SPIRITS
          - IMPORTED VODKAS
      - name: INVOICE_AND_ITEM_NUMBER
        description: A unique identifier combining the invoice number and line item number for each liquor sale transaction.
        expr: INVOICE_AND_ITEM_NUMBER
        data_type: VARCHAR(16777216)
        sample_values:
          - INV-82354200019
          - INV-82354000050
          - INV-68903900049
      - name: ITEM
        synonyms:
          - Item Description
          - Product
          - Product Name
        description: The brand name and description of the alcoholic beverage product.
        expr: ITEM_DESCRIPTION
        data_type: VARCHAR(16777216)
        sample_values:
          - JUAREZ SILVER
          - MILAGRO SILVER
          - 1800 SILVER
      - name: ITEM_NUMBER
        description: A unique identifier for each liquor product in the inventory.
        expr: ITEM_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '37663'
          - '36969'
          - '26826'
      - name: LIQUOR_FAMILY
        synonyms:
          - Liquor Family
          - Liquor Category
          - Normalized Category
        description: Normalized high-level liquor family rollup derived from category mappings.
        expr: LIQUOR_CATEGORY
        data_type: VARCHAR(16777216)
        sample_values:
          - TEQUILAS
          - SPECIALTY SPIRITS
          - WHISKIES
      - name: PACK
        description: The number of bottles contained in a single unit of product.
        expr: PACK
        data_type: NUMBER(38,0)
        sample_values:
          - '12'
          - '6'
          - '10'
      - name: STORE_NUMBER
        description: A unique identifier assigned to each retail store that sells liquor.
        expr: STORE_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '4296'
          - '3859'
          - '10389'
      - name: VENDOR_NAME
        description: The name of the company or business entity that supplies alcoholic beverages.
        expr: VENDOR_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - MISSISSIPPI RIVER DISTILLING COMPANY LLC
          - CAMPARI AMERICA
          - E & J GALLO WINERY
      - name: VENDOR_NUMBER
        description: A unique identifier assigned to each liquor vendor.
        expr: VENDOR_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '259'
          - '322'
          - '65'
    time_dimensions:
      - name: SALE_DATE
        description: The date when the liquor sale transaction occurred.
        expr: SALE_DATE
        data_type: DATE
        sample_values:
          - '2025-07-29'
          - '2025-07-31'
          - '2025-07-30'
      - name: SALE_MONTH
        synonyms:
          - Sale Month
        description: Calendar month number of the sale date.
        expr: SALE_MONTH
        data_type: NUMBER(38,0)
        sample_values:
          - '6'
          - '5'
          - '12'
      - name: SALE_YEAR
        synonyms:
          - Sale Year
        description: Calendar year of the sale date.
        expr: SALE_YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2025'
          - '2024'
          - '2023'
    facts:
      - name: BOTTLES_SOLD
        description: The quantity of liquor bottles sold in a single transaction.
        expr: BOTTLES_SOLD
        data_type: NUMBER(38,0)
        access_modifier: public_access
        sample_values:
          - '2'
          - '96'
          - '6'
      - name: SALE_DOLLARS
        description: The total dollar amount of the sale.
        expr: SALE_DOLLARS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '32'
          - '364.92'
          - '112.56'
      - name: STATE_BOTTLE_COST
        description: The wholesale cost per bottle charged by the state to retailers.
        expr: STATE_BOTTLE_COST
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '40'
          - '7'
          - '15'
      - name: STATE_BOTTLE_RETAIL
        description: The retail price of a bottle of liquor at the state level.
        expr: STATE_BOTTLE_RETAIL
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '9'
          - '6.54'
          - '11.45'
      - name: VOLUME_SOLD_GALLONS
        description: The volume of liquor sold measured in gallons.
        expr: VOLUME_SOLD_GALLONS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '0.21'
          - '0.59'
          - '0.19'
      - name: VOLUME_SOLD_LITERS
        description: The volume of liquor sold in liters.
        expr: VOLUME_SOLD_LITERS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '14.4'
          - '1.5'
          - '2'
    metrics:
      - name: TOTAL_SALES
        synonyms:
          - Total Sales
          - Sales
          - Revenue
        description: Total sales dollars across all transactions.
        expr: SUM(SALE_DOLLARS)
      - name: TOTAL_BOTTLES_SOLD
        synonyms:
          - Total Bottles Sold
          - Bottles Sold
          - Total Quantity
        description: Total number of bottles sold.
        expr: SUM(BOTTLES_SOLD)
      - name: TOTAL_VOLUME_LITERS
        synonyms:
          - Total Volume Liters
          - Volume Sold Liters
        description: Total volume sold in liters.
        expr: SUM(VOLUME_SOLD_LITERS)
      - name: TOTAL_VOLUME_GALLONS
        synonyms:
          - Total Volume Gallons
          - Volume Sold Gallons
        description: Total volume sold in gallons.
        expr: SUM(VOLUME_SOLD_GALLONS)
      - name: AVG_PRICE_PER_BOTTLE
        description: Average sale dollars per bottle sold.
        expr: SUM(SALE_DOLLARS) / NULLIF(SUM(BOTTLES_SOLD), 0)
      - name: AVG_UNIT_COST
        description: Average state bottle cost per bottle sold.
        expr: SUM(STATE_BOTTLE_COST * BOTTLES_SOLD) / NULLIF(SUM(BOTTLES_SOLD), 0)
      - name: GROSS_MARGIN_DOLLARS
        description: Sales dollars minus state bottle costs for sold bottles.
        expr: SUM(SALE_DOLLARS - (STATE_BOTTLE_COST * BOTTLES_SOLD))
      - name: GROSS_MARGIN_RATE
        description: Gross margin rate based on sales dollars.
        expr: (SUM(SALE_DOLLARS - (STATE_BOTTLE_COST * BOTTLES_SOLD)) / NULLIF(SUM(SALE_DOLLARS), 0))
    primary_key:
      columns:
        - INVOICE_AND_ITEM_NUMBER
relationships:
  - name: IOWA_LIQUOR_SALES_TO_DATE_DIM
    left_table: IOWA_LIQUOR_SALES
    right_table: DATE_DIM
    relationship_columns:
      - left_column: SALE_DATE
        right_column: DATE
  - name: IOWA_LIQUOR_SALES_TO_DIM_STORE_LOCATION_V
    left_table: IOWA_LIQUOR_SALES
    right_table: DIM_STORE_LOCATION_V
    relationship_columns:
      - left_column: STORE_NUMBER
        right_column: STORE_NUMBER
    relationship_type: many_to_one
    join_type: inner

# Snowflake verified query fields allowed: name, question, sql, verifiedAt, verifiedBy, useAsOnboardingQuestion, semanticModelName
verified_queries:
  - name: TTM_SALES
    question: What is our trailing twelve month (TTM) sales?
    useAsOnboardingQuestion: false
    sql: |
      WITH max_month AS (
          SELECT DATE_TRUNC('MONTH', MAX(SALE_DATE)) AS max_sale_month
          FROM IOWA_LIQUOR_SALES
      )
      SELECT TO_VARCHAR(SUM(SALE_DOLLARS), '$999,999,999,999') AS TTM_SALES
      FROM IOWA_LIQUOR_SALES
      CROSS JOIN max_month
      WHERE SALE_DATE >= DATEADD(MONTH, -11, max_month.max_sale_month)
        AND SALE_DATE <  DATEADD(MONTH,  1, max_month.max_sale_month)
  - name: TOP_TTM_ITEMS
    question: What are the top N best selling items for the past TTM?
    useAsOnboardingQuestion: false
    sql: |
      WITH max_month AS (
        SELECT DATE_TRUNC('MONTH', MAX(SALE_DATE)) AS max_sale_month
        FROM IOWA_LIQUOR_SALES
      ),
      ttm_sales AS (
        SELECT
            INITCAP(ITEM) AS item,
            SUM(SALE_DOLLARS)        AS ttm_sales
        FROM IOWA_LIQUOR_SALES
        CROSS JOIN max_month
        WHERE SALE_DATE >= DATEADD(MONTH, -11, max_month.max_sale_month)
          AND SALE_DATE <  DATEADD(MONTH,  1, max_month.max_sale_month)
        GROUP BY 1
      )
      SELECT
          item AS "Item",
          ROUND(ttm_sales, 0) AS "TTM Sales"
      FROM ttm_sales
      ORDER BY "TTM Sales" DESC
  - name: MONTHLY_SALES_WITH_TTM_AVG
    question: Show monthly sales with a trailing 12-month average for the last 36 months.
    useAsOnboardingQuestion: false
    sql: |
      WITH params AS (
        SELECT
          DATEADD(MONTH, -36, DATE_TRUNC('MONTH', CURRENT_DATE)) AS visible_start,
          DATE_TRUNC('MONTH', CURRENT_DATE) AS visible_end
      ),
      bounds AS (
        SELECT
          DATEADD(MONTH, -11, (SELECT visible_start FROM params)) AS scan_start,
          (SELECT visible_end FROM params) AS scan_end
      ),
      monthly AS (
        SELECT
            DATE_TRUNC('MONTH', sale_date) AS sale_month,
            SUM(sale_dollars) AS monthly_sales
        FROM IOWA_LIQUOR_SALES
        WHERE sale_date >= (SELECT scan_start FROM bounds)
          AND sale_date <  DATEADD(MONTH, 1, (SELECT scan_end FROM bounds))
        GROUP BY 1
      ),
      calc AS (
        SELECT
            sale_month,
            monthly_sales,
            AVG(monthly_sales) OVER (
              ORDER BY sale_month
              ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ttm_avg_sales
        FROM monthly
      )
      SELECT
          sale_month AS "Sale Month",
          ROUND(monthly_sales, 0) AS "Monthly Sales",
          ROUND(ttm_avg_sales, 0) AS "TTM Avg Sales"
      FROM calc
      WHERE sale_month BETWEEN (SELECT visible_start FROM params)
                          AND (SELECT visible_end FROM params)
      ORDER BY sale_month
$$, TRUE);

-- Create or replace the semantic view (copies grants from prior version)
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('EVO_DEMO.IOWA_LIQUOR_SALES', $$
name: SV_IOWA_LIQUOR_SALES
description: This semantic view (sv_) connects the iowa liquor sales fact table to its store and location table along with a traditional CY based date dimension joined by sale_date. The Iowa liquor sales table records sales receipts for all state liquor stores at the item level.
module_custom_instructions:
  sql_generation: |
    - When aggregating over months, years, or categories, round currency measures to 0 decimals.
    - For row-level or invoice-level outputs, round currency measures to 2 decimals.
    - Assume monthly sales are complete through the last day of each month.
tables:
  - name: DATE_DIM
    description: The table contains calendar date reference information with standard date parts and classifications. Each record represents a single date with its various temporal components, holiday designations, and common date-based calculations.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: DATE_DIM
    dimensions:
      - name: DATEKEY
        description: A date identifier represented as an eight-digit number in YYYYMMDD format.
        expr: DATEKEY
        data_type: NUMBER(38,0)
        sample_values:
          - '20120222'
          - '20120103'
          - '20120226'
      - name: DAYNUMINMONTH
        description: The numerical day within a month.
        expr: DAYNUMINMONTH
        data_type: NUMBER(38,0)
        sample_values:
          - '29'
          - '1'
          - '2'
      - name: DAYNUMINWEEK
        description: The numerical position of the day within a week starting from Sunday.
        expr: DAYNUMINWEEK
        data_type: NUMBER(38,0)
        sample_values:
          - '1'
          - '2'
          - '7'
      - name: DAYNUMINYEAR
        description: The sequential day number within a year from 1 to 366.
        expr: DAYNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '98'
          - '29'
          - '1'
      - name: DAYOFWEEK
        description: The name of the day in the week.
        expr: DAYOFWEEK
        data_type: VARCHAR(16777216)
        sample_values:
          - Sunday
          - Monday
          - Tuesday
      - name: DAYOFWEEK_SHORT
        description: The abbreviated name of the day of the week.
        expr: DAYOFWEEK_SHORT
        data_type: VARCHAR(16777216)
        sample_values:
          - Tue
          - Sun
          - Mon
      - name: HOLIDAY_NAME_US
        description: The name of the United States federal or widely observed holiday.
        expr: HOLIDAY_NAME_US
        data_type: VARCHAR(16777216)
        sample_values:
          - New Year's Day
          - New Year's Day (observed)
      - name: IS_HOLIDAY
        description: Indicates whether the date is a recognized holiday.
        expr: IS_HOLIDAY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_HOLIDAY_US
        description: Indicates whether the date is a United States holiday.
        expr: IS_HOLIDAY_US
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_LAST_DAY_IN_MONTH
        description: Indicator of whether the date falls on the last day of its month.
        expr: IS_LAST_DAY_IN_MONTH
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_LAST_DAY_IN_WEEK
        description: Indicator of whether the date falls on the last day of the week.
        expr: IS_LAST_DAY_IN_WEEK
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_WEEKDAY
        description: Indicates whether the date falls on a weekday.
        expr: IS_WEEKDAY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: ISO_WEEKNUMINYEAR
        description: The week number within the year based on the ISO 8601 standard.
        expr: ISO_WEEKNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '29'
          - '1'
          - '2'
      - name: ISO_YEAR
        description: The year represented in International Organization for Standardization format.
        expr: ISO_YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2013'
          - '2011'
          - '2012'
      - name: MONTH
        description: The name of the month.
        expr: MONTH
        data_type: VARCHAR(16777216)
        sample_values:
          - January
          - March
          - May
      - name: MONTHNUMINYEAR
        description: The numeric representation of the month within a year.
        expr: MONTHNUMINYEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '1'
          - '2'
          - '3'
      - name: MONTHYEAR
        description: Month and year expressed as a text value.
        expr: MONTHYEAR
        data_type: VARCHAR(16777216)
        sample_values:
          - Jan2012
          - Mar2012
          - Feb2012
      - name: YEAR
        description: The calendar year.
        expr: YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2013'
          - '2012'
          - '2014'
      - name: YEARMONTHNUM
        description: A six-digit number representing the year and month in YYYYMM format.
        expr: YEARMONTHNUM
        data_type: NUMBER(38,0)
        sample_values:
          - '202512'
          - '201508'
          - '201203'
    time_dimensions:
      - name: DATE
        synonyms:
          - Sale Date
        description: The calendar date and the date when the liquor sale transaction occurred as joined from iowa_liquor_sales[sale_date]
        expr: DATE
        data_type: DATE
        sample_values:
          - '2012-06-26'
          - '2012-07-13'
          - '2012-08-10'
    primary_key:
      columns:
        - DATE
  - name: DIM_STORE_LOCATION_V
    description: The table contains records of retail store locations and their geographic details. Each record represents a single store with its physical address, county information, and precise geographic coordinates.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: DIM_STORE_LOCATION_V
    dimensions:
      - name: LATITUDE
        synonyms:
          - Lat
        description: The LATITUDE of the store's location
        expr: LATITUDE
        data_type: FLOAT
      - name: LONGITUDE
        synonyms:
          - Lon
          - Long
        description: The longitude of the store's location in IA
        expr: LONGITUDE
        data_type: FLOAT
      - name: STORE_CITY
        description: City where the liquor store is located.
        expr: STORE_CITY
        data_type: VARCHAR(16777216)
        sample_values:
          - CEDAR RAPIDS
          - BROOKLYN
          - VILLISCA
      - name: STORE_COUNTY
        description: The county in Iowa where the liquor store is located.
        expr: STORE_COUNTY
        data_type: VARCHAR(16777216)
        sample_values:
          - CLINTON
          - MONTGOMERY
          - POTTAWATTAMIE
      - name: STORE_COUNTY_NUMBER
        description: Unique numeric identifier for the county where the store is located.
        expr: STORE_COUNTY_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '34'
          - '13'
          - '74'
      - name: STORE_LOCATION
        description: Physical address of the liquor store.
        expr: STORE_LOCATION
        data_type: OBJECT
        sample_values:
          - |-
            {
              "coordinates": [
                -92.72735,
                41.74124
              ],
              "type": "Point"
            }
          - |-
            {
              "coordinates": [
                -94.681939,
                43.111913
              ],
              "type": "Point"
            }
          - |-
            {
              "coordinates": [
                -91.71731,
                41.94423
              ],
              "type": "Point"
            }
      - name: STORE_NAME
        description: Name of the retail store location.
        expr: STORE_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - 'CASEY''S GENERAL STORE #2803 / VILLISCA'
          - 'KUM & GO #7701 / DES MOINES'
          - MEGA SAVER / CEDAR RAPIDS
      - name: STORE_NUMBER
        description: Unique identifier assigned to each liquor store location.
        expr: STORE_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '5970'
          - '10658'
          - '5864'
      - name: STORE_STATE
        description: Two-letter United States state code where the store is located.
        expr: STORE_STATE
        data_type: VARCHAR(2)
        sample_values:
          - IA
      - name: STORE_STREET_ADDRESS
        description: Physical street address of the retail liquor store.
        expr: STORE_STREET_ADDRESS
        data_type: VARCHAR(16777216)
        sample_values:
          - 2604 16TH AVE SW
          - 309 N U AVE
          - 319 7TH ST  STE 101
      - name: STORE_ZIP
        description: ZIP code of the store location.
        expr: STORE_ZIP
        data_type: VARCHAR(16777216)
        sample_values:
          - '52404'
          - '50112'
          - '50536'
    primary_key:
      columns:
        - STORE_NUMBER
  - name: IOWA_LIQUOR_SALES
    description: The table contains records of liquor sales transactions from retail stores across Iowa. Each record represents an individual sale and includes details about the store location, product information, vendor details, and temporal sale data.
    base_table:
      database: EVO_DEMO
      schema: IOWA_LIQUOR_SALES
      table: IOWA_LIQUOR_SALES
    dimensions:
      - name: BOTTLE_VOLUME_ML
        description: DNSThe volume of liquor in each bottle measured in milliliters.
        expr: BOTTLE_VOLUME_ML
        data_type: NUMBER(38,0)
        sample_values:
          - '750'
          - '1000'
          - '1750'
      - name: CATEGORY
        description: Product category identification number for liquor classification.
        expr: CATEGORY
        data_type: NUMBER(38,0)
        sample_values:
          - '1031100'
          - '1092100'
          - '1031200'
      - name: CATEGORY_NAME
        synonyms:
          - Category Name
          - Iowa Category
          - State Category
        description: Iowa-provided mid-level category name for the product.
        expr: CATEGORY_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - AMERICAN CORDIALS & LIQUEURS
          - NEUTRAL GRAIN SPIRITS
          - IMPORTED VODKAS
      - name: INVOICE_AND_ITEM_NUMBER
        description: A unique identifier combining the invoice number and line item number for each liquor sale transaction.
        expr: INVOICE_AND_ITEM_NUMBER
        data_type: VARCHAR(16777216)
        sample_values:
          - INV-82354200019
          - INV-82354000050
          - INV-68903900049
      - name: ITEM
        synonyms:
          - Item Description
          - Product
          - Product Name
        description: The brand name and description of the alcoholic beverage product.
        expr: ITEM_DESCRIPTION
        data_type: VARCHAR(16777216)
        sample_values:
          - JUAREZ SILVER
          - MILAGRO SILVER
          - 1800 SILVER
      - name: ITEM_NUMBER
        description: A unique identifier for each liquor product in the inventory.
        expr: ITEM_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '37663'
          - '36969'
          - '26826'
      - name: LIQUOR_FAMILY
        synonyms:
          - Liquor Family
          - Liquor Category
          - Normalized Category
        description: Normalized high-level liquor family rollup derived from category mappings.
        expr: LIQUOR_CATEGORY
        data_type: VARCHAR(16777216)
        sample_values:
          - TEQUILAS
          - SPECIALTY SPIRITS
          - WHISKIES
      - name: PACK
        description: The number of bottles contained in a single unit of product.
        expr: PACK
        data_type: NUMBER(38,0)
        sample_values:
          - '12'
          - '6'
          - '10'
      - name: STORE_NUMBER
        description: A unique identifier assigned to each retail store that sells liquor.
        expr: STORE_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '4296'
          - '3859'
          - '10389'
      - name: VENDOR_NAME
        description: The name of the company or business entity that supplies alcoholic beverages.
        expr: VENDOR_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - MISSISSIPPI RIVER DISTILLING COMPANY LLC
          - CAMPARI AMERICA
          - E & J GALLO WINERY
      - name: VENDOR_NUMBER
        description: A unique identifier assigned to each liquor vendor.
        expr: VENDOR_NUMBER
        data_type: NUMBER(38,0)
        sample_values:
          - '259'
          - '322'
          - '65'
    time_dimensions:
      - name: SALE_DATE
        description: The date when the liquor sale transaction occurred.
        expr: SALE_DATE
        data_type: DATE
        sample_values:
          - '2025-07-29'
          - '2025-07-31'
          - '2025-07-30'
      - name: SALE_MONTH
        synonyms:
          - Sale Month
        description: Calendar month number of the sale date.
        expr: SALE_MONTH
        data_type: NUMBER(38,0)
        sample_values:
          - '6'
          - '5'
          - '12'
      - name: SALE_YEAR
        synonyms:
          - Sale Year
        description: Calendar year of the sale date.
        expr: SALE_YEAR
        data_type: NUMBER(38,0)
        sample_values:
          - '2025'
          - '2024'
          - '2023'
    facts:
      - name: BOTTLES_SOLD
        description: The quantity of liquor bottles sold in a single transaction.
        expr: BOTTLES_SOLD
        data_type: NUMBER(38,0)
        access_modifier: public_access
        sample_values:
          - '2'
          - '96'
          - '6'
      - name: SALE_DOLLARS
        description: The total dollar amount of the sale.
        expr: SALE_DOLLARS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '32'
          - '364.92'
          - '112.56'
      - name: STATE_BOTTLE_COST
        description: The wholesale cost per bottle charged by the state to retailers.
        expr: STATE_BOTTLE_COST
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '40'
          - '7'
          - '15'
      - name: STATE_BOTTLE_RETAIL
        description: The retail price of a bottle of liquor at the state level.
        expr: STATE_BOTTLE_RETAIL
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '9'
          - '6.54'
          - '11.45'
      - name: VOLUME_SOLD_GALLONS
        description: The volume of liquor sold measured in gallons.
        expr: VOLUME_SOLD_GALLONS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '0.21'
          - '0.59'
          - '0.19'
      - name: VOLUME_SOLD_LITERS
        description: The volume of liquor sold in liters.
        expr: VOLUME_SOLD_LITERS
        data_type: FLOAT
        access_modifier: public_access
        sample_values:
          - '14.4'
          - '1.5'
          - '2'
    metrics:
      - name: TOTAL_SALES
        synonyms:
          - Total Sales
          - Sales
          - Revenue
        description: Total sales dollars across all transactions.
        expr: SUM(SALE_DOLLARS)
      - name: TOTAL_BOTTLES_SOLD
        synonyms:
          - Total Bottles Sold
          - Bottles Sold
          - Total Quantity
        description: Total number of bottles sold.
        expr: SUM(BOTTLES_SOLD)
      - name: TOTAL_VOLUME_LITERS
        synonyms:
          - Total Volume Liters
          - Volume Sold Liters
        description: Total volume sold in liters.
        expr: SUM(VOLUME_SOLD_LITERS)
      - name: TOTAL_VOLUME_GALLONS
        synonyms:
          - Total Volume Gallons
          - Volume Sold Gallons
        description: Total volume sold in gallons.
        expr: SUM(VOLUME_SOLD_GALLONS)
      - name: AVG_PRICE_PER_BOTTLE
        description: Average sale dollars per bottle sold.
        expr: SUM(SALE_DOLLARS) / NULLIF(SUM(BOTTLES_SOLD), 0)
      - name: AVG_UNIT_COST
        description: Average state bottle cost per bottle sold.
        expr: SUM(STATE_BOTTLE_COST * BOTTLES_SOLD) / NULLIF(SUM(BOTTLES_SOLD), 0)
      - name: GROSS_MARGIN_DOLLARS
        description: Sales dollars minus state bottle costs for sold bottles.
        expr: SUM(SALE_DOLLARS - (STATE_BOTTLE_COST * BOTTLES_SOLD))
      - name: GROSS_MARGIN_RATE
        description: Gross margin rate based on sales dollars.
        expr: (SUM(SALE_DOLLARS - (STATE_BOTTLE_COST * BOTTLES_SOLD)) / NULLIF(SUM(SALE_DOLLARS), 0))
    primary_key:
      columns:
        - INVOICE_AND_ITEM_NUMBER
relationships:
  - name: IOWA_LIQUOR_SALES_TO_DATE_DIM
    left_table: IOWA_LIQUOR_SALES
    right_table: DATE_DIM
    relationship_columns:
      - left_column: SALE_DATE
        right_column: DATE
  - name: IOWA_LIQUOR_SALES_TO_DIM_STORE_LOCATION_V
    left_table: IOWA_LIQUOR_SALES
    right_table: DIM_STORE_LOCATION_V
    relationship_columns:
      - left_column: STORE_NUMBER
        right_column: STORE_NUMBER
    relationship_type: many_to_one
    join_type: inner

# Snowflake verified query fields allowed: name, question, sql, verifiedAt, verifiedBy, useAsOnboardingQuestion, semanticModelName
verified_queries:
  - name: TTM_SALES
    question: What is our trailing twelve month (TTM) sales?
    useAsOnboardingQuestion: false
    sql: |
      WITH max_month AS (
          SELECT DATE_TRUNC('MONTH', MAX(SALE_DATE)) AS max_sale_month
          FROM IOWA_LIQUOR_SALES
      )
      SELECT TO_VARCHAR(SUM(SALE_DOLLARS), '$999,999,999,999') AS TTM_SALES
      FROM IOWA_LIQUOR_SALES
      CROSS JOIN max_month
      WHERE SALE_DATE >= DATEADD(MONTH, -11, max_month.max_sale_month)
        AND SALE_DATE <  DATEADD(MONTH,  1, max_month.max_sale_month)
  - name: TOP_TTM_ITEMS
    question: What are the top N best selling items for the past TTM?
    useAsOnboardingQuestion: false
    sql: |
      WITH max_month AS (
        SELECT DATE_TRUNC('MONTH', MAX(SALE_DATE)) AS max_sale_month
        FROM IOWA_LIQUOR_SALES
      ),
      ttm_sales AS (
        SELECT
            INITCAP(ITEM) AS item,
            SUM(SALE_DOLLARS)        AS ttm_sales
        FROM IOWA_LIQUOR_SALES
        CROSS JOIN max_month
        WHERE SALE_DATE >= DATEADD(MONTH, -11, max_month.max_sale_month)
          AND SALE_DATE <  DATEADD(MONTH,  1, max_month.max_sale_month)
        GROUP BY 1
      )
      SELECT
          item AS "Item",
          ROUND(ttm_sales, 0) AS "TTM Sales"
      FROM ttm_sales
      ORDER BY "TTM Sales" DESC
  - name: MONTHLY_SALES_WITH_TTM_AVG
    question: Show monthly sales with a trailing 12-month average for the last 36 months.
    useAsOnboardingQuestion: false
    sql: |
      WITH params AS (
        SELECT
          DATEADD(MONTH, -36, DATE_TRUNC('MONTH', CURRENT_DATE)) AS visible_start,
          DATE_TRUNC('MONTH', CURRENT_DATE) AS visible_end
      ),
      bounds AS (
        SELECT
          DATEADD(MONTH, -11, (SELECT visible_start FROM params)) AS scan_start,
          (SELECT visible_end FROM params) AS scan_end
      ),
      monthly AS (
        SELECT
            DATE_TRUNC('MONTH', sale_date) AS sale_month,
            SUM(sale_dollars) AS monthly_sales
        FROM IOWA_LIQUOR_SALES
        WHERE sale_date >= (SELECT scan_start FROM bounds)
          AND sale_date <  DATEADD(MONTH, 1, (SELECT scan_end FROM bounds))
        GROUP BY 1
      ),
      calc AS (
        SELECT
            sale_month,
            monthly_sales,
            AVG(monthly_sales) OVER (
              ORDER BY sale_month
              ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ttm_avg_sales
        FROM monthly
      )
      SELECT
          sale_month AS "Sale Month",
          ROUND(monthly_sales, 0) AS "Monthly Sales",
          ROUND(ttm_avg_sales, 0) AS "TTM Avg Sales"
      FROM calc
      WHERE sale_month BETWEEN (SELECT visible_start FROM params)
                          AND (SELECT visible_end FROM params)
      ORDER BY sale_month
$$);

-- Demo-friendly grants: allow PUBLIC to see the semantic view (adjust role as needed)
GRANT USAGE ON DATABASE EVO_DEMO TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA IOWA_LIQUOR_SALES TO ROLE PUBLIC;
GRANT SELECT ON SEMANTIC VIEW SV_IOWA_LIQUOR_SALES TO ROLE PUBLIC;
