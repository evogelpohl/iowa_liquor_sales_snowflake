import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Iowa Liquor Sales", layout="wide")

session = get_active_session()

st.title("Iowa Liquor Sales Dashboard")
st.caption("Data from EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES (SALE_YEAR = 2025)")

query = """
SELECT
  LIQUOR_CATEGORY,
  SUM(SALE_DOLLARS) AS SALE_DOLLARS
FROM EVO_DEMO.IOWA_LIQUOR_SALES.IOWA_LIQUOR_SALES
WHERE SALE_YEAR = 2025
GROUP BY LIQUOR_CATEGORY
ORDER BY SALE_DOLLARS ASC
"""

df = session.sql(query).to_pandas()
df = df.sort_values(by="SALE_DOLLARS", ascending=True)

if df.empty:
    st.info("No data found for 2025. Load data, then refresh.")
else:
    st.bar_chart(df, x="LIQUOR_CATEGORY", y="SALE_DOLLARS")
