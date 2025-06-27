import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# ─── PAGE CONFIG (must be first Streamlit command) ─────────────────────────────────
st.set_page_config(page_title="Inventory Dashboard", layout="wide")

# ─── OPTIONAL AUTO-REFRESH EVERY 60s ───────────────────────────────────────────────
st.markdown(
    '<meta http-equiv="refresh" content="60">',
    unsafe_allow_html=True
)

# ─── PAGE TITLE ─────────────────────────────────────────────────────────────────────
st.title("📊 Real-Time Inventory Dashboard")

# ─── DATABASE CONNECTION ─────────────────────────────────────────────────────────────
engine = create_engine(
    "postgresql+psycopg2://postgres:Aaditya%40123@localhost:5432/inventory"
)

# ─── SIDEBAR FILTER ─────────────────────────────────────────────────────────────────
dates = pd.read_sql("SELECT DISTINCT date FROM fact_inventory ORDER BY date DESC", engine)
selected_date = st.sidebar.selectbox("Select date", dates["date"])

# ─── DATA FETCH (cached for 60s) ────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch(q):
    return pd.read_sql(q, engine)

# ─── 1: Stock Levels by Store & Region ───────────────────────────────────────────────
q_stock_store = f"""
SELECT ds.store_id AS store,
       SUM(fi.inventory_level) AS total_stock
  FROM fact_inventory fi
  JOIN dim_store ds USING (store_key)
 WHERE fi.date = '{selected_date}'
 GROUP BY ds.store_id
 ORDER BY ds.store_id;
"""
df_stock_store = fetch(q_stock_store)

q_stock_region = f"""
SELECT ds.region AS region,
       SUM(fi.inventory_level) AS total_stock
  FROM fact_inventory fi
  JOIN dim_store ds USING (store_key)
 WHERE fi.date = '{selected_date}'
 GROUP BY ds.region
 ORDER BY ds.region;
"""
df_stock_region = fetch(q_stock_region)

# ─── 2: Low-Inventory Detection ─────────────────────────────────────────────────────
q_alerts = f"""
WITH avg_sales AS (
  SELECT product_key, store_key,
         AVG(units_sold) AS avg_daily_sales
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '30 days'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
),
reorder_calc AS (
  SELECT product_key, store_key,
         CEIL(avg_daily_sales * 7) AS reorder_point
    FROM avg_sales
),
latest_stock AS (
  SELECT product_key, store_key, inventory_level
    FROM fact_inventory
   WHERE date = '{selected_date}'
)
SELECT ds.store_id,
       dp.product_id AS sku,
       ls.inventory_level,
       rc.reorder_point
  FROM latest_stock ls
  JOIN reorder_calc rc USING (product_key, store_key)
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 WHERE ls.inventory_level < rc.reorder_point
 ORDER BY ds.store_id, dp.product_id;
"""
df_alerts = fetch(q_alerts)

# ─── 3: Reorder Point Estimation ───────────────────────────────────────────────────
q_reorder = f"""
WITH avg_sales AS (
  SELECT product_key, store_key,
         AVG(units_sold) AS avg_daily_sales
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '30 days'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
)
SELECT ds.store_id,
       dp.product_id AS sku,
       CEIL(avg_daily_sales * 7) AS reorder_point
  FROM avg_sales
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 ORDER BY ds.store_id, dp.product_id;
"""
df_reorder = fetch(q_reorder)

# ─── 4: Inventory Turnover (12mo) ─────────────────────────────────────────────────
q_turnover = f"""
WITH period_data AS (
  SELECT product_key, store_key,
         SUM(units_sold) AS total_sold,
         AVG(inventory_level) AS avg_inventory
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '1 year'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
)
SELECT ds.store_id,
       dp.product_id AS sku,
       ROUND(pd.total_sold::numeric / NULLIF(pd.avg_inventory,0), 2) AS turnover_ratio
  FROM period_data pd
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 ORDER BY ds.store_id, dp.product_id;
"""
df_turnover = fetch(q_turnover)

# ─── 5: Days of Inventory Outstanding (DIO) ───────────────────────────────────────
q_dio = f"""
WITH avg_data AS (
  SELECT product_key, store_key,
         AVG(units_sold) AS avg_daily_sales,
         AVG(inventory_level) AS avg_inventory
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '30 days'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
)
SELECT ds.store_id,
       dp.product_id AS sku,
       ROUND(avg_inventory / NULLIF(avg_daily_sales,0), 1) AS days_of_inventory
  FROM avg_data ad
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 ORDER BY ds.store_id, dp.product_id;
"""
df_dio = fetch(q_dio)

# ─── 6: ABC Classification (Fast vs Slow Moving) ─────────────────────────────────
q_abc = f"""
WITH yearly_sales AS (
  SELECT product_key, store_key,
         SUM(units_sold) AS total_sold
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '1 year'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
),
ranked AS (
  SELECT *,
         SUM(total_sold) OVER (PARTITION BY store_key ORDER BY total_sold DESC)
         / SUM(total_sold) OVER (PARTITION BY store_key) AS cum_pct
    FROM yearly_sales
)
SELECT ds.store_id,
       dp.product_id AS sku,
       CASE
         WHEN cum_pct <= 0.8  THEN 'A (Fast)'
         WHEN cum_pct <= 0.95 THEN 'B (Medium)'
         ELSE 'C (Slow)'
       END AS category,
       total_sold
  FROM ranked yr
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 ORDER BY ds.store_id, category, total_sold DESC;
"""
df_abc = fetch(q_abc)

# ─── 7: Stock Adjustment Recommendations ─────────────────────────────────────────
q_adjust = f"""
WITH avg_sales AS (
  SELECT product_key, store_key,
         AVG(units_sold) AS avg_daily_sales
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '30 days'
                  AND '{selected_date}'::date
   GROUP BY product_key, store_key
),
reorder_calc AS (
  SELECT product_key, store_key,
         CEIL(avg_daily_sales * 7) AS reorder_point
    FROM avg_sales
),
latest_stock AS (
  SELECT product_key, store_key, inventory_level
    FROM fact_inventory
   WHERE date = '{selected_date}'
)
SELECT ds.store_id,
       dp.product_id AS sku,
       ls.inventory_level,
       rc.reorder_point,
       (rc.reorder_point - ls.inventory_level) AS to_order
  FROM latest_stock ls
  JOIN reorder_calc rc USING (product_key, store_key)
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 ORDER BY to_order DESC;
"""
df_adjust = fetch(q_adjust)

# ─── 8: Stock-Out Rate (%) ───────────────────────────────────────────────────────
q_stockout = f"""
SELECT ds.store_id,
       dp.product_id AS sku,
       ROUND(
         100.0 * SUM(CASE WHEN fi.inventory_level = 0 THEN 1 ELSE 0 END)
         / NULLIF(COUNT(*),0)
       , 1) AS stockout_pct
  FROM fact_inventory fi
  JOIN dim_store ds USING (store_key)
  JOIN dim_product dp USING (product_key)
 WHERE fi.date = '{selected_date}'
 GROUP BY ds.store_id, dp.product_id
 ORDER BY ds.store_id, dp.product_id;
"""
df_stockout = fetch(q_stockout)

# ─── 9: Demand Forecast (Next 30 Days) ────────────────────────────────────────────
q_forecast = f"""
WITH dow_avg AS (
  SELECT extract(dow from date) AS dow,
         AVG(units_sold)    AS avg_sales
    FROM fact_inventory
   WHERE date BETWEEN '{selected_date}'::date - INTERVAL '365 days'
                  AND '{selected_date}'::date
   GROUP BY extract(dow from date)
),
future_dates AS (
  SELECT generate_series(
           '{selected_date}'::date + INTERVAL '1 day',
           '{selected_date}'::date + INTERVAL '30 days',
           '1 day'
         )::date AS date
)
SELECT fd.date,
       da.avg_sales AS forecast_sales
  FROM future_dates fd
  JOIN dow_avg da ON da.dow = extract(dow from fd.date)
 ORDER BY fd.date;
"""
df_forecast = fetch(q_forecast)

# ─── LAYOUT ───────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.subheader("Stock Level by Store")
    fig1 = px.bar(df_stock_store, x="store", y="total_stock", text="total_stock")
    st.plotly_chart(fig1, use_container_width=True)
with col2:
    st.subheader("Stock Level by Region")
    fig2 = px.bar(df_stock_region, x="region", y="total_stock", text="total_stock")
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    st.subheader("Low-Inventory Alerts")
    st.dataframe(df_alerts)
with col4:
    st.subheader("Stock Adjustment Recommendations (to_order > 0 = order more)")
    st.dataframe(df_adjust)

col5, col6 = st.columns(2)
with col5:
    st.subheader("Reorder Points (7-day)")
    st.dataframe(df_reorder)
with col6:
    st.subheader("Inventory Turnover Ratio (1yr)")
    st.dataframe(df_turnover)

col7, col8 = st.columns(2)
with col7:
    st.subheader("Days of Inventory Outstanding (DIO)")
    st.dataframe(df_dio)
with col8:
    st.subheader("ABC Classification (Fast/Slow Movers)")
    st.dataframe(df_abc)

col9, col10 = st.columns(2)
with col9:
    st.subheader("Stock-Out Rate (%)")
    st.dataframe(df_stockout)
with col10:
    st.subheader("Forecast Demand (Next 30 Days)")
    fig3 = px.line(df_forecast, x="date", y="forecast_sales", markers=True)
    fig3.update_layout(xaxis_title="Date", yaxis_title="Forecasted Units Sold")
    st.plotly_chart(fig3, use_container_width=True)
#      +----------------+        +----------------+        +--------------------+
#      | dim_store      |        | dim_product    |        | fact_inventory     |
#      +----------------+        +----------------+        +--------------------+
#      | * store_key    |◄───────┤ * product_key  │        | * fact_key         |
#      |   store_id     │        |   product_id   │        |   date             |
#      |   region       │        |   category     │        |   store_key (FK)   |
#      +----------------+        |   price        │        |   product_key (FK) |
#                                 |   discount     │        |   inventory_level  |
#                                 |   competitor   │        |   units_sold       |
#                                 |   seasonality  │        |   units_ordered    |
#                                 +----------------+        |   demand_forecast  |
#                                                            |   weather_cond     |
#   +------------------+                                     |   holiday_prom     |
#   | staging_retail   |                                     +--------------------+
#   +------------------+
#   |   date           |
#   |   store_id       |─┐
#   |   product_id     | │  (Loaded into dims + fact)
#   |   category       | |
#   |   region         | |
#   |   inventory_lvl  | |
#   |   units_sold     | |
#   |   units_ordered  | |
#   |   demand_forec   | |
#   |   price          | |
#   |   discount       | |
#   |   weather_cond   | |
#   |   holiday_prom   | |
#   |   competitor_pr  | |
#   |   seasonality    |─┘
#   +------------------+
