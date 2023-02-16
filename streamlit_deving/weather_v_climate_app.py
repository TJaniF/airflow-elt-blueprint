import streamlit as st
import numpy as np
import time
import duckdb
import pandas as pd
import sys
from datetime import datetime, date
from PIL import Image

country_name = sys.argv[0]
city_name = sys.argv[1]
user_name = sys.argv[2]

#### hardcoded input for testing - remove later
country_name = "United States"
country_name_no_space = country_name.replace(" ", "_")
city_name = "New York"
city_lat = 40.7128
city_long = -74.0060
####


# Query duckdb database `dwh`
cursor = duckdb.connect("dwh")

global_data = cursor.execute("SELECT dt, LandAverageTemperature, LandAverageTemperatureUncertainty FROM temp_global_table;").fetchall()
country_data = cursor.execute(f"SELECT dt, AverageTemperature FROM {country_name_no_space} ORDER BY dt;").fetchall()
city_data = cursor.execute(f"SELECT * FROM in_weather WHERE city == '{city_name}';").fetchall()

cursor.close()

# prepare global climate data
df_global = pd.DataFrame(global_data, columns=["dt", "LandAverageTemperature", "LandAverageTemperatureUncertainty"])
df_global["dates"] = pd.to_datetime(df_global.dt)
df_global["av_per_year_global"] = df_global.groupby(df_global.dates.dt.year)['LandAverageTemperature'].transform('mean')

# prepare country level climate data
df_country = pd.DataFrame(country_data, columns=["dt", "AvTemp"])
df_country['dates'] = pd.to_datetime(df_country.dt)
df_country["av_per_year_country"]  = df_country.groupby(df_country.dates.dt.year)['AvTemp'].transform('mean')


df_global_country = df_global
df_global_country["av_per_year_country"] = df_country["av_per_year_country"]

print(df_global_country)

# prepare local weather data
df_city = pd.DataFrame(city_data, columns=["city", "dt", "temperature", "windspeed", "winddirection", "weathercode"])

# Streamlit App
st.title("Global Climate and Local Weather")

st.subheader(f"Surface temperatures globally and in {country_name}")

col1, col2= st.columns([3, 1])

with col2:

    global_check = st.checkbox("Global", value=True, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible")
    country_check = st.checkbox(f"{city_name}", value=True, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible")

with col1:

    start_time = st.slider(
        "Adjust the time-period shown.",
        value=(date(1760, 1, 1), date(2023,1,1)),
        format="YYYY")
    
    df_global_country_cut_off = df_global_country[(df_global_country["dt"] >= start_time[0]) & (df_global["dt"] <= start_time[1])]

lines_to_show = []

if global_check:
    lines_to_show.append("av_per_year_global")
if country_check:
    lines_to_show.append("av_per_year_country")

st.line_chart(df_global_country_cut_off, x="dt", y=lines_to_show, width=1)


st.subheader(f"Current weather in {city_name}")
col1, col2, col3 = st.columns(3)
col1.metric("Temperature",round(df_city["temperature"],2))
col2.metric("Windspeed",  round(df_city["windspeed"],2))
col3.metric("Winddirection",  df_city["winddirection"])

city_coordinates_df = pd.DataFrame(
    [(city_lat, city_long)], 
    columns=['lat', 'lon'])

st.map(city_coordinates_df)

with st.sidebar:

    st.button("Re-run")

    image_astronomer = Image.open('A.png')

    # use direct link once the pics are decided and on GH
    st.markdown(f"[![Astronomer Logo](https://avatars.githubusercontent.com/u/12449437?s=280&v=4)](https://astronomer.io)") 
    st.markdown(f"[![Airflow Logo](https://pbs.twimg.com/media/EFOe7T4X4AEfIyl.jpg)](https://airflow.apache.org/docs/apache-airflow/stable/index.html)") 