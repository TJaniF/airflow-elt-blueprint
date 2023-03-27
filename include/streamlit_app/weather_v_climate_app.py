# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import date
import altair as alt
import json

# --------- #
# VARIABLES #
# --------- #

city_name = os.environ["my_city"]
city_coordinates = json.loads(os.environ["city_coordinates"])
city_lat = city_coordinates["lat"]
city_long = city_coordinates["long"]
user_name = os.environ["my_name"]

duck_db_instance_name = (
    "dwh"  # when changing this value also change the db name in .env
)
global_temp_col = "Global"
metric_col_name = "Average Surface Temperature"
date_col_name = "date"
decade_grain_col_name = "Average Surface Temperature per Decade"
year_grain_col_name = "Average Surface Temperature per Year"
month_grain_col_name = "Average Surface Temperature per Month"

# ------------ #
# GETTING DATA #
# ------------ #


# retrieving data
def get_city_data(city, db=f"/usr/local/airflow/{duck_db_instance_name}"):
    """Function to query a local DuckDB instance for climate and
    weather data processed through the data pipeline."""

    # Query duckdb database `dwh`
    cursor = duckdb.connect(db)

    # get local weather data
    city_data = cursor.execute(
        f"""SELECT *
        FROM in_weather
        WHERE city == '{city}' ORDER BY TIMESTAMP LIMIT 1;"""
    ).fetchall()

    cursor.close()

    return city_data


def get_global_surface_temp_data(db=f"/usr/local/airflow/{duck_db_instance_name}"):

    cursor = duckdb.connect(db)

    # get global surface temperature data
    global_surface_temp_data = cursor.execute(
        f"""SELECT * 
        FROM reporting_table;"""
    ).fetchall()

    global_surface_temp_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = 'reporting_table';"""
    ).fetchall()

    cursor.close()

    df = pd.DataFrame(
        global_surface_temp_data, columns=[x[0] for x in global_surface_temp_col_names]
    )

    return df


global_temp_df = get_global_surface_temp_data()
global_temp_df["Scale"] = "Global"
global_temp_df.columns = ['date', decade_grain_col_name, year_grain_col_name, month_grain_col_name, 'Raw', 'Scale']

city_data = get_city_data(city_name)

# ----------------#
# PREPARING DATA #
# ----------------#

# prepare local weather data for plotting
df_city = pd.DataFrame(
    city_data,
    columns=[
        "city",
        "api_response",
        "dt",
        "temperature",
        "windspeed",
        "winddirection",
        "weathercode",
    ],
)

# ---------------#
# STREAMLIT APP #
# ---------------#

st.title("Global Climate and Local Weather")

st.markdown(f"Hello {user_name} :wave: Welcome to your Streamlit App! :blush:")

st.subheader("Surface temperatures")

### Main App ###

### Climate section

# define columns
col1, col2 = st.columns([3, 1])

# col 1 contains the time-period slider
with col1:

    # add slider
    timespan = st.slider(
        "Adjust the time-period shown.",
        value=(date(1760, 1, 1), date(2023, 1, 1)),
        format="YYYY",
    )

    # modify the plotted table according to slider input
    global_temp_df_timecut = global_temp_df[
        (global_temp_df[date_col_name] >= timespan[0])
        & (global_temp_df[date_col_name] <= timespan[1])
    ]

# col 3 contains the selection of grain to display
with col2:

    # add selectbox for grain of data to display
    grain = st.selectbox(
        "Average temperatures per",
        ("Decade", "Year", "Raw"),
        label_visibility="visible",
        index=0,
    )


# get interactive chart
def get_chart(data, grain):
    """Function to return interactive chart."""

    # adjust grain based on selectbox input
    if grain == "Decade":
        y = decade_grain_col_name
    if grain == "Year":
        y = year_grain_col_name
    if grain == "Raw":
        y = "Raw"

    # add display of data when hovering
    hover = alt.selection_single(
        fields=[date_col_name],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    # add lines
    lines = alt.Chart(data).mark_line().encode(x=date_col_name, y=y, color="Scale")

    # draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # draw a ruler at the location of the selection
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x=date_col_name,
            y=y,
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip(date_col_name, title="Date"),
                alt.Tooltip(y, title="Average Surface °C", format=".2f"),
            ],
        )
        .add_selection(hover)
    )

    return (lines + points + tooltips).interactive()


chart = get_chart(global_temp_df_timecut, grain)

# plot climate chart
st.altair_chart((chart).interactive(), use_container_width=True)

### Weather section

if df_city["api_response"][0] == 200:

    st.subheader(f"Current weather in {city_name}")

    # create 3 columns for metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Temperature [°C]", round(df_city["temperature"], 1))
    col2.metric("Windspeed [km/h]", round(df_city["windspeed"], 2))
    col3.metric("Winddirection [° clockwise from north]", df_city["winddirection"])

    # plot location of user-defined city
    city_coordinates_df = pd.DataFrame([(city_lat, city_long)], columns=["lat", "lon"])

    st.map(city_coordinates_df)

else:
    st.markdown(
        f"""Your call to the open weather API returned
        {df_city['api_response'][0]}.
        Try running the pipeline again with a different city!"""
    )

st.success(f"Congratulations, {user_name}, on finishing this tutorial!", icon="🎉")


### Sidebar ###

with st.sidebar:

    # display logos of tools used with links to their websites as well as attribute data sources
    st.markdown(
        """
        <h2> Tools used </h2>
            <a href='https://docs.astronomer.io/astro/cli/install-cli', title='Astro CLI by Astronomer'>
                <img src='https://avatars.githubusercontent.com/u/12449437?s=280&v=4'  width='50' height='50'></a>
            <a href='https://airflow.apache.org/', title='Apache Airflow'>
                <img src='https://pbs.twimg.com/media/EFOe7T4X4AEfIyl.jpg'  width='50' height='50'></a>
            <a href='https://min.io/', title='MinIO'>
                <img src='https://min.io/resources/img/logo/MINIO_Bird.png'  width='25' height='50'></a>
            <a href='https://duckdb.org/', title='DuckDB'>
                <img src='https://duckdb.org/images/favicon/apple-touch-icon.png'  width='50' height='50'></a>
            <a href='https://streamlit.io/', title='Streamlit'>
                <img src='https://streamlit.io/images/brand/streamlit-mark-color.svg'  width='50' height='50'></a>
        </br>
        </br>
        <h2> Data sources </h2>
            <a href='https://open-meteo.com/'> Open Meteo API </a>
            </br>
            (<a href='https://creativecommons.org/licenses/by/4.0/'>CC BY 4.0</a>)
        </br>
        </br>
            <a href='https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data'> Berkely Earth and Kristen Sissener </a>
            </br>
            (<a href='https://creativecommons.org/licenses/by-nc-sa/4.0/'>CC BY-NC-SA 4.0</a>)
        </br>
        </br>
        """,
        # warning: using html in your streamlit app can open you to security
        # risk when writing unsafe html code,
        # see: https://github.com/streamlit/streamlit/issues/152
        unsafe_allow_html=True,
    )

    st.button("Re-run")
