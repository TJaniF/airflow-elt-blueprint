#-----------------#
# PACKAGE IMPORTS #
#-----------------#

import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime
import altair as alt
import json

#-----------#
# VARIABLES #
#-----------#

country_name = os.environ["my_country"]
city_name = os.environ["my_city"]
city_coordinates = json.loads(os.environ["city_coordinates"])
city_lat = city_coordinates["lat"]
city_long = city_coordinates["long"]
user_name = os.environ["my_name"]

duck_db_instance_name = "dwh" # when changing this value also change the db name in .env
global_temp_col = "Global"
country_temp_col = country_name
metric_col_name = "Average Surface Temperature"
date_col_name = "Year"
decade_grain_col_name = "Average Surface Temperature per Decade"
year_grain_col_name = "Average Surface Temperature per Year"
quarter_grain_col_name = "Average Surface Temperature per Quarter"
month_grain_col_name = "Average Surface Temperature per Month"

#--------------#
# GETTING DATA #
#--------------#

# retrieving data
def get_data(country, city, db=f"/usr/local/airflow/{duck_db_instance_name}"):
    """Function to query a local DuckDB instance for climate and
    weather data processed through the data pipeline."""

    # replace spaces in country names
    country_name_no_space = country.replace(" ", "_")

    # Query duckdb database `dwh`
    cursor = duckdb.connect(db)

    # get global surface temperature data
    global_data = cursor.execute(
        f"""SELECT dt AS {date_col_name}, LandAverageTemperature AS '{global_temp_col}'
        FROM temp_global_table;"""
    ).fetchall()
    # get country surface temperature data
    country_data = cursor.execute(
        f"""SELECT dt AS {date_col_name}, AverageTemperature AS '{country_temp_col}'
        FROM {country_name_no_space} ORDER BY dt;"""
    ).fetchall()
    # get local weather data
    city_data = cursor.execute(
        f"""SELECT *
        FROM in_weather
        WHERE city == '{city}' ORDER BY TIMESTAMP LIMIT 1;"""
    ).fetchall()

    cursor.close()

    return global_data, country_data, city_data

global_data, country_data, city_data = get_data(country_name, city_name)

#----------------#
# PREPARING DATA #
#----------------#

# prepare global climate data for plotting
df_g = pd.DataFrame(
    global_data,
    columns=[
        date_col_name,
        global_temp_col
    ]
)

# ensure correct datetime format
df_g[date_col_name] = pd.to_datetime(df_g[date_col_name])

# prepare country climate data for plotting
df_c = pd.DataFrame(
    country_data,
    columns=[
        date_col_name,
        country_temp_col
    ]
)

# ensure correct datetime format
df_c[date_col_name] = pd.to_datetime(df_c[date_col_name])

# combine dataframes for plotting
df = df_g
df[country_temp_col] = df_c[country_temp_col]

# melt data
df_molten = df[[date_col_name, country_temp_col, global_temp_col]].melt(
    id_vars=[date_col_name],
    var_name='Scale',
    value_name=metric_col_name,
    ignore_index=True
)

# average surface temperature over additional timespans
df_molten["Decade"] = df_molten[date_col_name].dt.year // 10 * 10
df_molten["Quarter"] = df_molten[date_col_name].dt.to_period("Q")
df_molten["YearMonth"] = df_molten[date_col_name].dt.to_period("M")

df_molten[decade_grain_col_name] = df_molten.groupby(
    [df_molten["Decade"], df_molten["Scale"]]
)[metric_col_name].transform('mean')

df_molten[year_grain_col_name] = df_molten.groupby(
    [df_molten[date_col_name].dt.year, df_molten["Scale"]]
)[metric_col_name].transform('mean')

df_molten[month_grain_col_name] = df_molten.groupby(
    [df_molten["YearMonth"], df_molten["Scale"]]
)[metric_col_name].transform('mean')

df_molten[quarter_grain_col_name] = df_molten.groupby(
    [df_molten["Quarter"], df_molten["Scale"]]
)[metric_col_name].transform('mean')

# prepare local weather data for plotting
df_city = pd.DataFrame(
    city_data,
    columns=[
        "city",
        "dt",
        "temperature",
        "windspeed",
        "winddirection",
        "weathercode"
    ]
)

#---------------#
# STREAMLIT APP #
#---------------#

st.title("Global Climate and Local Weather")

st.markdown(f"Hello {user_name} :wave: Welcome to your Streamlit App! :blush:")

st.subheader(f"Surface temperatures")

### Main App ###

### Climate section

# define columns
col1, col2, col3= st.columns([3, 1, 1])

# col 1 contains the time-period slider
with col1:

    # add slider
    start_time = st.slider(
        "Adjust the time-period shown.",
        value=(datetime(1760, 1, 1), datetime(2023,1,1)),
        format="YYYY")

    # modify the plotted table according to slider input
    df_melt_cut = df_molten[
        (df_molten[date_col_name] >= start_time[0]) & (df_molten[date_col_name] <= start_time[1])
    ]

# col 2 contains the check-boxes for global data and user-selected country
with col2:

    # add checkboxes
    global_check = st.checkbox("Global", value=True)
    country_check = st.checkbox(f"{country_name}", value=True)

    # create a list of lines to show for plotting
    lines_to_show = []
    if global_check:
        lines_to_show.append("Global")
    if country_check:
        lines_to_show.append(f"{country_name}")

# col 3 contains the selection of grain to display
with col3:

    # add selectbox for grain of data to display
    grain = st.selectbox(
        "Average temperatures per",
        ("Decade", "Year", "Quarter", "Month"),
        label_visibility="visible",
        index = 0
    )

# get interactive chart
def get_chart(data, grain):
    """Function to return interactive chart."""

    # only show selected Scales (global vs country vs neither or both)
    data = data[data['Scale'].isin(lines_to_show)]

    # adjust grain based on selectbox input
    if grain == "Decade":
        y = decade_grain_col_name
    if grain == "Year":
        y = year_grain_col_name
    if grain == "Quarter":
        y = quarter_grain_col_name
    if grain == "Month":
        y = month_grain_col_name

    # add display of data when hovering
    hover = alt.selection_single(
        fields=[date_col_name],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    # add lines
    lines = (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=date_col_name,
            y=y,
            color="Scale"
        )
    )

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

chart = get_chart(df_melt_cut, grain)

# plot climate chart
st.altair_chart(
    (chart).interactive(),
    use_container_width=True
)

### Weather section

st.subheader(f"Current weather in {city_name}")

# create 3 columns for metrics
col1, col2, col3 = st.columns(3)
col1.metric("Temperature [°C]", round(df_city['temperature'],1))
col2.metric("Windspeed [km/h]",  round(df_city["windspeed"],2))
col3.metric("Winddirection [° clockwise from north]",  df_city["winddirection"])


# plot location of user-defined city
city_coordinates_df = pd.DataFrame(
    [(city_lat, city_long)], 
    columns=['lat', 'lon'])

st.map(city_coordinates_df)

st.success(f"Congratulations, {user_name}, on finishing this tutorial!", icon="🎉")


### Sidebar ###

with st.sidebar:

    # display logos of tools used with links to their websites as well as attribute data sources
    st.markdown("""
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
    # warning: using html in your streamlit app can open you to security risk when writing unsafe html code, see: https://github.com/streamlit/streamlit/issues/152
    unsafe_allow_html=True)

    st.button("Re-run")