###################
# PACKAGE IMPORTS #
###################

import streamlit as st
import duckdb
import pandas as pd
import sys
from datetime import datetime
import altair as alt

#############
# VARIABLES #
#############

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

global_temp_col = "Global"
country_temp_col = "United States"
metric_col_name = "Average Surface Temperature"
date_col_name = "dt"

################
# GETTING DATA #
################

# retrieving and caching data
@st.cache_data
def get_data(country, city, db="dwh"):

    country_name_no_space = country.replace(" ", "_")

    # Query duckdb database `dwh`
    cursor = duckdb.connect(db)

    global_data = cursor.execute(
        f"""SELECT dt AS {date_col_name}, LandAverageTemperature AS '{global_temp_col}'
        FROM temp_global_table;"""
    ).fetchall()
    country_data = cursor.execute(
        f"""SELECT dt AS {date_col_name}, AverageTemperature AS '{country_temp_col}'
        FROM {country_name_no_space} ORDER BY dt;"""
    ).fetchall()
    city_data = cursor.execute(
        f"""SELECT *
        FROM in_weather
        WHERE city == '{city}';"""
    ).fetchall()

    cursor.close()

    return global_data, country_data, city_data

global_data, country_data, city_data = get_data(country_name, city_name)

##################
# PREPARING DATA #
##################

# prepare global climate data for plotting
df_g = pd.DataFrame(
    global_data,
    columns=[
        date_col_name,
        global_temp_col
    ]
)

# ensure correct datetime format
df_g[date_col_name] = pd.to_datetime(df_g.dt)

# prepare country climate data for plotting
df_c = pd.DataFrame(
    country_data,
    columns=[
        date_col_name,
        country_temp_col
    ]
)

# ensure correct datetime format
df_c[date_col_name] = pd.to_datetime(df_c.dt)



df = df_g
df[country_temp_col] = df_c[country_temp_col]

df_melt = df[[date_col_name, country_temp_col, global_temp_col]].melt(id_vars=[date_col_name], var_name='Scale', value_name=metric_col_name, ignore_index=True)

# add time grains
df_melt["Decade"] = df_melt[date_col_name].dt.year // 10 * 10
df_melt["Quarter"] = df_melt[date_col_name].dt.to_period("Q")
df_melt["YearMonth"] = df_melt[date_col_name].dt.to_period("M")

df_melt["decade_grain"] = df_melt.groupby(
    [df_melt["Decade"], df_melt["Scale"]]
)[metric_col_name].transform('mean')

df_melt["year_grain"] = df_melt.groupby(
    [df_melt[date_col_name].dt.year, df_melt["Scale"]]
)[metric_col_name].transform('mean')

df_melt["month_grain"] = df_melt.groupby(
    [df_melt["YearMonth"], df_melt["Scale"]]
)[metric_col_name].transform('mean')

df_melt["quarter_grain"] = df_melt.groupby(
    [df_melt["Quarter"], df_melt["Scale"]]
)[metric_col_name].transform('mean')


print(df_melt)

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

#################
# STREAMLIT APP #
#################

st.title("Global Climate and Local Weather")
st.subheader(f"Surface temperatures")

### Main App ###

### Climate section

# Define columns
col1, col2, col3= st.columns([3, 1, 1])

# Col 1 contains the time-period slider
with col1:

    start_time = st.slider(
        "Adjust the time-period shown.",
        value=(datetime(1760, 1, 1), datetime(2023,1,1)),
        format="YYYY")

    # modify the plotted table according to slider input
    df_melt_cut = df_melt[
        (df_melt[date_col_name] >= start_time[0]) & (df_melt[date_col_name] <= start_time[1])
    ]

# Col 2 contains the check-boxes for global data and user-selected country
with col2:

    # Checkboxes
    global_check = st.checkbox("Global", value=True)
    country_check = st.checkbox(f"{country_name}", value=True)

    # create a list of lines to show for plotting
    lines_to_show = []
    if global_check:
        lines_to_show.append("Global")
    if country_check:
        lines_to_show.append(f"{country_name}")

# Col 3 contains the selection of grain to display
with col3:
    grain = st.selectbox(
        "Average temperatures per",
        ("Decade", "Year", "Quarter", "Month"),
        label_visibility="visible",
        index = 0
    )

# get interactive chart
def get_chart(data, grain):

    data = data[data['Scale'].isin(lines_to_show)]

    if grain == "Decade":
        y = "decade_grain"
    if grain == "Year":
        y = "year_grain"
    if grain == "Quarter":
        y = "quarter_grain"
    if grain == "Month":
        y = "month_grain"


    hover = alt.selection_single(
        fields=[date_col_name],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=date_col_name,
            y=y,
            color="Scale"
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
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


### Sidebar ###

with st.sidebar:

    # use direct link once the pics are decided and on GH
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