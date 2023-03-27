from geopy.geocoders import Nominatim
from geopy.adapters import AdapterHTTPError
import requests
from include.global_variables import global_variables as gv


def get_lat_long_for_cityname(city: str):
    """Converts a string of a city name provided into
    lat/long coordinates."""

    geolocator = Nominatim(user_agent="MyApp")

    try:
        location = geolocator.geocode(city)
        lat = location.latitude
        long = location.longitude

        # log the coordinates retrieved
        gv.task_log.info(f"Coordinates for {city}: {lat}/{long}")

    # if the coordinates cannot be retrieved log a warning
    except (AttributeError, KeyError, ValueError, AdapterHTTPError) as err:
        gv.task_log.warn(
            f"""Coordinates for {city}: could not be retrieved.
            Error: {err}"""
        )
        lat = "NA"
        long = "NA"

    city_coordinates = {"city": city, "lat": lat, "long": long}

    return city_coordinates


def get_current_weather_from_city_coordinates(coordinates, timestamp):
    """Queries an open weather API for the current weather at the
    coordinates provided."""

    lat = coordinates["lat"]
    long = coordinates["long"]
    city = coordinates["city"]

    r = requests.get(
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true"
    )

    # if the API call is successful log the current temp
    if r.status_code == 200:
        current_weather = r.json()["current_weather"]

        gv.task_log.info(
            "The current temperature in {0} is {1}°C".format(
                city, current_weather["temperature"]
            )
        )

    # if the API call is not successful, log a warning
    else:
        current_weather = {
            "temperature": "NULL",
            "windspeed": "NULL",
            "winddirection": "NULL",
            "weathercode": "NULL",
            "time": f"{timestamp}",
        }

        gv.task_log.warn(
            f"""
                Could not retrieve current temperature for {city} at
                {lat}/{long} from https://api.open/meteo.com.
                Request returned {r.status_code}.
            """
        )

    return {
        "city": city,
        "lat": lat,
        "long": long,
        "current_weather": current_weather,
        "API_response": r.status_code,
    }
