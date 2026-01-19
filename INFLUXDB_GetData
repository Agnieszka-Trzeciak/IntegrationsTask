import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime

def Fetch_Data(START_DATE,END_DATE):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
            "latitude": 40.7143,
            "longitude": -74.006,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "wind_direction_10m", "apparent_temperature", "rain", "snowfall", "cloud_cover", "surface_pressure"],
            "timezone": "America/New_York",
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(4).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(5).ValuesAsNumpy()
    hourly_rain = hourly.Variables(6).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(9).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["surface_pressure"] = hourly_surface_pressure

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    hourly_dataframe = hourly_dataframe[hourly_dataframe['date']<=pd.to_datetime(datetime.now()).tz_localize('UTC')]

    Data = []
    for row_pk, row in hourly_dataframe.iterrows():
        Data.append(f"weather_data_final \
temperature_2m={row.iloc[1]},\
relative_humidity_2m={row.iloc[2]},\
precipitation={row.iloc[3]},\
wind_speed_10m={row.iloc[4]},\
wind_direction_10m={row.iloc[5]},\
apparent_temperature={row.iloc[6]},\
rain={row.iloc[7]},\
snowfall={row.iloc[8]},\
cloud_cover={row.iloc[9]},\
surface_pressure={row.iloc[10]} \
{int(row.iloc[0].timestamp() * 1e9)}")
    return Data


