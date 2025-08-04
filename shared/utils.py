import os
import requests
from dotenv import load_dotenv

load_dotenv()

def fetch_weather_data(startdate, enddate):
    """
    Fetches daily weather data from the Finnish Meteorological Institute's API using the provided API key, 
    time range, and coordinates (bbox), utilizing the model 'kriging_suomi_daily'. 
    Returns a DataFrame containing timestamp, latitude, longitude, and several weather parameters.

    Parameters:
    startdate (str): Start date in the format 'YYYY-MM-DD'.
    enddate (str): End date in the format 'YYYY-MM-DD'.

    Returns:
    pandas.DataFrame: Columns:
        - timestamp
        - latitude
        - longitude
        - Precipitation24h (mm)
        - MaximumTemperature24h (°C)
        - MinimumTemperature24h (°C)
        - MaximumWind (m/s)
        - DailyMeanTemperature (°C)
        - MinimumGroundTemperature06 (°C)
        - DailyGlobalRadiation (W/m²)
        - VolumetricSoilWaterLayer1 (%)
    """

    API_KEY = os.environ.get('API_KEY')
    bbox = [19.0, 59.8, 32.0, 70.1]

    url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, bbox))}"
        f"&param=utctime%2CPrecipitation24h%2CMaximumTemperature24h%2CMinimumTemperature24h%2CMaximumWind%2CDailyMeanTemperature%2CMinimumGroundTemperature06%2CDailyGlobalRadiation%2CVolumetricSoilWaterLayer1%2Clatlon"
        f"&model=kriging_suomi_daily&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json

def upload_weather_data():
    pass