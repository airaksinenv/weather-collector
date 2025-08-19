import os, io
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobClient
from dotenv import load_dotenv
import logging

load_dotenv()


FULL_FINLAND_BBOX = [19.0, 59.8, 32.0, 70.1]
API_KEY = os.environ.get('API_KEY')
if not API_KEY:
    raise RuntimeError('API_KEY not found in environment variables')

def parse_fmi_data(json_data, params):
    rows = []
    for entry in json_data:
        timestamp = entry['utctime']

        latlon_raw = entry['latlon'].replace("[", "").replace("]", "").split()
        latlon_clean = [float(v.strip(",")) for v in latlon_raw]
        latlon_pairs = list(zip(latlon_clean[::2], latlon_clean[1::2]))

        param_values = {}
        for param in params:
            val_str = entry[param].replace(" ", ",")
            param_values[param] = np.fromstring(val_str.strip("[]"), sep=",")

        for i, (lat, lon) in enumerate(latlon_pairs):
            row = {"timestamp": timestamp, "latitude": lat, "longitude": lon}
            for param in params:
                row[param] = param_values[param][i] if i < len(param_values[param]) else np.nan
            rows.append(row)

    df = pd.DataFrame(rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def fetch_fmi_data(startdate, enddate, model_type):
    """
    Fetch FMI weather data for given model_type.

    Parameters:
        startdate (str): Start date in 'YYYY-MM-DD'
        enddate (str): End date in 'YYYY-MM-DD'
        model_type (str): One of 'daily', 'kasvukausi', or 'synop'

    Returns:
        pandas.DataFrame with parsed data
    """

    params_map = {
        "daily": [
            'DailyMeanTemperature', 'MinimumTemperature24h', 'MaximumTemperature24h',
            'Precipitation24h', 'MaximumWind', 'DailyGlobalRadiation', 'VolumetricSoilWaterLayer1'
        ],
        "kasvukausi": ['EffectiveTemperatureSum'],
        "synop": ['Temperature', 'WindSpeedMS', 'Humidity'],
        "hourly": ['Precipitation1h', 'Humidity', 'WindSpeedMS', 'Temperature']
        #Precipitation3h ei toimi
    }

    model_map = {
        "daily": "kriging_suomi_daily",
        "kasvukausi": "kriging_suomi_kasvukausi",
        "synop": "kriging_suomi_synop",
        "hourly": "kriging_suomi_hourly"
    }

    if model_type not in model_map:
        raise ValueError(f"Invalid model_type '{model_type}'. Must be one of {list(model_map.keys())}")

    params = params_map[model_type]
    model = model_map[model_type]

    url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, FULL_FINLAND_BBOX))}"
        f"&param=utctime%2C{ '%2C'.join(params) }%2Clatlon"
        f"&model={model}&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    #print(url)
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    return parse_fmi_data(data, params)


def upload_weather_data(storage_account_name, container_name, filepath, data, file_type='csv'):
    """
    Uploads weather data to a blob storage in .csv -format.

    Parameters:
        storage_account_name (str) : Name of the storage account.
        container_name (str) : Name of the container.
        filepath (str) : Filepath within the container, also including the file name.
        data (pd.Dataframe) : Weather data in pandas dataframe format.
        file_type (str) : Filetype, defaults to .csv as its the only supported file type for now.
    """
    try:
        # Get SAS token from environment
        sas_token = os.environ.get('SAS_TOKEN')
        if not sas_token:
            raise ValueError("SAS_TOKEN not found in environment variables.")
        if not sas_token.startswith('?'):
            sas_token = '?' + sas_token

        # Build full SAS URL
        full_sas_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{filepath}{sas_token}"
        blob_client = BlobClient.from_blob_url(full_sas_url)

        # Convert DataFrame to bytes in-memory
        buffer = io.BytesIO()
        if file_type == 'csv':
            data.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        buffer.seek(0)

        # Upload to Azure Blob Storage
        blob_client.upload_blob(buffer, overwrite=True)
        logging.info(f"Successfully uploaded {filepath} to {container_name}.")

    except Exception as e:
        logging.warning(f"Error uploading blob data: {e}")