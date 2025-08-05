import os, io
import requests
import pandas as pd
from azure.storage.blob import BlobClient
from dotenv import load_dotenv

load_dotenv()

def fetch_weather_data(startdate, enddate):
    """
    Fetches daily weather data from the Finnish Meteorological Institute's API using the provided API key, 
    time range, and coordinates (bbox), utilizing the model 'kriging_suomi_daily'. 
    Returns a 3 dataframes containing timestamp, latitude, longitude, and several weather parameters.
    1st dataframe contains daily weather information, 2nd contains daily temperature sum and 3rd contains data from every 3h.

    Parameters:
    startdate (str): Start date in the format 'YYYY-MM-DD'.
    enddate (str): End date in the format 'YYYY-MM-DD'.

    Returns:
    pandas.DataFrame: Columns:
        - lon
        - lat
        - date
        - temp_avg
        - temp_min
        - temp_max
        - prec
        - wind_speed_avg
        - wind_speed_max
        - wind_dir_avg
        - rel_humid_avg
        - rel_humid_max
        - rel_humid_min
        - global_rad
        - vapour_press
        - snow_depth

    pandas.DataFrame: Columns:
        - lon
        - lat
        - date
        - temp_sum5

    pandas.DataFrame: Columns:
        - lon
        - lat
        - date
        - h
        - temp
        - prec
        - wind_speed
        - rel_humid
    """
    
    # Defining the access key and bounding box
    API_KEY = os.environ.get('API_KEY')
    full_finland = [19.0, 59.8, 32.0, 70.1]

    # Url for fetching data form kriging_suomi_daily model
    kriging_suomi_daily_url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, full_finland))}"
        f"&param=utctime%2CDailyMeanTemperature%2CMinimumTemperature24h%2CMaximumTemperature24h%2CPrecipitation24h%2CMaximumWind%2CDailyGlobalRadiation%2CVolumetricSoilWaterLayer1%2Clatlon"
        f"&model=kriging_suomi_daily&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    # Url for fetching data form kriging_suomi_synop model
    kriging_suomi_synop_url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, full_finland))}"
        f"&param=utctime%2CTemperature%2CPrecipitation3h%2CWindSpeedMS%2CHumidity%2Clatlon"
        f"&model=kriging_suomi_synop&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    # Url for fetching data form kriging_suomi_kasvukausi model
    kriging_suomi_kasvukausi_url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, full_finland))}"
        f"&param=utctime%2CEffectiveTemperatureSum%2Clatlon"
        f"&model=kriging_suomi_kasvukausi&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    # A dictionary for different model urls
    urls = {
        "daily": kriging_suomi_daily_url,
        "synop": kriging_suomi_synop_url,
        "kasvukausi": kriging_suomi_kasvukausi_url
    }

    # Defining dictionaries for the responses and data
    responses = {}
    data = {}

    for key, url in urls.items():
        resp = requests.get(url)
        resp.raise_for_status()
        responses[key] = resp
        data[key] = resp.json()


def upload_weather_data(storage_account_name, container_name, filepath, data, file_type='parquet'):
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
        if file_type == 'parquet':
            data.to_parquet(buffer, index=False)
        elif file_type == 'csv':
            data.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        buffer.seek(0)

        # Upload to Azure Blob Storage
        blob_client.upload_blob(buffer, overwrite=True)
        print(f"Successfully uploaded {filepath} to {container_name}.")

    except Exception as e:
        print(f"Error uploading blob data: {e}")
    
    def update_weather_parquet(storage_account_name, container_name, filepath):

        # Build full SAS URL
        sas_token = os.environ.get('SAS_TOKEN')
        if not sas_token.startswith('?'):
            sas_token = '?' + sas_token
        full_sas_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{filepath}{sas_token}"
        blob_client = BlobClient.from_blob_url(full_sas_url)

        # Step 1: Try to load existing parquet file
        existing_df = pd.DataFrame()
        try:
            download_stream = blob_client.download_blob()
            existing_df = pd.read_parquet(io.BytesIO(download_stream.readall()))
            print(f"Loaded {len(existing_df)} existing records.")
        except Exception:
            print("No existing file found, starting fresh.")

        # Step 2: Determine last date
        last_date = None
        if not existing_df.empty and "date" in existing_df.columns:
            last_date = existing_df["date"].max()
            print(f"Latest date in data: {last_date}")

        # Step 3: Fetch new data
        start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d") if last_date else "2020-01-01"
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
        new_data_dict = fetch_weather_data(start_date, end_date)
        new_df = new_data_dict["daily"]  # or combine all if needed

        # Step 4: Append and remove duplicates
        combined_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset=["lon", "lat", "date"])

        # Step 5: Upload updated parquet
        buffer = io.BytesIO()
        combined_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_client.upload_blob(buffer, overwrite=True)
        print(f"Uploaded {len(combined_df)} total records to {filepath}")
