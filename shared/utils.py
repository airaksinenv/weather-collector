import os
import io
import logging
import traceback
from datetime import datetime

import requests
import pandas as pd
import numpy as np
from azure.storage.blob import BlobClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional local dev support (Azure should use Application Settings)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

FULL_FINLAND_BBOX = [19.0, 59.8, 32.0, 70.1]

API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    raise RuntimeError("API_KEY not found in environment variables")

# -------------------------
# Requests session + retries
# -------------------------
def _make_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = _make_session()

# -------------------------
# Maps
# -------------------------
PARAMS_MAP = {
    "daily": [
        "DailyMeanTemperature",
        "MinimumTemperature24h",
        "MaximumTemperature24h",
        "Precipitation24h",
        "MaximumWind",
        "DailyGlobalRadiation",
        "VolumetricSoilWaterLayer1",
    ],
    "kasvukausi": ["EffectiveTemperatureSum"],
    "synop": ["Temperature", "WindSpeedMS", "Humidity"],
    "hourly": ["Precipitation1h", "Humidity", "WindSpeedMS", "Temperature"],
    "snow": ["WaterEquivalentOfSnow"],
}

MODEL_MAP = {
    "daily": "kriging_suomi_daily",
    "kasvukausi": "kriging_suomi_kasvukausi",
    "synop": "kriging_suomi_synop",
    "hourly": "kriging_suomi_hourly",
    "snow": "kriging_suomi_snow",
}

# -------------------------
# Parsing
# -------------------------
def parse_fmi_data(json_data, params):
    rows = []
    for entry in json_data:
        timestamp = entry["utctime"]

        latlon_raw = entry["latlon"].replace("[", "").replace("]", "").split()
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
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# -------------------------
# Fetch (date-only)
# -------------------------
def fetch_fmi_data(startdate, enddate, model_type):
    if model_type not in MODEL_MAP:
        raise ValueError(f"Invalid model_type '{model_type}'. Must be one of {list(MODEL_MAP.keys())}")

    params = PARAMS_MAP[model_type]
    model = MODEL_MAP[model_type]

    url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, FULL_FINLAND_BBOX))}"
        f"&param=utctime%2C{'%2C'.join(params)}%2Clatlon"
        f"&model={model}&format=json&timeformat=sql"
        f"&starttime={startdate}T00%3A00%3A00&endtime={enddate}T00%3A00%3A00"
        f"&timestep=data&precision=double"
    )

    logging.info(f"Fetching FMI: model={model} start={startdate} end={enddate}")

    try:
        resp = SESSION.get(url, timeout=(10, 300))
        if resp.status_code != 200:
            logging.error(f"FMI HTTP {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"fetch_fmi_data failed ({model_type} {startdate}->{enddate}): {e}")
        logging.error(traceback.format_exc())
        raise

    return parse_fmi_data(data, params)

# -------------------------
# Fetch (datetime range) - needed for hourly chunking
# -------------------------
def fetch_fmi_data_timerange(start_dt: datetime, end_dt: datetime, model_type: str) -> pd.DataFrame:
    if model_type not in MODEL_MAP:
        raise ValueError(f"Invalid model_type '{model_type}'. Must be one of {list(MODEL_MAP.keys())}")

    params = PARAMS_MAP[model_type]
    model = MODEL_MAP[model_type]

    # URL encode ":" as %3A
    start_s = start_dt.strftime("%Y-%m-%dT%H:%M:%S").replace(":", "%3A")
    end_s = end_dt.strftime("%Y-%m-%dT%H:%M:%S").replace(":", "%3A")

    url = (
        f"https://data.fmi.fi/fmi-apikey/{API_KEY}/timeseries"
        f"?bbox={','.join(map(str, FULL_FINLAND_BBOX))}"
        f"&param=utctime%2C{'%2C'.join(params)}%2Clatlon"
        f"&model={model}&format=json&timeformat=sql"
        f"&starttime={start_s}&endtime={end_s}"
        f"&timestep=data&precision=double"
    )

    logging.info(f"Fetching FMI: model={model} start={start_dt} end={end_dt}")

    try:
        resp = SESSION.get(url, timeout=(10, 300))
        if resp.status_code != 200:
            logging.error(f"FMI HTTP {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"fetch_fmi_data_timerange failed ({model_type} {start_dt}->{end_dt}): {e}")
        logging.error(traceback.format_exc())
        raise

    return parse_fmi_data(data, params)

# -------------------------
# Hourly aggregation helpers (memory-safe)
# -------------------------
def vapour_pressure(temp_c, rel_humid):
    es = 0.6108 * np.exp((17.27 * temp_c) / (temp_c + 237.3))  # kPa
    ea = es * (rel_humid / 100.0)  # actual vapor pressure
    return ea * 10

def aggregate_hourly_chunk(hourlydf: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates ONE chunk of hourly data into partial sums/min/max so we can combine chunks later
    without keeping all hourly rows in memory.
    """
    hourlydf["timestamp"] = pd.to_datetime(hourlydf["timestamp"])
    hourlydf["date"] = hourlydf["timestamp"].dt.date

    hourlydf["vapour_press"] = vapour_pressure(hourlydf["Temperature"], hourlydf["Humidity"])

    agg = (
        hourlydf.groupby(["date", "latitude", "longitude"])
        .agg(
            rel_humid_min=("Humidity", "min"),
            rel_humid_max=("Humidity", "max"),
            rel_humid_sum=("Humidity", "sum"),
            rel_humid_count=("Humidity", "count"),
            wind_speed_sum=("WindSpeedMS", "sum"),
            wind_speed_count=("WindSpeedMS", "count"),
            vapour_press_sum=("vapour_press", "sum"),
            vapour_press_count=("vapour_press", "count"),
        )
        .reset_index()
    )
    return agg

def combine_hourly_aggs(aggs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combines partial aggregates across all chunks and produces final daily feature columns.
    """
    if not aggs:
        return pd.DataFrame()

    all_agg = pd.concat(aggs, ignore_index=True)

    combined = (
        all_agg.groupby(["date", "latitude", "longitude"], as_index=False)
        .agg(
            rel_humid_min=("rel_humid_min", "min"),
            rel_humid_max=("rel_humid_max", "max"),
            rel_humid_sum=("rel_humid_sum", "sum"),
            rel_humid_count=("rel_humid_count", "sum"),
            wind_speed_sum=("wind_speed_sum", "sum"),
            wind_speed_count=("wind_speed_count", "sum"),
            vapour_press_sum=("vapour_press_sum", "sum"),
            vapour_press_count=("vapour_press_count", "sum"),
        )
    )

    combined["rel_humid_avg"] = (combined["rel_humid_sum"] / combined["rel_humid_count"]).round(1)
    combined["wind_speed_avg"] = (combined["wind_speed_sum"] / combined["wind_speed_count"]).round(1)
    combined["vapour_press"] = (combined["vapour_press_sum"] / combined["vapour_press_count"]).round(1)

    return combined[
        [
            "date",
            "latitude",
            "longitude",
            "rel_humid_avg",
            "rel_humid_max",
            "rel_humid_min",
            "wind_speed_avg",
            "vapour_press",
        ]
    ]

# -------------------------
# Uploading
# -------------------------
def upload_weather_data(storage_account_name, container_name, filepath, data, file_type="csv"):
    try:
        sas_token = os.environ.get("SAS_TOKEN")
        if not sas_token:
            raise ValueError("SAS_TOKEN not found in environment variables.")
        if not sas_token.startswith("?"):
            sas_token = "?" + sas_token

        full_sas_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{filepath}{sas_token}"
        blob_client = BlobClient.from_blob_url(full_sas_url)

        buffer = io.BytesIO()
        if file_type == "csv":
            data.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        buffer.seek(0)

        blob_client.upload_blob(buffer, overwrite=True)
        logging.info(f"Successfully uploaded {filepath} to {container_name}.")
    except Exception as e:
        logging.warning(f"Error uploading blob data: {e}")
        logging.warning(traceback.format_exc())