import os
import io
import logging
import traceback
from datetime import datetime, timedelta

import requests
import pandas as pd
import numpy as np
from azure.storage.blob import BlobClient

# Optional local development support
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
# Common maps
# -------------------------
PARAMS_MAP = {
    "daily": [
        "DailyMeanTemperature", "MinimumTemperature24h", "MaximumTemperature24h",
        "Precipitation24h", "MaximumWind", "DailyGlobalRadiation", "VolumetricSoilWaterLayer1"
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
# Date-based fetch (your original)
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
        # NOTE: timeouts are important in Functions
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
# Datetime-based fetch (needed for hourly chunking)
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
# Hourly: fetch exactly 00:00 -> 00:00 next day in chunks
# -------------------------
def fetch_hourly_one_day(date_yyyy_mm_dd: str, chunk_hours: int = 6) -> pd.DataFrame:
    """
    Fetches hourly data for exactly one day:
      date 00:00:00 -> (date+1) 00:00:00
    but splits into smaller requests (e.g. 6h chunks).
    """
    day_start = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)

    dfs = []
    cur = day_start
    while cur < day_end:
        nxt = min(cur + timedelta(hours=chunk_hours), day_end)
        logging.info(f"Hourly chunk: {cur} -> {nxt}")
        dfs.append(fetch_fmi_data_timerange(cur, nxt, "hourly"))
        cur = nxt

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# -------------------------
# Feature engineering (unchanged)
# -------------------------
def vapour_pressure(temp_c, rel_humid):
    es = 0.6108 * np.exp((17.27 * temp_c) / (temp_c + 237.3))  # kPa
    ea = es * (rel_humid / 100.0)  # actual vapor pressure
    return ea * 10

def calculate_daily_from_hourly(hourlydf, dailydf):
    hourlydf["timestamp"] = pd.to_datetime(hourlydf["timestamp"])
    dailydf["timestamp"] = pd.to_datetime(dailydf["timestamp"])

    hourlydf["date"] = hourlydf["timestamp"].dt.date
    dailydf["date"] = dailydf["timestamp"].dt.date

    hourlydf["vapour_press"] = vapour_pressure(hourlydf["Temperature"], hourlydf["Humidity"])

    agg_df = (
        hourlydf.groupby(["date", "latitude", "longitude"])
        .agg({
            "Humidity": ["min", "max", "mean"],
            "WindSpeedMS": ["mean"],
            "vapour_press": ["mean"]
        })
    )

    agg_df.columns = ["_".join(col) for col in agg_df.columns]
    agg_df = agg_df.reset_index()

    agg_df = agg_df.rename(columns={
        "Humidity_mean": "rel_humid_avg",
        "Humidity_max": "rel_humid_max",
        "Humidity_min": "rel_humid_min",
        "WindSpeedMS_mean": "wind_speed_avg",
        "vapour_press_mean": "vapour_press"
    })

    agg_df["rel_humid_avg"] = agg_df["rel_humid_avg"].round(1)
    agg_df["wind_speed_avg"] = agg_df["wind_speed_avg"].round(1)
    agg_df["vapour_press"] = agg_df["vapour_press"].round(1)

    merged = pd.merge(dailydf, agg_df, on=["date", "latitude", "longitude"], how="left")
    merged = merged.drop(columns=["date"])

    return merged

# -------------------------
# Upload (unchanged, but log stack)
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