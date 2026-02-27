import logging
from datetime import datetime, timedelta

import pandas as pd
try:
    from shared.utils import (
        fetch_fmi_data,
        fetch_fmi_data_timerange,
        aggregate_hourly_chunk,
        combine_hourly_aggs,
        upload_weather_data,
    )
except:
    from utils import (
        fetch_fmi_data,
        fetch_fmi_data_timerange,
        aggregate_hourly_chunk,
        combine_hourly_aggs,
        upload_weather_data,
    )

def main():
    logging.info("Running main.py...")

    # NOTE: Use UTC for timer-driven jobs to avoid DST/local-time surprises
    startdate = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    enddate = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info("fetching data from kriging_suomi_daily...")
    dailydf = fetch_fmi_data(startdate, enddate, "daily")
    logging.info("Success!")

    logging.info("fetching data from kriging_suomi_synop...")
    threeH = fetch_fmi_data(startdate, enddate, "synop")
    logging.info("Success!")

    logging.info("fetching data from kriging_suomi_kasvukausi...")
    tempsum = fetch_fmi_data(startdate, enddate, "kasvukausi")
    logging.info("Success!")

    logging.info("fetching data from kriging_suomi_snow...")
    snow = fetch_fmi_data(startdate, enddate, "snow")
    logging.info("Success!")

    # ----------------------------
    # HOURLY (stream + aggregate)
    # ----------------------------
    logging.info("fetching data from kriging_suomi_hourly...")

    day_start = datetime.strptime(startdate, "%Y-%m-%d")  # 00:00
    day_end = day_start + timedelta(days=1)              # next day 00:00

    aggs = []
    cur = day_start
    while cur < day_end:
        nxt = min(cur + timedelta(hours=1), day_end)
        logging.info(f"Hourly chunk: {cur} -> {nxt}")

        chunk_df = fetch_fmi_data_timerange(cur, nxt, "hourly")
        chunk_df.dropna(inplace=True)

        aggs.append(aggregate_hourly_chunk(chunk_df))

        # Free memory immediately (important on consumption plans)
        del chunk_df
        cur = nxt

    logging.info("Combining hourly aggregates...")
    hourly_agg = combine_hourly_aggs(aggs)
    del aggs

    logging.info("Merging hourly aggregates into daily df...")
    dailydf["timestamp"] = pd.to_datetime(dailydf["timestamp"])
    dailydf["date"] = dailydf["timestamp"].dt.date

    dailydf = pd.merge(
        dailydf,
        hourly_agg,
        on=["date", "latitude", "longitude"],
        how="left",
    ).drop(columns=["date"])

    logging.info("Success!")

    # Upload outputs
    files = {
        "daily": dailydf,
        "3h": threeH,
        "tempsum": tempsum,
        "snow":snow
    }

    for label, df in files.items():
        logging.info(f"Processing {label}...")

        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            logging.warning(f"{label}.df had no viable rows")
            continue

        filename = f"{label}-{(datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')}.csv"
        upload_weather_data(
            storage_account_name="stweatherdata",
            container_name="weather-data",
            filepath=f"weather_history/daily-automated/{filename}",
            data=df,
            file_type="csv",
        )
        logging.info("Success!")

if __name__ == "__main__":
    main()