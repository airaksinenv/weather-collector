try:
    from shared.utils import fetch_fmi_data, upload_weather_data, calculate_daily_from_hourly
except:
    from utils import fetch_fmi_data, upload_weather_data, calculate_daily_from_hourly
from datetime import datetime, timedelta
import logging



def main():
    #print("Running main.py...")
    logging.info("Running main.py...")
    startdate = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    enddate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    #print('fetching data from kriging_suomi_daily...')
    logging.info('fetching data from kriging_suomi_daily...')
    dailydf = fetch_fmi_data(startdate, enddate, 'daily')
    logging.info('Success!')

    #print('fetching data from kriging_suomi_synop...')
    logging.info('fetching data from kriging_suomi_synop...')
    threeH = fetch_fmi_data(startdate, enddate, 'synop')
    logging.info('Success!')

    #print('fetching data from kriging_suomi_kasvukausi...')
    logging.info('fetching data from kriging_suomi_kasvukausi...')
    tempsum = fetch_fmi_data(startdate, enddate, 'kasvukausi')
    logging.info('Success!')

    #print('fetching data from kriging_suomi_hourly...')
    logging.info('fetching data from kriging_suomi_hourly...')
    hourly = fetch_fmi_data(startdate, enddate, 'hourly')
    hourly.dropna(inplace=True)
    hourly.reset_index(drop=True, inplace=True)
    #print('Calculating daily data from kriging_suomi_hourly...')
    logging.info('Calculating daily data from kriging_suomi_hourly...')
    dailydf = calculate_daily_from_hourly(hourly, dailydf)
    #print('Success!')
    logging.info('Success!')

    files = {
        'daily': dailydf,
        '3h': threeH,
        'tempsum': tempsum
    }

    for label, df in files.items():
        #print(f"Processing {label}...")
        logging.info(f"Processing {label}...")
        df.dropna(inplace=True)
        df.reset_index(drop=True ,inplace=True)
        if df.empty:
            #print(f"{label}.df had no viable rows")
            logging.warning(f'{label}.df had no viable rows')
        else:
            filename = f"{label}-{(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')}.csv"
            upload_weather_data(
                storage_account_name='stweatherdata',
                container_name='weather-data',
                filepath=f'weather_history/daily-automated/{filename}',
                data=df,
                file_type='csv'
            )
            #print("Success!")
            logging.info("Success!")
if __name__ == "__main__":
    main()