try:
    from shared.utils import fetch_fmi_data, upload_weather_data
except:
    from utils import fetch_fmi_data, upload_weather_data
from datetime import datetime, timedelta
import logging



def main():
    logging.info("Running main.py...")
    startdate = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    enddate = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logging.info('fetching data from kriging_suomi_daily...')
    dailydf = fetch_fmi_data(startdate, enddate, 'daily')
    logging.info('Success!')

    logging.info('fetching data from kriging_suomi_synop...')
    threeH = fetch_fmi_data(startdate, enddate, 'synop')
    logging.info('Success!')

    logging.info('fetching data from kriging_suomi_kasvukausi...')
    tempsum = fetch_fmi_data(startdate, enddate, 'kasvukausi')
    logging.info('Success!')

    files = {
        'daily': dailydf,
        '3h': threeH,
        'tempsum': tempsum
    }

    for label, df in files.items():
        df.dropna(inplace=True)
        df.reset_index(drop=True ,inplace=True)
        if df.empty:
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

if __name__ == "__main__":
    main()