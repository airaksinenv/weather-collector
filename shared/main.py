try:
    from shared.utils import fetch_weather_data, upload_weather_data
except:
    from utils import fetch_weather_data, upload_weather_data

def main():
    print("success")
    startdate = ''
    enddate = ''
    dailydf, dailytempsumdf, df3h = fetch_weather_data(startdate, enddate) 
    pass

if __name__ == "__main__":
    main()