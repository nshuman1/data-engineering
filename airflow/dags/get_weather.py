import calendar
import urllib.request
import json
import os


# WeatherData = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/new%20york%20city/{year}-{month}-01/{year}-{month}-{last_day_of_month}?unitGroup=us&include=days&key=E6B9TY3PS3PNBPKC8YVBNHG6Z&contentType=json")



def get_weather_data(year, month):

    last_day_of_month = calendar.monthrange(int(year), int(month))[1]
    
    # visual_crossing_key = os.environ.get("WEATHER_API")

    weather_data = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/new%20york%20city/{year}-{month}-01/{year}-{month}-{last_day_of_month}?unitGroup=us&include=days&key={os.environ['WEATHER_API']}&contentType=json")

    weather_data_raw = weather_data.read()

    with open(f'weather_nyc_{year}_{month}.json', 'w', encoding='utf-8') as f:
        json.dump(json.loads(weather_data_raw), f, ensure_ascii=False, indent=4)

    return None


