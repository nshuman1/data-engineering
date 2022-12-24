import calendar
import urllib.request
import json
import os


def get_weather_data(year: str, month: str) -> None:
    """
    Function to extract weather data via Visual Crossing's weather API.
    Requires API key to be stored as an environment variable.
    Saves returned JSON data in working directory.
    """

    last_day_of_month = calendar.monthrange(int(year), int(month))[1]

    weather_data = urllib.request.urlopen(
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/new%20york%20city/{year}-{month}-01/{year}-{month}-{last_day_of_month}?unitGroup=us&include=days&key={os.getenv('WEATHER_API')}&contentType=json"
    )

    weather_data_raw = weather_data.read()

    with open(f"weather_nyc_{year}_{month}.json", "w", encoding="utf-8") as f:
        json.dump(json.loads(weather_data_raw), f, ensure_ascii=False, indent=4)

    return None
