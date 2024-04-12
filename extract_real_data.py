import requests
from dotenv import load_dotenv
import os
import requests
import json 

def call_purple_air_api(headers, parameters, url):
  try:
    # Make the GET request to the PurpleAir API
    response = requests.get(url, headers=headers, params=parameters)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response data as JSON
        data = response.json()
        return data
    else:
        print(f"Error {response.status_code}: {response.text}")
        return {}
  except requests.exceptions.Timeout:
      print("Request timed out")
  except requests.exceptions.RequestException as e:
      print(f"Request failed: {e}")


if __name__=="__main__":
    
    load_dotenv(".env")

    purple_api_key = os.getenv("PURPLE_API_KEY")
    headers = {
        "X-API-Key": purple_api_key,
        
    }
    parameters = {"fields": "date_created, pm1.0, \
                pm1.0_cf_1, pm2.5_alt, \
                pm2.5_cf_1, pm2.5_atm,\
                pm10.0, pm10.0_cf_1, \
                humidity, temperature, pressure,\
                latitude, longitude, rssi, uptime"}

    # The URL for the PurpleAir API endpoint
    url = "https://api.purpleair.com/v1/sensors"

    data = call_purple_air_api(headers, parameters, url)

    with open('data.json', 'w') as f:
        json.dump(data, f)
