import requests
from dotenv import load_dotenv
load_dotenv()
import os
import datetime as dt
from db import mass_insert_data


today = dt.date.today().strftime("%Y-%m-%d")
# lowbound = (dt.date.today() - dt.timedelta(days=2)).strftime("%Y-%m-%d")
# uppbound = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

# print(f"Date range: {lowbound} to {uppbound}")
# print(f"Today's date: {today}")

UNIT_LIST = [2324, 1307714, 4272428, 25516, 12915154, 1188, 1758, 3952, 4272273, 3316, 3324, 4272325]

API_KEY = os.getenv("QA_API_KEY", "___")

API_BASE = "https://api.openaq.org/v3"

LOC_DICT = {
    "Garinger, NC": 1297,
    "Washington, DC": 691,
    "Elizabeth, NJ": 971,
    "Miami, FL": 1877
}

def get_daily_data():
    print("\nFetching daily data from OpenAQ...\n")
    def get_daily_sensors(location_id):
        r = requests.get(f"{API_BASE}/locations/{location_id}/sensors", 
                        headers={"X-API-Key": API_KEY})
        return r.json()["results"]

    def get_daily_sensor_values(location_id):
        r = requests.get(f"{API_BASE}/locations/{location_id}/latest",
                        headers={"X-API-Key": API_KEY})
        return r.json()["results"]

    daily_all = {}
    daily_data = {}
    row_data = []
    for location, location_id in LOC_DICT.items():
        print(f"\n-- Fetching data for {location} (ID: {location_id}) --\n")
        sensors = get_daily_sensors(location_id)
        sensor_values = get_daily_sensor_values(location_id)

        for sensor in sensor_values:
            if sensor["sensorsId"] not in UNIT_LIST:
                continue
            else:
                sid = sensor["sensorsId"]
                daily_data[sid] = sensor["value"]

        for sensor in sensors:
            if sensor["id"] not in UNIT_LIST:
                continue
            else:
                sid = sensor["id"]

                param = str(location) + " " + sensor["parameter"]["displayName"] + " " + \
                        sensor["parameter"]["units"]

                row_data.append(
                    [
                        location,
                        sensor["parameter"]["displayName"] + " " + \
                        sensor["parameter"]["units"],
                        daily_data[sid],
                        today
                    ]
                )

                daily_all[param] = daily_data[sid]
                # print(f"Parameter: {param}, Data: {daily_data[sid]}")
    print("\nData collection complete. Preparing for database insertion...\n")
    mass_insert_data(row_data)
    return row_data

#get_daily_data()

def get_daily_data_test():
    print("\nFetching daily data from OpenAQ...\n")
    def get_daily_sensors(location_id):
        r = requests.get(f"{API_BASE}/locations/{location_id}/sensors", 
                        headers={"X-API-Key": API_KEY})
        return r.json()["results"]

    def get_daily_sensor_values(location_id):
        r = requests.get(f"{API_BASE}/locations/{location_id}/latest",
                        headers={"X-API-Key": API_KEY})
        return r.json()["results"]

    daily_all = {}
    daily_data = {}
    row_data = []
    for location, location_id in LOC_DICT.items():
        print(f"\n-- Fetching data for {location} (ID: {location_id}) --\n")
        sensors = get_daily_sensors(location_id)
        sensor_values = get_daily_sensor_values(location_id)

        for sensor in sensor_values:
            if sensor["sensorsId"] not in UNIT_LIST:
                continue
            else:
                sid = sensor["sensorsId"]
                daily_data[sid] = sensor["value"]

        for sensor in sensors:
            if sensor["id"] not in UNIT_LIST:
                continue
            else:
                sid = sensor["id"]

                param = str(location) + " " + sensor["parameter"]["displayName"] + " " + \
                        sensor["parameter"]["units"]

                row_data.append(
                    [
                        location,
                        sensor["parameter"]["displayName"] + " " + \
                        sensor["parameter"]["units"],
                        daily_data[sid],
                        today
                    ]
                )

                daily_all[param] = daily_data[sid]
                # print(f"Parameter: {param}, Data: {daily_data[sid]}")
    return row_data


# for param, data in daily_all.items():
#     print(f"\nParameter: {param}, Data: {data}")


