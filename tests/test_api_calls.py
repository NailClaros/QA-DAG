import os
from dotenv import load_dotenv
load_dotenv()
import time
from base import Testing_get_daily_sensors, Testing_get_daily_sensor_values, UNIT_LIST


API_KEY = os.getenv("QA_API_KEY", "___")
BAD_KEY = "INVALID_KEY"

def test_sucessful_daily_sensors():
    time.sleep(3)
    location_id = 1297  # Example location ID
    results, code = Testing_get_daily_sensors(location_id, API_KEY)
    assert code == 200, "API call should be successful with valid key"
    assert isinstance(results, list), "Results should be a list"
    assert sum(sensor["id"] in UNIT_LIST for sensor in results) == 3, "At least 3 sensor IDs should be in UNIT_LIST"

def test_failed_daily_sensors():
    location_id = 1297  # Example location ID
    time.sleep(3)
    results, code = Testing_get_daily_sensors(location_id, BAD_KEY)
    assert code == 401, "API call should fail with invalid key"
    assert isinstance(results, dict), f"Response should be a dictionary. got {results}"
    assert "detail" in results, "Response should contain error details"
    assert results['detail'] == "Invalid credentials", "Error message should be as expected"

def test_sucessful_daily_sensor_values():
    time.sleep(3)
    location_id = 1297 # Example sensor ID
    results, code = Testing_get_daily_sensor_values(location_id, API_KEY)
    assert code == 200, "API call should be successful with valid key"
    assert isinstance(results, list), "Results should be a list"
    assert sum(sensor["sensorsId"] in UNIT_LIST for sensor in results) == 3, "At least 3 sensor IDs should be in UNIT_LIST"

def test_failed_daily_sensor_values():
    time.sleep(3)
    location_id = 1297  # Example sensor ID
    results, code = Testing_get_daily_sensor_values(location_id, BAD_KEY)
    assert code == 401, "API call should fail with invalid key"
    assert isinstance(results, dict), f"Response should be a dictionary. got {results}"
    assert "detail" in results, "Response should contain error details"
    assert results['detail'] == "Invalid credentials", "Error message should be as expected"

