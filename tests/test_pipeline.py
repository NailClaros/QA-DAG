from base import get_daily_data_test
from db import insert_test_data

def test_get_daily_data_inserts_rows(db_rows):
    rows, daily_sensors_code, daily_sensors_values_code = get_daily_data_test() 
    assert len(rows) > 0, "Should insert at least one row from API"
    assert daily_sensors_code == 200, "Daily sensors API call should be successful"
    assert daily_sensors_values_code == 200, "Daily sensor values API call should be successful"
    
    insert_test_data(rows)
    db_result = db_rows()

    # Row counts should match
    assert len(db_result) == len(rows), "DB row count should match inserted rows"

    # No column should be None
    for row in db_result:
        for col_index, value in enumerate(row):
            assert value is not None, f"Column {col_index} in row {row} should not be None"

    # Measurement should always be numeric
    for row in db_result:
        assert isinstance(row[3], (int, float)), f"Measurement should be numeric in row {row}"