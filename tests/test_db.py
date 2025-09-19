import pytest
from db import insert_test_data

def test_insert_and_retrieve(db_rows):
    test_row = ("Charlotte", "TempSensor_C", 25.5, "2025-09-17")

    # Insert one row
    insert_test_data([test_row])

    rows = db_rows()
    # Check row count
    assert len(rows) == 1, "Should insert exactly one row"

    row = rows[0]

    # Column checks
    assert row[0] is not None, "id should not be null"
    assert row[1] == "Charlotte", "location should match"
    assert row[2] == "TempSensor_C", "sensor_name_units should match"
    assert isinstance(row[3], float), "measurement should be numeric"
    assert str(row[4]) == "2025-09-17", "date_inserted should match"


@pytest.mark.parametrize("bad_row", [
    (None, "TempSensor_C", 22.0, "2025-09-17"),   # location null
    ("Room1", None, 22.0, "2025-09-17"),          # sensor_name_units null
    ("Room1", "TempSensor_C", None, "2025-09-17"),# measurement null
    ("Room1", "TempSensor_C", 22.0, None),        # date_inserted null
])
def test_insert_invalid_row_raises(bad_row, db_rows):
    with pytest.raises(Exception):
        insert_test_data([bad_row])

    assert len(db_rows()) == 0, "No rows should be inserted on failure"


def test_unique_constraint(db_rows):
    row = ("Charlotte", "TempSensor_C", 25.5, "2025-09-17")

    insert_test_data([row])
    insert_test_data([row])  # Insert duplicate (should be ignored due to ON CONFLICT)

    rows = db_rows()
    assert len(rows) == 1, "Duplicate inserts should be ignored due to unique constraint"
