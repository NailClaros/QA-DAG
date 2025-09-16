import pytest
from base_ import get_daily_data_test
from db import clear_data_test_db, insert_test_data, get_all_data_test_db
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(autouse=True)
def setup_and_teardown():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        dbname=os.getenv("DB_NAME", "testdb"),
        user=os.getenv("DB_USER", "testuser"),
        password=os.getenv("DB_PASS", "testpass")
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
          id serial primary key,
          location text not null,
          sensor_name_units text not null,
          measurement real not null,
          date_inserted date not null,
          constraint uq_daily unique (location, sensor_name_units, date_inserted)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

    """Clean DB before and after every test."""
    clear_data_test_db()
    yield

def test_get_daily_data_inserts_rows():
    rows = get_daily_data_test() 
    
    insert_test_data(rows)
    
    assert len(rows) > 0, "Should insert at least one row from API"
    
    db_rows = get_all_data_test_db()
    
    assert len(db_rows) == len(rows), "DB row count should match inserted rows"

    for row in db_rows:
        for col_index, value in enumerate(row):
            assert value is not None, f"Column {col_index} in row {row} should not be None"

    for row in db_rows:
        assert isinstance(row[3], (int, float)), f"Measurement should be numeric in row {row}"