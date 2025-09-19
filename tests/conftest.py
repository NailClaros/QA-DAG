import os
import psycopg2
import pytest
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="session")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )
    yield conn
    conn.close()

@pytest.fixture(autouse=True)
def prepare_schema(db_conn):
    cur = db_conn.cursor()
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
    db_conn.commit()
    cur.close()
    # Clean before each test
    cur = db_conn.cursor()
    cur.execute("TRUNCATE TABLE sensor_data;")
    db_conn.commit()
    cur.close()


@pytest.fixture
def db_rows(db_conn):
    def _get_all():
        cur = db_conn.cursor()
        cur.execute("SELECT id, location, sensor_name_units, measurement, date_inserted FROM sensor_data ORDER BY id;")
        rows = cur.fetchall()
        cur.close()
        return rows
    return _get_all