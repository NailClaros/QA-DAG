import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
 


def mass_insert_data(data):
    try:
        conn = psycopg2.connect(os.getenv("DB_URL", "___"))
        print("\nInserting data into the database...\n")
        for row in data:
            cur = conn.cursor()
            cur.execute("""INSERT INTO aq_data.daily_measurements 
                        (location, sensor_name_units, measurement, date_inserted) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (location, sensor_name_units, date_inserted) DO NOTHING""", 
                        (row[0], row[1], row[2], row[3]))
            conn.commit()
            cur.close()
        print(f"\nData has been sent! {len(data)} rows (ATTEMPTED to be)inserted.")
    except Exception as e:
        print("Error during data insertion:", e)
    finally:
        conn.close()

def test_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )

def clear_data_test_db():
    conn = test_db()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE sensor_data;")  
    conn.commit()
    conn.close()

def get_all_data_test_db():
    conn = test_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sensor_data;")
    rows = cur.fetchall()
    conn.close()
    return rows

def insert_test_data(data):
    conn = test_db()
    for row in data:
            cur = conn.cursor()
            cur.execute("""INSERT INTO sensor_data 
                        (location, sensor_name_units, measurement, date_inserted) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (location, sensor_name_units, date_inserted) DO NOTHING""", 
                        (row[0], row[1], row[2], row[3]))
    conn.commit()
    cur.close()