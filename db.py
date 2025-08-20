import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
 
conn = psycopg2.connect(os.getenv("DB_URL", "___"))

def mass_insert_data(data):
    try:
        print("\nInserting data into the database...\n")
        for row in data:
            cur = conn.cursor()
            cur.execute("""INSERT INTO aq_data.daily_measurements 
                        (location, sensor_name_units, measurement, date_inserted) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (location, sensor_name_units, date_inserted) DO NOTHING""", 
                        (row[0], row[1], row[2], row[3]))
            conn.commit()
            cur.close()
        print(f"\nData has been sent! {len(data)} rows inserted.")
    except Exception as e:
        print("Error during data insertion:", e)
    finally:
        conn.close()

