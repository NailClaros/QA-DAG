CREATE TABLE AQ_data.daily_measurements (
    id SERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    sensor_name_units TEXT NOT NULL,
    measurement NUMERIC,
    date_inserted DATE NOT NULL DEFAULT CURRENT_DATE
);