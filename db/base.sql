ALTER TABLE aq_data.daily_measurements
ADD CONSTRAINT uq_daily UNIQUE (location, sensor_name_units, date_inserted);


CREATE TABLE AQ_data.daily_measurements (
    id SERIAL PRIMARY KEY,
    location TEXT NOT NULL,
    sensor_name_units TEXT NOT NULL,
    measurement NUMERIC,
    date_inserted DATE NOT NULL 
);

 
create table sensor_data
(
    id                serial primary key,
    location          text not null,
    sensor_name_units text not null,
    measurement       real not null,
    date_inserted     date not null,
    constraint uq_daily
        unique (location, sensor_name_units, date_inserted)
);

