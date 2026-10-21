-- TASK 4.1 -- Data Warehouse: Star Schema (PostgreSQL)
-- สร้าง schema ก่อน แล้วค่อยโหลดข้อมูลด้วย src/task4_load_postgres.py

DROP TABLE IF EXISTS fact_readings;
DROP TABLE IF EXISTS dim_station;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_station (
    station_key   SERIAL PRIMARY KEY,
    station_id    VARCHAR(10) NOT NULL UNIQUE,   -- ST001
    station_name  VARCHAR(100) NOT NULL,          -- ชื่อมาตรฐาน
    district      VARCHAR(50)  NOT NULL,
    lat           DOUBLE PRECISION,
    long          DOUBLE PRECISION
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,      -- yyyymmdd เช่น 20250115
    date     DATE NOT NULL UNIQUE,
    year     INT NOT NULL,
    month    INT NOT NULL,
    day      INT NOT NULL,
    month_name_th VARCHAR(15)      -- มกราคม ...
);

CREATE TABLE fact_readings (
    reading_id       BIGSERIAL PRIMARY KEY,
    date_key         INT NOT NULL REFERENCES dim_date(date_key),
    station_key      INT NOT NULL REFERENCES dim_station(station_key),
    reading_ts       TIMESTAMP NOT NULL,
    hour             INT NOT NULL,
    pm25_ugm3        DOUBLE PRECISION NOT NULL CHECK (pm25_ugm3 > 0),
    exceeds_standard BOOLEAN NOT NULL
);

CREATE INDEX idx_fact_date    ON fact_readings(date_key);
CREATE INDEX idx_fact_station ON fact_readings(station_key);
