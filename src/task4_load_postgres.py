"""
TASK 4.1b -- โหลด clean.parquet เข้า PostgreSQL (star schema)

รัน:  python src/task4_load_postgres.py
ต้องมี: PostgreSQL ที่เปิดใช้งาน + ตั้งค่า DB_URL ในไฟล์ .env (ดู .env.example)
สคริปต์จะ: สร้างตารางตาม sql/task4_schema.sql -> เติม dim -> เติม fact
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "processed" / "clean.parquet"
SCHEMA_SQL = ROOT / "sql" / "task4_schema.sql"

THAI_MONTHS = ["มกราคม", "กุมภาพันธ์", "มีนาคม", "เมษายน", "พฤษภาคม", "มิถุนายน",
               "กรกฎาคม", "สิงหาคม", "กันยายน", "ตุลาคม", "พฤศจิกายน", "ธันวาคม"]


def main() -> None:
    load_dotenv(ROOT / ".env")
    db_url = os.getenv("DB_URL",
                       "postgresql+psycopg2://postgres:postgres@localhost:5432/pm25")
    engine = create_engine(db_url)

    facts = pd.read_parquet(PARQUET)
    facts["date"] = pd.to_datetime(facts["date"]).dt.date

    # --- สร้างตาราง (DDL) ---
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL.read_text(encoding="utf-8")))

    # --- dim_station: 1 แถวต่อสถานี ---
    dim_station = (facts[["station_id", "station_name", "district", "lat", "long"]]
                   .drop_duplicates(subset=["station_id"])
                   .sort_values("station_id")
                   .reset_index(drop=True))
    dim_station.to_sql("dim_station", engine, if_exists="append", index=False)

    # --- dim_date: 1 แถวต่อวันที่มีข้อมูล ---
    dates = pd.DataFrame({"date": sorted(facts["date"].unique())})
    dim_date = pd.DataFrame({
        "date_key": [int(d.strftime("%Y%m%d")) for d in dates["date"]],
        "date": dates["date"],
        "year": [d.year for d in dates["date"]],
        "month": [d.month for d in dates["date"]],
        "day": [d.day for d in dates["date"]],
        "month_name_th": [THAI_MONTHS[d.month - 1] for d in dates["date"]],
    })
    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)

    # --- fact_readings: แทน key ธรรมชาติด้วย surrogate key จาก dim ---
    station_map = pd.read_sql("SELECT station_key, station_id FROM dim_station", engine)
    date_map = dim_date[["date_key", "date"]]
    fact = (facts
            .merge(station_map, on="station_id", how="left")
            .merge(date_map, on="date", how="left")
            [["date_key", "station_key", "reading_ts", "hour",
              "pm25_ugm3", "exceeds_standard"]])
    fact["exceeds_standard"] = fact["exceeds_standard"].astype(bool)
    fact.to_sql("fact_readings", engine, if_exists="append", index=False)

    print(f"dim_station   : {len(dim_station)} แถว")
    print(f"dim_date      : {len(dim_date)} แถว")
    print(f"fact_readings : {len(fact)} แถว")
    print("โหลดครบแล้ว -- รันคำถามได้ที่ sql/task4_answers.sql")


if __name__ == "__main__":
    main()
