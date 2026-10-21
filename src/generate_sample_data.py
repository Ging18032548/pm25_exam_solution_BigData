"""
สร้างชุดข้อมูลจำลองสำหรับฝึกสอบ PM2.5 (จำลอง "ไฟล์ที่อาจารย์แจก" ในห้องสอบ)

รัน:  python src/generate_sample_data.py
ผลลัพธ์:
  data/source/pm25_readings.csv   ค่าฝุ่นรายสถานี (มีปัญหาคุณภาพฝังอยู่)
  data/source/stations.csv        ข้อมูลสถานี (ชื่อมาตรฐาน + lat/long) -- สำหรับ Spark join
  data/source/stations.xlsx       เดียวกันแต่เป็น Excel -- สำหรับ Power BI
  data/source/standards.xlsx      ค่ามาตรฐาน + กฎแปลงหน่วย

หมายเหตุ: ถ้าอยากฝึกเหมือนสอบจริง อย่าอ่านไฟล์นี้ -- ให้รันแล้วไปทำ Task 2 ก่อน
(เฉลยปัญหาที่ฝังไว้อยู่ท้าย README.md)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "source"
SRC.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(2025)

# (เขต, lat, long) พิกัดโดยประมาณของ 20 สถานีตรวจวัดใน กทม.
STATIONS = [
    ("ดินแดง", 13.758, 100.542), ("ราชเทวี", 13.759, 100.501),
    ("ปทุมวัน", 13.732, 100.536), ("วัฒนา", 13.724, 100.586),
    ("บางซื่อ", 13.805, 100.517), ("ห้วยขวาง", 13.771, 100.576),
    ("ลาดพร้าว", 13.807, 100.609), ("บึงกุ่ม", 13.772, 100.662),
    ("บางเขน", 13.833, 100.607), ("จตุจักร", 13.812, 100.562),
    ("ธนบุรี", 13.713, 100.486), ("คลองเตย", 13.717, 100.581),
    ("ยานนาวา", 13.722, 100.525), ("สาทร", 13.712, 100.536),
    ("ดุสิต", 13.784, 100.512), ("พญาไท", 13.779, 100.541),
    ("วังทองหลาง", 13.770, 100.630), ("สวนหลวง", 13.730, 100.655),
    ("ประเวศ", 13.712, 100.676), ("คันนายาว", 13.826, 100.673),
]

# ฤดูผ่านไฟ: ม.ค.-ก.พ. ฝุ่นสูง, มี.ค. เริ่มลด
MONTH_BASE_SD = {1: (75, 22), 2: (65, 20), 3: (48, 16)}
HOURS = [1, 7, 13, 19]
DAYS = pd.date_range("2025-01-01", "2025-03-01", freq="D")  # 60 วัน


def make_value(month: int) -> float:
    base, sd = MONTH_BASE_SD[month]
    return float(np.clip(rng.normal(base, sd), 8, 165))


def station_name_variant(canonical: str, district: str) -> str:
    r = rng.random()
    if r < 0.80:
        return canonical                      # สถานีตรวจวัดอากาศดินแดง
    if r < 0.92:
        return f"สถ.{district}"               # สถ.ดินแดง
    return district                           # ดินแดง


def main() -> None:
    rows = []
    for idx, (district, lat, long) in enumerate(STATIONS, start=1):
        sid = f"ST{idx:03d}"
        canonical = f"สถานีตรวจวัดอากาศ{district}"
        for day in DAYS:
            for hour in HOURS:
                ug = make_value(day.month)
                is_aqi = rng.random() < 0.15           # ~15% เป็นหน่วย AQI
                value = round(ug * 0.8, 1) if is_aqi else round(ug, 1)
                unit = "AQI" if is_aqi else "µg/m³"
                if rng.random() < 0.005:               # ~0.5% ค่าติดลบ
                    value = -abs(round(ug, 1))
                    unit = "µg/m³"
                if rng.random() < 0.80:                # ISO ค.ศ.
                    ts = f"{day:%Y-%m-%d} {hour:02d}:00"
                else:                                   # พ.ศ.
                    ts = f"{day:%d}/{day:%m}/{day.year + 543} {hour:02d}:00"
                rows.append({
                    "station_id": sid,
                    "station_name": station_name_variant(canonical, district),
                    "district": district,
                    "lat": lat,
                    "long": long,
                    "reading_time": ts,
                    "value": value,
                    "unit": unit,
                })

    df = pd.DataFrame(rows)

    # ฝังแถวซ้ำ (สถานี + เวลาเดียวกัน) ~25 แถว
    dups = df.sample(25, random_state=7)
    df = pd.concat([df, dups], ignore_index=True)
    df = df.sample(frac=1.0, random_state=99).reset_index(drop=True)  # สลับลำดับ

    df.to_csv(SRC / "pm25_readings.csv", index=False, encoding="utf-8")

    stations = pd.DataFrame(STATIONS, columns=["district", "lat", "long"])
    stations.insert(0, "station_id", [f"ST{i:03d}" for i in range(1, len(STATIONS) + 1)])
    stations.insert(2, "station_name",
                    [f"สถานีตรวจวัดอากาศ{d}" for d in stations["district"]])
    stations.to_csv(SRC / "stations.csv", index=False, encoding="utf-8")
    stations.to_excel(SRC / "stations.xlsx", index=False)

    standards = pd.DataFrame([
        {"parameter": "PM2.5", "averaging_period": "24 ชม.",
         "standard_value_ugm3": 37.5,
         "note": "มาตรฐานไทย เฉลี่ย 24 ชม."},
        {"parameter": "PM2.5", "averaging_period": "1 ปี",
         "standard_value_ugm3": 15.0, "note": "มาตรฐานไทย เฉลี่ยรายปี"},
        {"parameter": "CONVERSION", "averaging_period": "-",
         "standard_value_ugm3": None,
         "note": "AQI -> ug/m3 = AQI x 1.25"},
    ])
    standards.to_excel(SRC / "standards.xlsx", index=False)

    print(f"เขียนไฟล์ครบแล้วที่ {SRC}")
    print(f"pm25_readings.csv : {len(df)} แถว, {df.shape[1]} คอลัมน์")
    print(f"สถานี {len(STATIONS)} แห่ง x {len(DAYS)} วัน x {len(HOURS)} ชั่วโมง/วัน")


if __name__ == "__main__":
    main()
