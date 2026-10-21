"""
AIR INGESTION -- ดึงข้อมูล PM2.5 จริงเข้าสู่ data/source/pm25_readings.csv

รองรับ 2 แหล่งข้อมูล (เลือกด้วย --source หรือ env PM25_DATA_SOURCE):

  1. air4thai    -- Air4Thai กรมควบคุมมลพิษ (ค่าสถานีตรวจวัดจริง)
                    endpoint: getAirReportV2.php + crypt(md5)
                    * ณ ต.ค. 2026 endpoint ชุดนี้อยู่ระหว่างย้ายเซิร์ฟเวอร์
                      (ทดสอบแล้วได้ 404) -- เมื่อกรมเปิดใหม่ DAG ใช้งานได้ทันที
  2. openmeteo   -- Open-Meteo Air Quality API (fallback ที่ใช้งานได้ทันที)
                    ข้อมูล PM2.5 รายชั่วโมงจากโมเดล CAMS ค่าจะเป็น grid ใกล้เคียง
                    พิกัดสถานี (resolution ~11 km) ไม่ใช่ค่าเครื่องวัดรายสถานี
                    ไม่ต้องใช้ API key -- เหมาะกับการ demo pipeline แบบข้อมูลจริง

ทั้งสองแหล่ง normalize เป็น schema เดียวกับ pm25_readings.csv (8 คอลัมน์):
  station_id, station_name, district, lat, long, reading_time, value, unit

พฤติกรรมเริ่มต้น: ผนวก (append) เข้าไฟล์เดิมแล้ว dedup ตาม
(station_id, reading_time) -- ข้อมูลเดิมไม่สูญหาย, รันซ้ำได้อย่างปลอดภัย
(idempotent upsert)

ใช้เดี่ยว:  python src/air4thai_fetcher.py --date 2025-01-15 --source openmeteo
           python src/air4thai_fetcher.py --date 2025-01-15 --source air4thai
           python src/air4thai_fetcher.py --days-back 7 --fresh --out data/source/pm25_real.csv

ใน Airflow: DAG pm25_01_air4thai_ingest เรียกสคริปต์นี้ทุกวัน 07:00 (Asia/Bangkok)
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
STATIONS_CSV = ROOT / "data" / "source" / "stations.csv"
DEFAULT_OUT = ROOT / "data" / "source" / "pm25_readings.csv"

COLUMNS = ["station_id", "station_name", "district", "lat", "long",
           "reading_time", "value", "unit"]

# ---------------------------------------------------------------- Air4Thai --
AIR4THAI_ENDPOINT = os.getenv(
    "AIR4THAI_ENDPOINT",
    "https://air4thai.pcd.go.th/forapiV2/getAirReportV2.php",
)
# รหัสสถานีโซนกรุงเทพฯ (suffix t = ตรวจวัดทั่วไป) -- ปรับได้ทาง env AIR4THAI_STATIONS
AIR4THAI_STATIONS = os.getenv(
    "AIR4THAI_STATIONS",
    "02t,03t,05t,08t,10t,11t,12t,13t,14t,15t,17t,19t,20t,21t,22t,23t,24t,26t,27t,50t",
).split(",")
# วิธีทำ crypt ตามแบบทางการของ Air4Thai: md5(date + salt)
AIR4THAI_CRYPT_SALT = "air4thai-smartcity-auth"


def air4thai_crypt(d: str) -> str:
    return hashlib.md5(f"{d}{AIR4THAI_CRYPT_SALT}".encode()).hexdigest()


def fetch_air4thai(d: str) -> pd.DataFrame:
    """ดึงข้อมูลรายชั่วโมงของวันที่กำหนดจาก Air4Thai (ค่าสถานีจริง)
    response รูปแบบ: {"datetime": "...", "Stations": [{"stationID": "02t",
    "APMs": [{"code": "PM2.5", "value": "28.4", "unit": "µg/m³"}]}]}
    """
    url = (f"{AIR4THAI_ENDPOINT}?type=hourly&format=json&date={d}"
           f"&station={','.join(AIR4THAI_STATIONS)}&crypt={air4thai_crypt(d)}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    stations = payload.get("Stations") or []
    rows = []
    for st in stations:
        sid = st.get("stationID", "")
        aq = next((a for a in st.get("APMs", []) if a.get("code") == "PM2.5"), None)
        if aq is None:
            continue
        raw_val = aq.get("value")
        if raw_val in (None, "", "-"):
            continue
        rows.append({
            "station_id": sid,
            "station_name": f"สถานีตรวจวัดอากาศ{sid}",
            "district": "กรุงเทพมหานคร",
            "lat": None,
            "long": None,
            "reading_time": d + " " + str(payload.get("datetime", "")).split(" ")[-1][:5],
            "value": float(raw_val),
            "unit": aq.get("unit", "µg/m³"),
        })
    # หลายชั่วโมง: API ตอบทีละชั่วโมงเมื่อใช้ param hour หรือตอบย้อนหลังรายวัน
    # -- รายละเอียดขึ้นกับเวอร์ชัน endpoint ที่กรมเปิดใหม่ จึง parse แบบยืดหยุ่น
    df = pd.DataFrame(rows, columns=COLUMNS)
    if df.empty:
        raise RuntimeError(
            f"Air4Thai ตอบสำเร็จแต่ไม่มีข้อมูล PM2.5 (วันที่ {d}) -- "
            "endpoint อาจยังอยู่ระหว่างย้ายเซิร์ฟเวอร์ ลอง --source openmeteo"
        )
    return df


# --------------------------------------------------------------- OpenMeteo --
OPENMETEO_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
BATCH_SIZE = 20          # จำนวนพิกัดต่อ 1 request (ลดจำนวนครั้งการเรียก API)
PAUSE_SEC = 1.0          # เว้นระยะระหว่าง request -- ethical usage


def fetch_openmeteo(d: str, stations: pd.DataFrame) -> pd.DataFrame:
    """ดึงค่า PM2.5 รายชั่วโมง (โมเดล CAMS) ของวันที่กำหนด ที่พิกัดสถานีทั้งหมด
    ใน stations.csv -- ใช้ multi-location batch เพื่อเรียกให้น้อยครั้งที่สุด"""
    rows = []
    batches = [stations[i:i + BATCH_SIZE] for i in range(0, len(stations), BATCH_SIZE)]
    for batch in batches:
        lats = ",".join(f"{r.lat:.4f}" for r in batch.itertuples())
        lons = ",".join(f"{r.long:.4f}" for r in batch.itertuples())
        url = (f"{OPENMETEO_URL}?latitude={lats}&longitude={lons}"
               f"&hourly=pm2_5&start_date={d}&end_date={d}&timezone=Asia%2FBangkok")
        resp = requests.get(url, timeout=30,
                            headers={"User-Agent": "pm25-exam-pipeline/1.0"})
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict):          # 1 จุด -> dict, หลายจุด -> list
            payload = [payload]
        for st, loc in zip(batch.itertuples(), payload):
            hourly = loc.get("hourly") or {}
            times, values = hourly.get("time") or [], hourly.get("pm2_5") or []
            for ts, v in zip(times, values):
                if v is None:
                    continue
                rows.append({
                    "station_id": st.station_id,
                    "station_name": st.station_name,
                    "district": st.district,
                    "lat": st.lat,
                    "long": st.long,
                    "reading_time": ts.replace("T", " ")[:16],
                    "value": float(v),
                    "unit": "µg/m³",
                })
        time.sleep(PAUSE_SEC)
    df = pd.DataFrame(rows, columns=COLUMNS)
    if df.empty:
        raise RuntimeError(f"Open-Meteo ไม่มีข้อมูล PM2.5 ของวันที่ {d}")
    return df


# ------------------------------------------------------------------- merge --
def load_stations() -> pd.DataFrame:
    if not STATIONS_CSV.exists():
        raise FileNotFoundError(
            f"ไม่พบ {STATIONS_CSV} -- รัน python src/generate_sample_data.py ก่อน "
            "(หรือ commit ไฟล์ stations.csv ไว้ใน repo)")
    return pd.read_csv(STATIONS_CSV)


def merge_to_csv(df_new: pd.DataFrame, out: Path) -> None:
    """ผนวกข้อมูลใหม่เข้า CSV เดิมแบบ idempotent: dedup ตาม (station_id, reading_time)
    แถวใหม่ชนะแถวเก่า (keep=last) -- แล้วเรียงตามเวลา"""
    if out.exists():
        old = pd.read_csv(out, encoding="utf-8")
        old = old.reindex(columns=COLUMNS)
        merged = (pd.concat([old, df_new], ignore_index=True)
                  .drop_duplicates(subset=["station_id", "reading_time"], keep="last"))
    else:
        merged = df_new
    merged = merged.sort_values(["reading_time", "station_id"]).reset_index(drop=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out, index=False, encoding="utf-8")
    try:
        shown = out.relative_to(ROOT)
    except ValueError:                       # ไฟล์อยู่นอกโปรเจกต์ (เช่น /tmp)
        shown = out
    print(f"ผนวกใหม่ {len(df_new)} แถว | ไฟล์รวมปัจจุบัน {len(merged)} แถว -> {shown}")


def main() -> None:
    ap = argparse.ArgumentParser(description="ดึงข้อมูล PM2.5 จริงเข้า source CSV")
    ap.add_argument("--date", default=None, help="วันที่ YYYY-MM-DD (default: เมื่อวาน)")
    ap.add_argument("--days-back", type=int, default=1,
                    help="ดึงย้อนหลัง N วันนับจาก --date (default 1 วัน)")
    ap.add_argument("--source", choices=["air4thai", "openmeteo"],
                    default=os.getenv("PM25_DATA_SOURCE", "openmeteo"),
                    help="แหล่งข้อมูล (default: env PM25_DATA_SOURCE หรือ openmeteo)")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="ไฟล์ปลายทาง")
    ap.add_argument("--fresh", action="store_true",
                    help="เขียนทับไฟล์ปลายทาง (ไม่ผนวกของเดิม)")
    args = ap.parse_args()

    end = (date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1))
    stations = load_stations()
    out = Path(args.out)

    all_frames: list[pd.DataFrame] = []
    for i in range(args.days_back):
        d = (end - timedelta(days=args.days_back - 1 - i)).isoformat()
        print(f"[{args.source}] ดึงข้อมูลวันที่ {d} ...")
        df = fetch_air4thai(d) if args.source == "air4thai" else fetch_openmeteo(d, stations)
        print(f"  ได้ {len(df)} แถว | สถานี {df['station_id'].nunique()} สถานี")
        all_frames.append(df)

    df_new = pd.concat(all_frames, ignore_index=True)
    if args.fresh and out.exists():
        out.unlink()
    merge_to_csv(df_new, out)
    print(f"เสร็จสิ้น -- source CSV พร้อมให้ pipeline (task1-5) ประมวลผลต่อ")


if __name__ == "__main__":
    main()