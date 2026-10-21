"""
DAG 01 -- pm25_01_air4thai_ingest
ดึงข้อมูล PM2.5 จริงทุกวัน (07:00 น. เวลาไทย ของวันที่เมื่อวาน) เข้า source CSV
แล้วเดิน task1-2 ให้ raw zone + profiling ทันที

ทริกเกอร์: schedule @daily (07:00 Asia/Bangkok) หรือ manual

แหล่งข้อมูล เลือกได้ 2 แบบ (conf {"source": ...} หรือ env PM25_DATA_SOURCE):
  - openmeteo : Open-Meteo Air Quality API -- ไม่ต้องมี key ใช้งานได้ทันที
                (ค่า PM2.5 จากโมเดล CAMS แบบ grid ใกล้พิกัดสถานี)
  - air4thai  : Air4Thai กรมควบคุมมลพิษ (ค่าสถานีตรวจวัดจริง) -- endpoint
                getAirReportV2.php ณ ต.ค. 2026 กำลังย้ายเซิร์ฟเวอร์ (404)
                เมื่อกรมเปิดใหม่ ใส่ endpoint ผ่าน env AIR4THAI_ENDPOINT ได้เลย

ผลลัพธ์: ผนวกแบบ idempotent เข้า data/source/pm25_readings.csv (dedup ตาม key)
จบงานปล่อย Dataset -> DAG pm25_02_processing_pipeline รันต่ออัตโนมัติ
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

_DAGS_DIR = str(Path(__file__).resolve().parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from _pm25_common import PM25_SOURCE_READY, py

# ค่า default อ่านจาก env ของ airflow container (ดู docker-compose.yml)
DEFAULT_SOURCE = os.getenv("PM25_DATA_SOURCE", "openmeteo")

FETCH_CMD = (
    py("src/air4thai_fetcher.py")
    + " --date {{ ds }}"
    + " --source {{ params.source }}"
)

with DAG(
    dag_id="pm25_01_air4thai_ingest",
    description="ดึงข้อมูล PM2.5 จริงรายวัน -> source CSV -> raw zone -> profiling",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["pm25", "bigdata", "ingest"],
    params={
        "source": Param(DEFAULT_SOURCE, type="string",
                        enum=["openmeteo", "air4thai"],
                        description="แหล่งข้อมูล: openmeteo หรือ air4thai"),
    },
) as dag:
    fetch_pm25 = BashOperator(
        task_id="fetch_pm25_source_csv",
        bash_command=FETCH_CMD,
        retries=3,
        retry_delay=pendulum.duration(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=pendulum.duration(minutes=30),
    )

    task1_raw_zone = BashOperator(
        task_id="task1_raw_zone",
        bash_command=py("src/task1_raw_zone.py"),
        retries=2,
        retry_delay=pendulum.duration(minutes=2),
    )

    task2_profiling = BashOperator(
        task_id="task2_profiling",
        bash_command=py("src/task2_profiling.py"),
        outlets=[PM25_SOURCE_READY],
    )

    fetch_pm25 >> task1_raw_zone >> task2_profiling