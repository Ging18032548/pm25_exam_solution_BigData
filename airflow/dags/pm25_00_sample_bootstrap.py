"""
DAG 00 -- pm25_00_sample_bootstrap
จำลอง "ไฟล์ที่อาจารย์แจก" แล้วเตรียมข้อมูลดิบให้พร้อมสำหรับสายพานประมวลผล

ทริกเกอร์: manual (schedule=None) -- ใช้ตอน demo/ฝึกโดยไม่ต้องพึ่ง API ภายนอก

ขั้นตอน:
  1. generate_sample_data  -- สร้าง data/source/* (ข้ามอัตโนมัติถ้าไฟล์มีอยู่แล้ว
     เพื่อไม่ทับข้อมูลจริงที่ ingest มา -- จะบังคับสร้างใหม่ด้วย conf {"force": true})
  2. task1_raw_zone        -- raw zone แบ่งปี/เดือน + _manifest.json (md5)
  3. task2_profiling       -- รายงานคุณภาพ output/profiling_report.md

จบงานปล่อย Dataset "pm25://data/source/pm25_readings.csv"
ซึ่งทำให้ DAG pm25_02_processing_pipeline (task3-5) รันต่ออัตโนมัติ
"""
from __future__ import annotations

import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# กันลืม: เผื่อบางสภาพแวดล้อมยังไม่ได้เพิ่ม dags folder เข้า sys.path
_DAGS_DIR = str(Path(__file__).resolve().parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from _pm25_common import PM25_SOURCE_READY, PROJECT_DIR, SOURCE_CSV, py

SOURCE = PROJECT_DIR / "data" / "source"

# ถ้าไฟล์ยังไม่มี (หรือถูกสั่ง force) ค่อย generate -- กันทับข้อมูลจริงที่ ingest มา
# (ใน f-string ต้องเขียนวงเล็บปีกกาคู่ {{ }} เพื่อให้เหลือ { } ตัวเดียวใน bash)
GENERATE_CMD = f"""
if [ "{{{{ params.force }}}}" = "True" ] || [ ! -f {SOURCE_CSV} ]; then
  {py("src/generate_sample_data.py")}
else
  echo 'พบ data/source/pm25_readings.csv อยู่แล้ว -- ข้ามการ generate'
  echo 'ถ้าต้องการสร้างชุดจำลองใหม่ ให้ trigger DAG พร้อม conf: {{"force": true}}'
  echo 'หรือรันในเครื่อง: python src/air4thai_fetcher.py --fresh --source openmeteo'
fi
"""

with DAG(
    dag_id="pm25_00_sample_bootstrap",
    description="เตรียมชุดข้อมูลจำลอง (ไฟล์ที่อาจารย์แจก) แล้วเดิน task1-2 ให้พร้อม",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["pm25", "bigdata", "bootstrap"],
    params={
        "force": Param(False, type="boolean",
                       description="บังคับ generate ชุดจำลองใหม่ทับไฟล์เดิม"),
    },
) as dag:
    generate_sample_data = BashOperator(
        task_id="generate_sample_data",
        bash_command=GENERATE_CMD,
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

    generate_sample_data >> task1_raw_zone >> task2_profiling