"""
DAG 02 -- pm25_02_processing_pipeline
ประมวลผลข้อมูลดิบที่พร้อมแล้ว จนถึง warehouse + dashboard

ทริกเกอร์:
  - อัตโนมัติ: เมื่อ DAG pm25_00_sample_bootstrap หรือ pm25_01_air4thai_ingest
    จบงาน (ปล่อย Dataset "pm25://data/source/pm25_readings.csv")
  - manual: trigger เองได้ตลอด เช่น อยากรันซ้ำหลังแก้ cleaning rule

ขั้นตอน (ต่อจาก task2 ของข้อสอบ):
  task3_spark_cleaning  -- PySpark: วันที่/หน่วย/ค่าติดลบ/ชื่อสถานี/dup
                           -> data/processed/clean.parquet
  task4_load_postgres   -- star schema (dim_station, dim_date, fact_readings)
                           ลง PostgreSQL service "postgres_target" (DB pm25)
  task5_charts          -- กราฟ 3 ภาพ (แนวโน้มรายวัน / top เขต / แผนที่)
                           -> output/chart_*.png
"""
from __future__ import annotations

import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

_DAGS_DIR = str(Path(__file__).resolve().parent)
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from _pm25_common import PM25_SOURCE_READY, py

with DAG(
    dag_id="pm25_02_processing_pipeline",
    description="Spark cleaning -> โหลด PostgreSQL (star schema) -> สร้างกราฟ",
    schedule=[PM25_SOURCE_READY],        # รันเมื่อข้อมูลต้นทางพร้อม (data-aware)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["pm25", "bigdata", "processing"],
    doc_md=__doc__,
) as dag:
    task3_spark_cleaning = BashOperator(
        task_id="task3_spark_cleaning",
        bash_command=py("spark/task3_cleaning.py"),
        retries=1,
        retry_delay=pendulum.duration(minutes=3),
        execution_timeout=pendulum.duration(minutes=30),
    )

    task4_load_postgres = BashOperator(
        task_id="task4_load_postgres",
        bash_command=py("src/task4_load_postgres.py"),
        retries=2,
        retry_delay=pendulum.duration(minutes=3),
    )

    task5_charts = BashOperator(
        task_id="task5_charts",
        bash_command=py("src/task5_python_charts.py"),
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
    )

    task3_spark_cleaning >> task4_load_postgres >> task5_charts