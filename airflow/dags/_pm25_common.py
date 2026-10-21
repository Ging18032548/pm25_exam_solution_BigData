"""
ตัวช่วยกลางของ DAG ทั้งหมดในโปรเจกต์ PM2.5

โปรเจกต์นี้ mount โค้ดเดิมของ repo เข้ามาที่ /opt/airflow/project (ดู docker-compose.yml)
  src/   -> /opt/airflow/project/src    (read-only, ยกเว้นข้อมูล)
  spark/ -> /opt/airflow/project/spark  (read-only)
  sql/   -> /opt/airflow/project/sql    (read-only)
  data/  -> /opt/airflow/project/data   (read-write: task1 เขียน raw, task3 เขียน processed)
  output/-> /opt/airflow/project/output (read-write: profiling report + charts)

DAG จึง "ออร์เคสเตรตสคริปต์ข้อสอบเดิม" ผ่าน BashOperator โดยไม่ต้องแก้ logic ใด ๆ
"""
from __future__ import annotations

import os
from pathlib import Path

from airflow.datasets import Dataset

# โฟลเดอร์โปรเจกต์ในเครื่อง (local test) หรือใน container (override ด้วย env)
PROJECT_DIR = Path(os.getenv("PM25_PROJECT_DIR", "/opt/airflow/project"))

SRC_DIR = PROJECT_DIR / "src"
SPARK_DIR = PROJECT_DIR / "spark"
DATA_DIR = PROJECT_DIR / "data"
OUTPUT_DIR = PROJECT_DIR / "output"

SOURCE_CSV = DATA_DIR / "source" / "pm25_readings.csv"

# Dataset ที่ใช้เชื่อมสองสายพาน: DAG เตรียมข้อมูล (00/01) ปล่อย dataset นี้
# เมื่อ source CSV พร้อม -> ทำให้ DAG ประมวลผล (02) รันเองอัตโนมัติ
PM25_SOURCE_READY = Dataset("pm25://data/source/pm25_readings.csv")


def py(script_rel: str) -> str:
    """สร้างคำสั่งรันสคริปต์ python ของโปรเจกต์ เช่น py('src/task1_raw_zone.py')"""
    return f"python {PROJECT_DIR / script_rel}"