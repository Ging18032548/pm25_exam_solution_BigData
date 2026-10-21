# =============================================================
# Airflow image สำหรับโปรเจกต์ PM2.5
# - Airflow 2.10.4 (เวอร์ชันเดียวกับ repo airflow-docker)
# - เพิ่ม JDK 17 สำหรับ PySpark local mode (task3_cleaning.py)
# - เพิ่มฟอนต์ไทย (fonts-thai-tlwg) สำหรับ task5_python_charts.py
# build: docker compose build airflow
# =============================================================
FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        fonts-thai-tlwg \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# TARGETARCH = amd64 | arm64 (BuildKit ใส่ค่าให้อัตโนมัติ) -> รันได้ทั้ง Intel/Apple Silicon
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV SPARK_LOCAL_IP=127.0.0.1

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt