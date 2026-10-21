from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pm25_big_data_pipeline',
    default_args=default_args,
    description='End-to-end PM2.5 Data Pipeline DAG',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Task 1: จำลองการดึง/เตรียมข้อมูล Raw Zone
    # (หรือถ้ามีสคริปต์ Python แยก สามารถเรียกผ่าน python src/task1_ingest.py ได้)
    t1_ingest = BashOperator(
        task_id='raw_ingestion_task',
        bash_command='echo "Raw data ingested successfully"',
    )

    # Task 3: รันสคริปต์ทำความสะอาดข้อมูล (แปลงเป็น clean.parquet)
    t3_clean = BashOperator(
        task_id='data_cleaning_task',
        bash_command='python /opt/airflow/src/task5_python_charts.py', # ปรับเปลี่ยนตามสคริปต์จริง
    )

    # Task 5: รันสคริปต์สร้างกราฟวิเคราะห์ผลลัพธ์
    t5_charts = BashOperator(
        task_id='generate_charts_task',
        bash_command='python /opt/airflow/src/task5_python_charts.py',
    )

    # กำหนดลำดับการทำงาน (Pipeline Flow)
    t1_ingest >> t3_clean >> t5_charts