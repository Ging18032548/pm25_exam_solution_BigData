"""
TASK 3 -- Cleaning ด้วย PySpark (25 คะแนน)

ขั้นตอน (ตามลำดับที่ควรเป็นในข้อสอบ):
  3.1  อ่าน CSV ด้วย schema ที่กำหนดเอง (ห้าม inferSchema)
  3.2  แปลงวันที่ 2 ฟอร์แมต (ISO ค.ศ. / dd/MM/yyyy พ.ศ.) ให้เป็น timestamp เดียว
  3.3  แปลงหน่วย AQI -> µg/m³ (x1.25 ตาม standards.xlsx)
  3.4  ตัดค่าติดลบทิ้ง (นับจำนวนก่อนตัดเพื่อรายงาน)
  3.5  ชื่อสถานีมาตรฐานเดียว -- join กับตาราง stations.csv
  3.6  ลบ duplicate ตาม (station_id, reading_time)
  3.7  เพิ่มคอลัมน์ช่วยวิเคราะห์: hour, exceeds_standard (เทียบ 37.5 µg/m³)
  3.8  เขียนออกเป็น data/processed/clean.parquet

รัน: python spark/task3_cleaning.py   (ต้องมี Java 8/11/17 ในเครื่อง)
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "pm25"
STATIONS = ROOT / "data" / "source" / "stations.csv"
OUTPUT = ROOT / "data" / "processed" / "clean.parquet"

THAI_STANDARD_24H = 37.5   # µg/m³ จาก standards.xlsx
AQI_TO_UGM3 = 1.25         # จาก standards.xlsx


def main() -> None:
    spark = (SparkSession.builder
             .appName("pm25-cleaning")
             .master("local[*]")
             .config("spark.hadoop.io.native.lib.available", "false")
             .config("spark.sql.warehouse.dir", "file:///C:/temp")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("station_id",    StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("district",     StringType(), True),
        StructField("lat",          DoubleType(), True),
        StructField("long",         DoubleType(), True),
        StructField("reading_time", StringType(), True),
        StructField("value",        DoubleType(), True),
        StructField("unit",         StringType(), True),
    ])
    
    # ค้นหาไฟล์ part-0.csv ทั้งหมดในพาร์ติชัน raw zone มาอ่านรวมกันแบบ explicit paths ป้องกัน Windows path issue
    csv_files = [str(p) for p in RAW_DIR.glob("year=*/month=*/*.csv")]
    if not csv_files:
        raise FileNotFoundError(f"ไม่พบไฟล์ CSV ใน {RAW_DIR} กรุณารัน task1_raw_zone.py ก่อน")

    df = (spark.read.option("header", True)
          .option("encoding", "UTF-8")
          .schema(schema)
          .csv(csv_files))
    n_start = df.count()

    # --- 3.2 แปลงวันที่: ลอง ISO ก่อน ถ้าไม่ได้แปลว่าเป็น พ.ศ. ---
    iso_ts = F.to_timestamp("reading_time", "yyyy-MM-dd HH:mm")
    d = F.regexp_extract("reading_time", r"^(\d{1,2})/(\d{1,2})/(\d{4})", 1)
    m = F.regexp_extract("reading_time", r"^(\d{1,2})/(\d{1,2})/(\d{4})", 2)
    y = F.regexp_extract("reading_time", r"^(\d{1,2})/(\d{1,2})/(\d{4})", 3).cast("int") - 543
    t = F.regexp_extract("reading_time", r"(\d{1,2}:\d{2})$")
    be_ts = F.to_timestamp(F.concat_ws(" ", F.concat_ws("/", d, m, y.cast("string")), t),
                            "dd/MM/yyyy HH:mm")
    df = df.withColumn("reading_ts", F.coalesce(iso_ts, be_ts))
    n_bad_ts = df.filter(F.col("reading_ts").isNull()).count()

    # --- 3.3 แปลงหน่วยให้เป็น µg/m³ ทั้งหมด ---
    df = (df.withColumn("pm25_ugm3",
                        F.when(F.col("unit") == "AQI",
                               F.round(F.col("value") * AQI_TO_UGM3, 1))
                        .otherwise(F.col("value")))
            .drop("value", "unit", "station_name", "district", "lat", "long"))

    # --- 3.4 ตัดค่าติดลบ (นับก่อนตัดเพื่อรายงาน) ---
    n_negative = df.filter(F.col("pm25_ugm3") <= 0).count()
    df = df.filter(F.col("pm25_ugm3") > 0)

    # --- 3.5 ชื่อสถานี/เขต/พิกัด มาตรฐานเดียว: join กับตาราง stations ---
    stations = (spark.read.option("header", True).option("encoding", "UTF-8")
                .csv(str(STATIONS))
                .withColumnRenamed("station_name", "station_name_std"))
    df = (df.join(F.broadcast(stations), "station_id", "left")
            .withColumnRenamed("station_name_std", "station_name"))
    n_no_match = df.filter(F.col("station_name").isNull()).count()
    df = df.filter(F.col("station_name").isNotNull())

    # --- 3.6 ลบ duplicate ---
    n_before_dedup = df.count()
    df = df.dropDuplicates(["station_id", "reading_ts"])
    n_dup = n_before_dedup - df.count()

    # --- 3.7 คอลัมน์ช่วยวิเคราะห์ ---
    df = (df.withColumn("hour", F.hour("reading_ts"))
            .withColumn("date", F.to_date("reading_ts"))
            .withColumn("exceeds_standard", F.col("pm25_ugm3") > THAI_STANDARD_24H)
            .select("station_id", "station_name", "district", "lat", "long",
                    "reading_ts", "date", "hour", "pm25_ugm3", "exceeds_standard"))

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(OUTPUT))

    print(f"แถวตั้งต้น            : {n_start}")
    print(f"วันที่แปลงไม่ได้ (drop) : {n_bad_ts}")
    print(f"ค่าติดลบ (drop)        : {n_negative}")
    print(f"station ไม่ตรง (drop)  : {n_no_match}")
    print(f"duplicate (drop)      : {n_dup}")
    print(f"แถวหลัง clean          : {df.count()}  ->  {OUTPUT.relative_to(ROOT)}")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()