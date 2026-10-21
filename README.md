# ชุดเฉลยโจทย์สอบ PM2.5 — Mini Big Data Pipeline

โครงสร้างไฟล์ตามลำดับข้อสอบ (3 ชั่วโมง / 100 คะแนน):

| ข้อ | ไฟล์ | หน้าที่ |
|---|---|---|
| — | `src/generate_sample_data.py` | สร้าง "ไฟล์ที่อาจารย์แจก" (ฝังปัญหาคุณภาพไว้) — รันครั้งแรกก่อน |
| Task 1 | `src/task1_raw_zone.py` | Raw zone + manifest (checksum/partition) |
| Task 2 | `src/task2_profiling.py` | รายงานคุณภาพข้อมูล → `output/profiling_report.md` |
| Task 3 | `spark/task3_cleaning.py` | PySpark cleaning → `data/processed/clean.parquet` |
| Task 4 | `sql/task4_schema.sql` + `src/task4_load_postgres.py` + `sql/task4_answers.sql` | Star schema + โหลด + คำตอบ SQL |
| Task 5 | `powerbi/task5_dax_measures.md` หรือ `src/task5_python_charts.py` | Dashboard (Power BI) หรือกราฟ Python แทน |
| ส่งงาน | `REPORT.md`, `DATA_CONTRACT.md`, `AI_USAGE.md` | ส่วนเขียนอธิบาย 10 คะแนน |

## ลำดับการรัน

```bash
pip install -r requirements.txt

python src/generate_sample_data.py     # ได้ data/source/* (จำลองไฟล์ข้อสอบ)
python src/task1_raw_zone.py           # -> data/raw/pm25/...
python src/task2_profiling.py          # -> output/profiling_report.md
python spark/task3_cleaning.py         # ต้องมี Java (8/11/17) ในเครื่อง
python src/task4_load_postgres.py      # ต้องมี PostgreSQL + ตั้ง .env ก่อน
python src/task5_python_charts.py      # ทางเลือกแทน Power BI
```

Task 5 แบบ Power BI: เปิด `powerbi/task5_dax_measures.md` — มี DAX ครบทุก measure
ที่โจทย์ขอ + โครงหน้า dashboard + เช็กลิสต์ก่อนส่ง .pbix

## วิธีใช้ฝึกให้เหมือนสอบจริง

1. รัน `generate_sample_data.py` แล้ว **ปิดไฟล์นั้นทิ้ง** และอย่าดูเฉลยด้านล่าง
2. เริ่มจับเวลา 3 ชั่วโมง ทำ Task 2 ก่อน (profiling เพื่อหาปัญหาเอง)
3. ครบเวลาแล้วค่อยเทียบกับโค้ดเฉลยและเฉลยปัญหาที่ฝังไว้

---

## 🔑 เฉลย: ปัญหาคุณภาพที่ฝังไว้ในข้อมูลจำลอง (spoiler)

1. **หน่วยคละกัน** — ~15% เป็นหน่วย AQI (ค่าถูกคูณ 0.8 ไว้) ต้องคูณ 1.25 กลับ
   ตามกฎใน `standards.xlsx` ก่อนรวมกับ µg/m³ ไม่งั้นค่าเฉลี่ยต่ำกว่าจริง
2. **ค่าติดลบ** ~0.5% (เซนเซอร์ขัดข้อง) → drop พร้อมรายงานจำนวน
3. **แถวซ้ำ** ตาม (station_id, reading_time) 25 แถว
4. **ชื่อสถานีหลายแบบ** — 1 สถานีปรากฏได้ 3 ชื่อ:
   `สถานีตรวจวัดอากาศดินแดง` / `สถ.ดินแดง` / `ดินแดง` → fix ด้วย join `stations.csv`
5. **วันที่ 2 ฟอร์แมต** — ISO ค.ศ. ~80% ปน `dd/MM/yyyy` พ.ศ. ~20%
   กับดัก: ต้องลบ 543 เฉพาะแบบ พ.ศ. ถ้าลบทุกแถว ปีจะเป็น 1482

ขนาดข้อมูล: 20 สถานี × 60 วัน × 4 ชั่วโมง = 4,800 แถว (+25 ซ้ำ)
