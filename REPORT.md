# REPORT.md — แม่แบบรายงานส่งข้อสอบ

> กรอกทุกหัวข้อให้จบ อาจารย์ให้คะแนนส่วนใหญ่จากตรงนี้ + ผลรันใน output/

## Task 1 — Raw Zone
- โครงสร้างพาร์ติชันที่ใช้: `data/raw/pm25/year=YYYY/month=MM/`
- เหตุผลที่แยกตามปี/เดือน: (เขียน 2–3 บรรทัด เช่น partition pruning, ตามลำดับเวลาข้อมูลมาถึง)
- เหตุผลที่ห้ามแก้ raw: (เขียน 2–3 บรรทัด เช่น audit ย้อนหลัง, ซ้ำสร้างผลลัพธ์ได้, กันทำลายต้นทาง)
- แนบผล manifest: จำนวนไฟล์ / แถว / md5

## Task 2 — Profiling
- ปัญหาคุณภาพที่พบ ≥ 5 ข้อ (ดู output/profiling_report.md) พร้อมตัวเลขยืนยัน
- 1) …  2) …  3) …  4) …  5) …

## Task 3 — Cleaning
- จำนวนแถว ก่อน → หลัง: …… → ……
- ตาราง rule ที่ใช้:

| ปัญหา | Rule ที่เลือก | เหตุผล |
|---|---|---|
| หน่วย AQI | คูณ 1.25 → µg/m³ | ตามกฎใน standards.xlsx |
| ค่าติดลบ | drop | เซนเซอร์ขัดข้อง ไม่มีทางรีสตอร์ |
| ชื่อสถานีหลายแบบ | join ตาราง stations | ใช้ station_id เป็น key ที่เชื่อถือได้ |
| duplicate | dropDuplicates ตาม (station_id, reading_ts) | … |
| วันที่ 2 ฟอร์แมต | ลบ 543 เฉพาะปี > 2400 | … |

## Task 4 — Warehouse + SQL
- ไดอะแกรม star schema (วาดง่าย ๆ หรือบรรยาย)
- ผล query 4 ข้อ แนบใน output/answers.md (query + ตารางผลลัพธ์)
- ข้อสังเกตจากผลลัพธ์ เช่น สถานี/เขตไหนแย่สุด ช่วงเวลาไหนฝุ่นสูงสุด

## Task 5 — Dashboard
- แนบ screenshot ทุกหน้า + ไฟล์ .pbix
- Insight 3 ข้อ:
  1) …  2) …  3) …

---

# DATA_CONTRACT.md — ข้อมูลสะอาด (data/processed/clean.parquet)

| คอลัมน์ | ชนิด | คำอธิบาย / ขอบเขตค่า | Nullable |
|---|---|---|---|
| station_id | string | ST001–ST020 | ไม่ |
| station_name | string | ชื่อมาตรฐาน "สถานีตรวจวัดอากาศ*" | ไม่ |
| district | string | เขตใน กทม. | ไม่ |
| lat, long | double | พิกัดสถานี | ไม่ |
| reading_ts | timestamp | ทุกค่าเป็น ค.ศ. | ไม่ |
| date | date | วันที่ (ตัดชั่วโมงออก) | ไม่ |
| hour | int | 0–23 | ไม่ |
| pm25_ugm3 | double | > 0, หน่วยเดียวคือ µg/m³ | ไม่ |
| exceeds_standard | boolean | pm25_ugm3 > 37.5 | ไม่ |

# AI_USAGE.md — การใช้ AI ในการสอบ

| งาน | ใช้ AI อย่างไร | ตรวจสอบอย่างไร |
|---|---|---|
| โค้ด cleaning | ให้ AI ร่าง when/otherwise แปลงวันที่ | รันจริง + สุ่มเช็ก 10 แถวเทียบ raw |
| DAX | ให้ AI ร่าง measure | สร้างใน .pbix จริง ดูค่าไม่ error และตรงกับ SQL ข้อ 2 |
| … | … | … |
