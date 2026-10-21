# TASK 5 — Power BI: Dashboard + DAX (25 คะแนน)

## 1) นำเข้าข้อมูลและสร้างโมเดล (สำคัญที่สุด — อาจารย์ดูโมเดลก่อนดูกราฟ)

1. **Get Data → Parquet** เลือก `data/processed/clean.parquet` (ถ้าเครื่องสอบไม่มี driver ให้ export เป็น CSV ก่อนด้วย pandas บรรทัดเดียว)
2. สร้าง **date table** (Modeling → New table):

```dax
dim_date =
ADDCOLUMNS(
    CALENDAR(DATE(2025,1,1), DATE(2025,3,1)),
    "date_key", FORMAT([Date], "yyyymmdd") * 1,
    "year", YEAR([Date]),
    "month", MONTH([Date]),
    "month_name_th", SWITCH(MONTH([Date]),
        1, "ม.ค.", 2, "ก.พ.", 3, "มี.ค.", 4, "เม.ย.", 5, "พ.ค.", 6, "มิ.ย.",
        7, "ก.ค.", 8, "ส.ค.", 9, "ก.ย.", 10, "ต.ค.", 11, "พ.ย.", 12, "ธ.ค.")
)
```

3. **Mark as date table** ให้ dim_date (Table tools → Mark as date table → เลือกคอลัมน์ Date)
4. สร้าง relationship แบบ **star schema**:
   - `dim_date[date_key]` **1 → ∞** `fact_readings[date_key]`
   - `stations[station_id]` **1 → ∞** `fact_readings[station_id]`
   - (fact มี lat/long อยู่แล้ว จะทำแผนที่จาก fact ตรง ๆ ได้)

## 2) Measures ที่โจทย์กำหนด

```dax
Avg PM2.5 = AVERAGE(fact_readings[pm25_ugm3])

จำนวนสถานี = DISTINCTCOUNT(fact_readings[station_id])

สูงสุดรายวัน = MAXX(VALUES(dim_date[Date]), [Avg PM2.5])

-- % วันเกินมาตรฐาน (มาตรฐาน 37.5 µg/m³ เฉลี่ยรายวัน)
% วันเกินมาตรฐาน =
VAR daily =
    ADDCOLUMNS(
        SUMMARIZE(fact_readings, dim_date[Date]),
        "avg", [Avg PM2.5]
    )
RETURN
    DIVIDE(
        COUNTROWS(FILTER(daily, [avg] > 37.5)),
        COUNTROWS(daily)
    )

-- ค่าเฉลี่ยเคลื่อนที่ 7 วัน (จุดสอบหลัก: ต้องใช้ AVERAGEX + DATESINPERIOD
-- ถ้าใช้ค่าเฉลี่ยธรรมดาตัวเลขจะผิด)
ค่าเฉลี่ยเคลื่อนที่ 7 วัน =
AVERAGEX(
    DATESINPERIOD(dim_date[Date], MAX(dim_date[Date]), -6, DAY),
    CALCULATE(AVERAGE(fact_readings[pm25_ugm3]))
)
```

## 3) โครงหน้า dashboard 1 หน้า

| ตำแหน่ง | Visual | เหตุผล |
|---|---|---|
| แถวบน | Card ×3: `[Avg PM2.5]`, `[จำนวนสถานี]`, `[% วันเกินมาตรฐาน]` | KPI ผู้บริหารเห็นทันที |
| ซ้าย | Line chart: แกน X = dim_date[Date], Y = `[Avg PM2.5]` และ `[ค่าเฉลี่ยเคลื่อนที่ 7 วัน]` | เห็นแนวโน้ม + เทียบค่าเรียบ |
| ขวาบน | **Map**: lat/long จาก fact, size = `[Avg PM2.5]` | จุดที่โจทย์ให้ lat/long มาเพื่อข้อนี้ |
| ขวาล่าง | Bar chart: district, `[Avg PM2.5]` + Top N filter (5) | อันดับเขตแย่สุด |
| แถวล่าง/บนสุด | Slicer: dim_date[Date] (range) + stations[district] | โจทย์กำหนด ≥1 ตัว |

## 4) Insight 3 ข้อ (เตรียมเป็น text box ในหน้า)

1. ม.ค.–ก.พ. เป็นช่วงฝุ่นสูง (ฤดูผ่านไฟ) แล้วลดลงใน มี.ค.
2. ชั่วโมงเช้ามืด (01:00/07:00) สูงกว่าช่วงบ่าย — pattern รายวันของ PM2.5
3. เขตนอกเมือง/ฝั่งตะวันออกมีค่าเฉลี่ยต่างจากใจกลางเมือง (ดูจากแผนที่)

## ตัวตรวจก่อนส่ง .pbix

- [ ] measure ทุกตัวรันแล้วได้ตัวเลวไม่ error/ไม่ว่างเปล่า
- [ ] relationship เป็น 1 → ∞ ทั้งหมด ไม่มีเส้นประ (inactive)
- [ ] slicer ใช้งานได้จริง (ลองคลิกกรองแล้วกราฟเปลี่ยน)
- [ ] screenshot ทุกหน้าเก็บไว้ใน output/ กันไฟล์ .pbix เปิดไม่ได้ตอนตรวจ
