-- TASK 4.2 -- คำตอบ SQL (แนบ query + ผลลัพธ์ใน output/answers.md ตอนสอบ)

-- ---------------------------------------------------------------
-- ข้อ 1) ค่าเฉลี่ยรายวันต่อสถานี (30 วันล่าสุดของข้อมูล)
-- ---------------------------------------------------------------
SELECT s.station_name,
       d.date,
       ROUND(AVG(f.pm25_ugm3), 1) AS daily_avg_ugm3
FROM fact_readings f
JOIN dim_station s ON s.station_key = f.station_key
JOIN dim_date   d ON d.date_key    = f.date_key
WHERE d.date >= (SELECT MAX(date) - INTERVAL '30 days' FROM dim_date)
GROUP BY s.station_name, d.date
ORDER BY s.station_name, d.date;


-- ---------------------------------------------------------------
-- ข้อ 2) จำนวน "วัน" ที่ค่าเฉลี่ยรายวันเกินมาตรฐาน 37.5 µg/m³ รายสถานี
--        จุดสอบ: ต้อง aggregate เป็นรายวันก่อน แล้วค่อยนับวัน -- นับแถวตรง ๆ ผิด
-- ---------------------------------------------------------------
WITH daily AS (
    SELECT f.station_key, d.date, AVG(f.pm25_ugm3) AS daily_avg
    FROM fact_readings f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY f.station_key, d.date
)
SELECT s.station_name,
       COUNT(*) AS total_days,
       COUNT(*) FILTER (WHERE daily.daily_avg > 37.5) AS exceed_days,
       ROUND(100.0 * COUNT(*) FILTER (WHERE daily.daily_avg > 37.5) / COUNT(*), 1)
           AS exceed_pct
FROM daily
JOIN dim_station s ON s.station_key = daily.station_key
GROUP BY s.station_name
ORDER BY exceed_days DESC, s.station_name;


-- ---------------------------------------------------------------
-- ข้อ 3) Top 5 เขตที่ฝุ่นเฉลี่ยสูงสุด
-- ---------------------------------------------------------------
SELECT s.district,
       ROUND(AVG(f.pm25_ugm3), 1) AS avg_ugm3,
       COUNT(*)                   AS n_readings
FROM fact_readings f
JOIN dim_station s ON s.station_key = f.station_key
GROUP BY s.district
ORDER BY avg_ugm3 DESC
LIMIT 5;


-- ---------------------------------------------------------------
-- ข้อ 4) ชั่วโมงของวันที่ฝุ่นสูงสุดโดยเฉลี่ย (หา pattern รายวัน)
-- ---------------------------------------------------------------
SELECT f.hour,
       ROUND(AVG(f.pm25_ugm3), 1) AS avg_ugm3
FROM fact_readings f
GROUP BY f.hour
ORDER BY avg_ugm3 DESC;
