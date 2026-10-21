"""
TASK 2 -- Data Profiling (15 คะแนน)

อ่านข้อมูลดิบแล้วสร้างรายงานคุณภาพเป็น markdown ที่ output/profiling_report.md
โจทย์กำหนด: ต้องชี้ปัญหาคุณภาพอย่างน้อย 5 ข้อ "พร้อมตัวเลขยืนยัน"

รัน: python src/task2_profiling.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "source" / "pm25_readings.csv"
OUT = ROOT / "output" / "profiling_report.md"


def main() -> None:
    df = pd.read_csv(SOURCE, encoding="utf-8")
    lines: list[str] = ["# Profiling Report -- pm25_readings.csv", ""]

    # 1) ขนาดข้อมูล
    lines += [f"- ขนาด: **{len(df):,} แถว x {df.shape[1]} คอลัมน์**"]

    # 2) missing รายคอลัมน์
    lines += ["", "## Missing ต่อคอลัมน์", ""]
    miss = (df.isna().sum() / len(df) * 100).round(2)
    lines += [f"- `{c}`: {m}% ({int(df[c].isna().sum())} แถว)"
              for c, m in miss.items() if m > 0] or ["- ไม่มี missing"]

    # 3) แถวซ้ำตาม (station_id, reading_time)
    dup_mask = df.duplicated(subset=["station_id", "reading_time"], keep=False)
    n_dup_all = int(dup_mask.sum())
    n_dup_extra = int(df.duplicated(subset=["station_id", "reading_time"]).sum())
    lines += ["", "## ความซ้ำซ้อน", "",
              f"- แถวที่ key (station_id, reading_time) ซ้ำทั้งหมด: **{n_dup_all} แถว**"
              f" (ต้องตัดทิ้งจริง {n_dup_extra} แถว)"]

    # 4) ค่า distinct ของคอลัมน์เชิงหมวดหมู่ -- จะเห็นชื่อสถานีเขียนต่างกัน
    lines += ["", "## ค่า distinct สำคัญ", ""]
    lines.append(f"- `unit`: {sorted(df['unit'].dropna().unique().tolist())}")
    lines.append(f"- จำนวนชื่อสถานีที่ไม่ซ้ำ: **{df['station_name'].nunique()}** "
                 f"(สถานีจริงมีเพียง {df['station_id'].nunique()} สถานี --> มีปัญหา!)")
    bad_names = (df.groupby("station_id")["station_name"].nunique()
                 .loc[lambda s: s > 1])
    if len(bad_names):
        sample_sid = bad_names.index[0]
        variants = sorted(df.loc[df.station_id == sample_sid, "station_name"].unique())
        lines.append(f"- ตัวอย่าง: `{sample_sid}` ปรากฏชื่อ {len(variants)} แบบ: {variants}")
    lines.append(f"- `district`: {df['district'].nunique()} เขต")

    # 5) สถิติค่าฝุ่น -- จะเห็นค่าติดลบ
    val = pd.to_numeric(df["value"], errors="coerce")
    lines += ["", "## สถิติ `value` (รวมทุกหน่วย)", "",
              f"- min = **{val.min()}** / max = {val.max()} / mean = {val.mean():.1f}",
              f"- ค่าติดลบ: **{int((val < 0).sum())} แถว** (เซนเซอร์ขัดข้อง --> ต้อง drop)"]
    unit_counts = df["unit"].value_counts().to_dict()
    lines.append(f"- สัดส่วนหน่วย: {unit_counts} "
                 "(ต้องแปลง AQI -> µg/m³ ด้วยสูตร x1.25 จาก standards.xlsx ก่อนรวมกัน)")

    # 6) ฟอร์แมตวันที่ -- ปี พ.ศ. 25xx อยู่นอกช่วง pandas Timestamp (1677-2262)
    #    จึงต้องลบ 543 ในสตริงก่อน parse (parse ตรง ๆ จะได้ NaT ทั้งหมด)
    ce_str = df["reading_time"].astype("string").str.replace(
        r"^(\d{1,2})/(\d{1,2})/(\d{4})",
        lambda m: f"{m.group(1)}/{m.group(2)}/{int(m.group(3)) - 543 if int(m.group(3)) > 2400 else int(m.group(3)):04d}",
        regex=True)
    iso = pd.to_datetime(df["reading_time"], format="%Y-%m-%d %H:%M", errors="coerce")
    be = pd.to_datetime(ce_str, format="%d/%m/%Y %H:%M", errors="coerce")
    n_iso, n_be = int(iso.notna().sum()), int(be.notna().sum())
    lines += ["", "## ฟอร์แมต `reading_time`", "",
              f"- ISO ค.ศ.: {n_iso:,} แถว | ไทย พ.ศ.: {n_be:,} แถว | อื่น ๆ: "
              f"{len(df) - n_iso - n_be} แถว",
              f"- ช่วงวันที่จริง: {min(iso.min(), be.min()):%Y-%m-%d}"
              f" ถึง {max(iso.max(), be.max()):%Y-%m-%d}"]

    # 7) สรุปปัญหา (เฉลย 5 ข้อ)
    lines += ["", "## สรุปปัญหาคุณภาพที่พบ (สำหรับ Task 3)", "",
              "1. หน่วยคละกัน 2 แบบ (µg/m³ / AQI)",
              "2. มีค่าฝุ่นติดลบ (เซนเซอร์ขัดข้อง)",
              f"3. แถวซ้ำตาม (station_id, reading_time) จำนวน {n_dup_extra} แถว",
              f"4. ชื่อสถานี 1 รหัสมีหลายรูปแบบเขียน ({df['station_name'].nunique()} "
              f"ชื่อ / {df['station_id'].nunique()} สถานี)",
              f"5. วันที่ 2 ฟอร์แมตคละกัน (ค.ศ. {n_iso:,} / พ.ศ. {n_be:,}) -- ต้องลบ 543 เฉพาะแบบ พ.ศ."]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"เขียนรายงานแล้ว: {OUT.relative_to(ROOT)}")
    print("\n".join(lines[-6:]))


if __name__ == "__main__":
    main()