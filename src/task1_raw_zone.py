"""
TASK 1 -- Raw Zone & Manifest (10 คะแนน)

อ่านไฟล์ดิบจากอาจารย์ (data/source/pm25_readings.csv) แล้วจัดเก็บลง raw zone
ตามโครงสร้าง data/raw/pm25/year=YYYY/month=MM/ พร้อมไฟล์ _manifest.json
ที่บันทึกจำนวนแถวและ checksum (md5) ของทุกไฟล์

ไอเดียเฉลยสำหรับ REPORT.md:
  (ก) แยกตามปี/เดือน เพื่อให้ pipeline ประมวลผลเฉพาะพาร์ติชันที่สนใจ
      (partition pruning) และตามลำดับเวลาที่ข้อมูลมาถึง
  (ข) raw คือหลักฐานต้นทางที่แก้ไม่ได้ -- ทุกการแก้ต้องเกิดในขั้นถัดไป
      เพื่อให้ย้อน audit ได้เสมอ หาก cleaning rule เปลี่ยนก็เริ่มใหม่จาก raw ได้

รัน: python src/task1_raw_zone.py
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "source" / "pm25_readings.csv"
RAW_BASE = ROOT / "data" / "raw" / "pm25"


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_mixed_datetime(series: pd.Series) -> pd.Series:
    """raw มี 2 ฟอร์แมตคละกัน: ISO yyyy-MM-dd HH:mm (ค.ศ.) และ dd/MM/yyyy HH:mm (พ.ศ.)
    ต้องแยกฟอร์แมตก่อนแปลง แล้วลบ 543 เฉพาะปี พ.ศ. (year > 2400)"""
    iso = pd.to_datetime(series, format="%Y-%m-%d %H:%M", errors="coerce")
    be = pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")
    be = be - pd.DateOffset(years=543)
    return iso.fillna(be)


def main() -> None:
    df = pd.read_csv(SOURCE, encoding="utf-8")
    dt = parse_mixed_datetime(df["reading_time"])

    # ตรวจว่าแปลงได้ครบ (ถ้าไม่ครบแปลว่ามีฟอร์แมตแปลกที่ 3 ต้องจัดการเพิ่ม)
    n_bad = int(dt.isna().sum())
    print(f"อ่าน raw {len(df)} แถว | วันที่แปลงไม่ได้: {n_bad} แถว")

    df = df.assign(_dt=dt)
    
    # กรองเฉพาะแถวที่แปลงวันที่สำเร็จ (ไม่เป็น NaT) เพื่อป้องกันข้อผิดพลาดตอนจัดกลุ่ม
    valid_df = df.dropna(subset=["_dt"]).copy()

    manifest = {
        "source_file": SOURCE.name,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "encoding": "utf-8",
        "files": [],
    }

    for (year, month), part in valid_df.groupby([valid_df["_dt"].dt.year, valid_df["_dt"].dt.month]):
        y_int = int(year)
        m_int = int(month)
        
        out_dir = RAW_BASE / f"year={y_int}" / f"month={m_int:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part-0.csv"
        
        part_cleaned = part.drop(columns=["_dt"])
        part_cleaned.to_csv(out_path, index=False, encoding="utf-8")
        
        manifest["files"].append({
            "path": str(out_path.relative_to(ROOT)).replace("\\", "/"),
            "rows": len(part_cleaned),
            "columns": len(part_cleaned.columns),
            "md5": md5_of(out_path),
        })
        print(f"  เขียน {out_path.relative_to(ROOT)} ({len(part_cleaned)} แถว)")

    manifest_path = RAW_BASE / "_manifest.json"
    RAW_BASE.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                           encoding="utf-8")
    print(f"manifest: {manifest_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
