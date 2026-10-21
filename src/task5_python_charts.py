"""
TASK 5 (ทางเลือก) -- กราฟด้วย Python แทน Power BI

ใช้เมื่อเครื่องสอบไม่มี Power BI Desktop หรืออาจารย์ให้เลือกเครื่องมือได้
รัน: python src/task5_python_charts.py  (ต้องรัน Task 3 ให้ได้ clean.parquet ก่อน)
ผลลัพธ์: output/chart_daily_trend.png, chart_top_district.png, chart_map.png
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "processed" / "clean.parquet"
OUT = ROOT / "output"

# ฟอนต์ไทยบน Windows
matplotlib.rcParams["font.family"] = ["Tahoma", "Leelawadee UI", "sans-serif"]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(PARQUET)
    df["date"] = pd.to_datetime(df["date"])

    # --- 1) เส้นแนวโน้มรายวัน + ค่าเฉลี่ยเคลื่อนที่ 7 วัน ---
    daily = df.groupby("date")["pm25_ugm3"].mean()
    ma7 = daily.rolling(7, min_periods=1).mean()
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(daily.index, daily.values, lw=1, alpha=0.6, label="ค่าเฉลี่ยรายวัน")
    ax.plot(ma7.index, ma7.values, lw=2.5, label="ค่าเฉลี่ยเคลื่อนที่ 7 วัน")
    ax.axhline(37.5, color="red", ls="--", lw=1, label="มาตรฐาน 37.5 µg/m³")
    ax.set_title("แนวโน้ม PM2.5 รายวัน (ทุกสถานี)")
    ax.set_ylabel("µg/m³")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "chart_daily_trend.png", dpi=150)

    # --- 2) Top 5 เขต ---
    top = (df.groupby("district")["pm25_ugm3"].mean()
             .sort_values(ascending=False).head(5))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(top.index, top.values, color="#d9534f")
    ax.set_title("5 เขตที่มีค่า PM2.5 เฉลี่ยสูงสุด")
    ax.set_ylabel("µg/m³")
    for i, v in enumerate(top.values):
        ax.text(i, v + 0.8, f"{v:.1f}", ha="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / "chart_top_district.png", dpi=150)

    # --- 3) แผนที่คร่าว ๆ จาก lat/long (scatter) ---
    st = df.groupby(["district", "lat", "long"])["pm25_ugm3"].mean().reset_index()
    fig, ax = plt.subplots(figsize=(7, 7))
    sc = ax.scatter(st["long"], st["lat"], c=st["pm25_ugm3"], cmap="Reds",
                    s=st["pm25_ugm3"] * 2.5, edgecolors="black", linewidths=0.5)
    for _, r in st.iterrows():
        ax.annotate(r["district"], (r["long"], r["lat"]),
                    fontsize=7, xytext=(3, 3), textcoords="offset points")
    fig.colorbar(sc, label="µg/m³ (เฉลี่ย)")
    ax.set_title("ค่าเฉลี่ย PM2.5 รายสถานี (ขนาด/สี = ค่าเฉลี่ย)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    fig.tight_layout()
    fig.savefig(OUT / "chart_map.png", dpi=150)

    print(f"บันทึกกราฟ 3 ภาพที่ {OUT.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
