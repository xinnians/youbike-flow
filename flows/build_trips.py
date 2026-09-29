"""租借紀錄 zip → 每月一個 Parquet。

    python -m flows.build_trips [raw/rentals/*.zip]

容錯：
- 編碼：UTF-8 讀不了就改用 cp950（Big5）轉碼
- 欄位名：英文（2026 實測）或中文（官方說明）都認；都不是就依官方說明的欄位順序對應
- 時間格式：- 或 / 分隔、有無秒數都接受；解析失敗的列保留但時間為 NULL，並計入報表
注意：2026-05～07 實測借還時間只到「小時」（分秒都是 0），不能做 15 分鐘粒度。
"""
from __future__ import annotations

import csv
import sys
import tempfile
import zipfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "raw" / "trips"
REPORT = ROOT / "out" / "build_report.csv"

COLS = ["rent_time", "rent_station", "return_time", "return_station", "duration", "bike_type", "info_date"]
_HEADER_ALIASES = {
    "rent_time": "rent_time", "借車時間": "rent_time",
    "rent_station": "rent_station", "借車場站": "rent_station",
    "return_time": "return_time", "還車時間": "return_time",
    "return_station": "return_station", "還車場站": "return_station",
    "rent": "duration", "借用時長": "duration",
    "bike_type": "bike_type", "車種": "bike_type",
    "infodate": "info_date", "借用日期": "info_date",
}


def detect_encoding(sample: bytes) -> str:
    # 取樣可能切在多位元組字元中間，去掉尾巴再判斷
    try:
        sample[: sample.rfind(b"\n")].decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp950"


def map_header(first_line: str) -> tuple[list[str], bool]:
    """回傳 (標準欄位名清單, 第一行是否為表頭)。"""
    cells = [c.strip().lstrip("﻿") for c in next(csv.reader([first_line]))]
    mapped = [_HEADER_ALIASES.get(c) for c in cells]
    if all(mapped) and set(mapped) == set(COLS):
        return mapped, True
    if cells and cells[0][:1].isdigit():
        return COLS, False  # 沒有表頭，第一行就是資料
    print(f"  警告：認不得表頭 {cells}，依官方欄位順序對應", file=sys.stderr)
    return COLS, True


def extract_utf8(zf: zipfile.ZipFile, member: str, dest: Path) -> tuple[str, list[str], bool]:
    with zf.open(member) as f:
        enc = detect_encoding(f.read(1 << 20))
    with zf.open(member) as src, open(dest, "wb") as out:
        if enc == "utf-8":
            while chunk := src.read(1 << 24):
                out.write(chunk)
        else:
            import io
            for line in io.TextIOWrapper(src, encoding=enc, errors="replace", newline=""):
                out.write(line.encode("utf-8"))
    with open(dest, encoding="utf-8", errors="replace") as f:
        cols, has_header = map_header(f.readline())
    return enc, cols, has_header


_TS = "coalesce(try_strptime({c}, '%Y-%m-%d %H:%M:%S'), try_strptime({c}, '%Y/%m/%d %H:%M:%S'), " \
      "try_strptime({c}, '%Y-%m-%d %H:%M'), try_strptime({c}, '%Y/%m/%d %H:%M'))"


def _duration_sec(c: str) -> str:
    parts = f"string_split({c}, ':')"
    return (f"CASE WHEN len({parts}) = 3 THEN try_cast({parts}[1] AS INT) * 3600 "
            f"+ try_cast({parts}[2] AS INT) * 60 + try_cast({parts}[3] AS INT) END")


def build_month(con: duckdb.DuckDBPyConnection, zip_path: Path) -> dict:
    month = zip_path.stem
    zf = zipfile.ZipFile(zip_path)
    members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"{zip_path.name} 內有 {len(members)} 個 CSV，預期 1 個")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUT_DIR / f"{month}.parquet"
    with tempfile.TemporaryDirectory(dir=ROOT / "raw") as tmp:
        csv_path = Path(tmp) / "trips.csv"
        enc, cols, has_header = extract_utf8(zf, members[0], csv_path)
        colspec = ", ".join(f"'{c}': 'VARCHAR'" for c in cols)
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW src AS
            SELECT * FROM read_csv('{csv_path.as_posix()}', header={str(has_header).lower()},
                                   columns={{{colspec}}}, quote='"', strict_mode=false)
        """)
        con.execute(f"""
            COPY (
              SELECT {_TS.format(c='rent_time')} AS rent_time,
                     trim(rent_station) AS rent_station,
                     {_TS.format(c='return_time')} AS return_time,
                     trim(return_station) AS return_station,
                     {_duration_sec('duration')} AS duration_sec,
                     trim(bike_type) AS bike_type,
                     coalesce(try_strptime(info_date, '%Y-%m-%d'), try_strptime(info_date, '%Y/%m/%d'))::DATE AS info_date
              FROM src
            ) TO '{dest.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
    stats = con.execute(f"""
        SELECT count(*) AS rows,
               count(*) FILTER (rent_time IS NULL OR return_time IS NULL) AS bad_time,
               count(*) FILTER (rent_station IS NULL OR return_station IS NULL) AS bad_station,
               count(*) FILTER (strftime(rent_time, '%M:%S') <> '00:00') AS rent_time_not_on_hour,
               min(rent_time) AS min_rent, max(rent_time) AS max_rent
        FROM '{dest.as_posix()}'
    """).fetchone()
    keys = ["rows", "bad_time", "bad_station", "rent_time_not_on_hour", "min_rent", "max_rent"]
    return {"month": month, "encoding": enc, "header": has_header, **dict(zip(keys, stats))}


def main(argv: list[str]) -> int:
    zips = [Path(p) for p in argv] or sorted((ROOT / "raw" / "rentals").glob("*.zip"))
    if not zips:
        print("找不到 zip，先執行 python -m flows.download", file=sys.stderr)
        return 1
    con = duckdb.connect()
    reports = []
    for z in zips:
        print(f"處理 {z.name} …")
        r = build_month(con, z)
        print(f"  {r}")
        reports.append(r)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(reports[0]))
        w.writeheader()
        w.writerows(reports)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
