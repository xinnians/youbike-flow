"""把「今天以前」的 raw/<日期>/ 小檔合併成 Parquet，然後刪掉小檔。

    python -m collector.compact --data-dir DIR

輸出：
  snapshots/date=<日期>/part-<該日第一筆抓取時間>.parquet
  stations/date=<日期>/part-<同上>.parquet
檔名帶第一筆抓取時間，萬一同一天在合併後又冒出晚到的 raw 檔，也不會蓋掉既有檔案。
每 5 分鐘一個小檔直接留在 git 會讓每次 checkout 越來越慢，所以每天合併一次。
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

TPE = ZoneInfo("Asia/Taipei")

_SNAPSHOT_TYPES = {
    "fetched_at": "TIMESTAMP", "feed_update_time": "VARCHAR", "sno": "VARCHAR",
    "rent_bikes": "INTEGER", "return_slots": "INTEGER", "quantity": "INTEGER",
    "act": "VARCHAR", "info_time": "TIMESTAMP",
}
_STATION_TYPES = {
    "fetched_at": "TIMESTAMP", "sno": "VARCHAR", "sna": "VARCHAR", "snaen": "VARCHAR",
    "sarea": "VARCHAR", "sareaen": "VARCHAR", "ar": "VARCHAR", "aren": "VARCHAR",
    "latitude": "DOUBLE", "longitude": "DOUBLE", "quantity": "INTEGER", "act": "VARCHAR",
}


def _read(paths: list[Path], types: dict[str, str]) -> str:
    files = ", ".join(f"'{p.as_posix()}'" for p in paths)
    cols = ", ".join(f"'{k}': '{v}'" for k, v in types.items())
    return f"read_csv([{files}], header=true, columns={{{cols}}})"


def compact_day(con: duckdb.DuckDBPyConnection, data_dir: Path, day_dir: Path) -> list[Path]:
    day = day_dir.name
    snaps = sorted(p for p in day_dir.glob("*.csv.gz") if p.name != "stations.csv.gz")
    stations = day_dir / "stations.csv.gz"
    out = []
    if snaps:
        part = f"part-{snaps[0].name.split('.')[0]}.parquet"
        dest = data_dir / "snapshots" / f"date={day}" / part
        dest.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM {_read(snaps, _SNAPSHOT_TYPES)} ORDER BY sno, fetched_at) "
            f"TO '{dest.as_posix()}' (FORMAT parquet, COMPRESSION zstd)"
        )
        out.append(dest)
    if stations.exists():
        stem = snaps[0].name.split(".")[0] if snaps else "stations"
        dest = data_dir / "stations" / f"date={day}" / f"part-{stem}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM {_read([stations], _STATION_TYPES)} ORDER BY sno) "
            f"TO '{dest.as_posix()}' (FORMAT parquet, COMPRESSION zstd)"
        )
        out.append(dest)
    shutil.rmtree(day_dir)
    return out


def compact(data_dir: Path, today: str) -> list[Path]:
    raw = data_dir / "raw"
    if not raw.is_dir():
        return []
    con = duckdb.connect()
    written = []
    for day_dir in sorted(d for d in raw.iterdir() if d.is_dir() and d.name < today):
        written += compact_day(con, data_dir, day_dir)
        print(f"合併 {day_dir.name} → {[str(p.relative_to(data_dir)) for p in written[-2:]]}")
    return written


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    p.add_argument("--today", help="測試用，YYYY-MM-DD；預設為 Asia/Taipei 今天")
    a = p.parse_args(argv)
    compact(a.data_dir, a.today or datetime.now(TPE).strftime("%Y-%m-%d"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
