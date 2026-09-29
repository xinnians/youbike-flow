"""用 TDX 歷史車位 API 回補全臺北指定日期，轉成收集器的快照格式，寫進 data 分支的工作目錄。

    python -m tdx.backfill --data-dir ~/youbike-data 2026-09-23 2026-09-24 2026-09-25

輸出：<data-dir>/backfill/tdx/date=<日期>/part-tdx.parquet（欄位同 snapshots/，quantity 為 NULL）
每天呼叫 1 次（全市約 10MB 壓縮後、0.6 點）；每次呼叫前檢查 probe.Ledger 的每月上限，
呼叫間隔 13 秒（基礎會員每分鐘 5 次）。已存在的日期略過，不重複扣點。
寫完後請在 data-dir 裡 commit，再由你 push。
"""
from __future__ import annotations

import argparse
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import duckdb

from tdx.probe import (
    GZIP_RATIO, MB_PER_POINT, POINTS_PER_CALL, TAIPEI_STATIONS, TPE, Ledger, ResponseTooLarge,
    fetch, get_token, load_credentials,
)

ROWS_PER_STATION_DAY = 477   # 2026-09-29 探測實測
BYTES_PER_ROW = 110
MAX_DAY_RAW_BYTES = 25_000_000  # 預期約 10MB，超過 25MB 視為異常並中斷
CALL_GAP_SECONDS = 13


def estimate_points_per_day() -> float:
    mb = TAIPEI_STATIONS * ROWS_PER_STATION_DAY * BYTES_PER_ROW * GZIP_RATIO / 1e6
    return POINTS_PER_CALL + mb / MB_PER_POINT


def convert(con: duckdb.DuckDBPyConnection, csv_body: bytes, dest: Path) -> dict:
    """TDX CSV → 快照 Parquet。回傳筆數與檢查結果。"""
    with tempfile.NamedTemporaryFile(suffix=".csv") as f:
        f.write(csv_body)
        f.flush()
        con.execute(f"CREATE OR REPLACE TEMP VIEW tdx AS SELECT * FROM read_csv('{f.name}', header=true, all_varchar=true)")
        bad_tz = con.execute("SELECT count(*) FROM tdx WHERE SrcUpdateTime NOT LIKE '%+08:00'").fetchone()[0]
        if bad_tz:
            raise ValueError(f"{bad_tz} 列的時間不是 +08:00，不能直接當臺北時間")
        dest.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT strptime(left(SrcUpdateTime, 19), '%Y-%m-%dT%H:%M:%S') AS fetched_at,
                     UpdateTime AS feed_update_time,
                     regexp_replace(StationUID, '^TPE', '') AS sno,
                     AvailableRentBikes::INTEGER AS rent_bikes,
                     AvailableReturnBikes::INTEGER AS return_slots,
                     NULL::INTEGER AS quantity,
                     CASE WHEN ServiceStatus = '1' THEN '1' ELSE '0' END AS act,
                     strptime(left(SrcUpdateTime, 19), '%Y-%m-%dT%H:%M:%S') AS info_time
              FROM tdx
              WHERE ServiceType = '2'
              ORDER BY sno, fetched_at
            ) TO '{dest.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
        rows, stations, t0, t1 = con.execute(
            f"SELECT count(*), count(DISTINCT sno), min(fetched_at), max(fetched_at) FROM '{dest.as_posix()}'").fetchone()
    return {"rows": rows, "stations": stations, "range": f"{t0} ~ {t1}"}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    p.add_argument("dates", nargs="+", help="YYYY-MM-DD")
    a = p.parse_args(argv)
    data_dir = a.data_dir.expanduser()
    month = datetime.now(TPE).strftime("%Y-%m")
    ledger = Ledger()
    todo = [d for d in a.dates if not (data_dir / "backfill" / "tdx" / f"date={d}" / "part-tdx.parquet").exists()]
    for d in sorted(set(a.dates) - set(todo)):
        print(f"{d} 已存在，略過")
    if not todo:
        return 0
    est = estimate_points_per_day()
    ledger.check(month, est * len(todo))  # 整批先檢查，避免補到一半才被擋
    print(f"預計 {len(todo)} 天 × 約 {est:.2f} 點；{month} 目前已用約 {ledger.used(month):.3f} 點")

    token = get_token(*load_credentials())
    con = duckdb.connect()
    for i, d in enumerate(todo):
        if i:
            time.sleep(CALL_GAP_SECONDS)
        ledger.check(month, est)
        try:
            raw_bytes, body, _ = fetch(token, d, top=None, max_raw_bytes=MAX_DAY_RAW_BYTES)
        except ResponseTooLarge as e:
            ledger.record(month, 1, e.args[0], 0)
            print(f"{d}：回應超過 {MAX_DAY_RAW_BYTES:,} bytes，已中斷（Content-Encoding={e.args[1]}，gzip 內容={e.args[2]}）",
                  file=sys.stderr)
            return 1
        pts = ledger.record(month, 1, raw_bytes, len(body))
        dest = data_dir / "backfill" / "tdx" / f"date={d}" / "part-tdx.parquet"
        info = convert(con, body, dest)
        print(f"{d}：傳輸 {raw_bytes / 1e6:.1f}MB（解壓後 {len(body) / 1e6:.0f}MB），約 {pts:.2f} 點；{info}")
    print(f"{month} 累計約 {ledger.used(month):.3f} 點。請在 {data_dir} commit 後 push")
    return 0


if __name__ == "__main__":
    sys.exit(main())
