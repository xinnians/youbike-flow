"""收集器完成條件檢查：指定期間內，至少有 1 筆快照的 15 分鐘時段比例。

    git clone --branch data --single-branch <repo> /path/to/data
    python -m collector.coverage --data-dir /path/to/data --start 2026-09-30 --days 3

達標條件（README 第 1 週）：覆蓋率 ≥ 95%。同時列出最長的連續缺口，方便判斷是偶發丟 job 還是整段停擺。
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

SLOT_MIN = 15
TARGET = 0.95


def fetch_times(data_dir: Path) -> list[datetime]:
    con = duckdb.connect()
    parts = []
    if list(data_dir.glob("snapshots/*/*.parquet")):
        parts.append(f"SELECT DISTINCT fetched_at FROM '{(data_dir / 'snapshots/*/*.parquet').as_posix()}'")
    if list(data_dir.glob("raw/*/[0-9]*.csv.gz")):
        # 還沒合併的當天小檔：抓取時間就在檔名與目錄名裡，不必讀內容
        parts.append(
            "SELECT DISTINCT strptime(regexp_extract(filename, '(\\d{4}-\\d{2}-\\d{2})/(\\d{6})', 1) || ' ' || "
            "regexp_extract(filename, '(\\d{4}-\\d{2}-\\d{2})/(\\d{6})', 2), '%Y-%m-%d %H%M%S') AS fetched_at "
            f"FROM read_text('{(data_dir / 'raw/*/[0-9]*.csv.gz').as_posix()}')"
        )
    if not parts:
        return []
    return [r[0] for r in con.execute(" UNION ".join(parts)).fetchall()]


def coverage(times: list[datetime], start: datetime, end: datetime) -> tuple[float, int, timedelta, datetime | None]:
    n_slots = int((end - start) / timedelta(minutes=SLOT_MIN))
    hit = {int((t - start) / timedelta(minutes=SLOT_MIN)) for t in times if start <= t < end}
    longest, run, run_start, gap_at = 0, 0, None, None
    for i in range(n_slots):
        if i in hit:
            run = 0
            continue
        if run == 0:
            run_start = i
        run += 1
        if run > longest:
            longest, gap_at = run, start + run_start * timedelta(minutes=SLOT_MIN)
    return len(hit) / n_slots, n_slots, longest * timedelta(minutes=SLOT_MIN), gap_at


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    p.add_argument("--start", required=True, help="YYYY-MM-DD（Asia/Taipei 00:00 起算）")
    p.add_argument("--days", type=int, default=3)
    a = p.parse_args(argv)
    start = datetime.strptime(a.start, "%Y-%m-%d")
    end = start + timedelta(days=a.days)
    ratio, n, gap, gap_at = coverage(fetch_times(a.data_dir), start, end)
    ok = ratio >= TARGET
    print(f"{a.start} 起 {a.days} 天：{ratio:.1%} 的 15 分鐘時段有快照（共 {n} 段），"
          f"最長缺口 {gap}" + (f"（{gap_at:%F %H:%M} 起）" if gap_at else "") +
          f" → {'達標' if ok else '未達標'}（門檻 {TARGET:.0%}）")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
