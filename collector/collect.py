"""抓一次即時資料，寫進 <data-dir>/raw/<日期>/。

    python -m collector.collect --data-dir DIR [--feed-file 本機JSON]

--feed-file 用於離線測試，不打 API。
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from collector.snapshot import (
    FEED_URL, SNAPSHOT_FIELDS, STATION_FIELDS, FeedError,
    parse_feed, snapshot_rows, station_rows, write_csv_gz,
)

TPE = ZoneInfo("Asia/Taipei")


def fetch(url: str, attempts: int = 3, timeout: float = 20.0) -> bytes:
    last: Exception | None = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "youbike-flow-collector"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as e:  # 網路錯誤種類很多，全部重試
            last = e
            if i < attempts - 1:
                time.sleep(5 * (i + 1))
    raise FeedError(f"抓取失敗（{attempts} 次）：{last}")


def collect(data_dir: Path, raw: bytes, now: datetime) -> list[Path]:
    feed = parse_feed(raw)
    day_dir = data_dir / "raw" / now.strftime("%Y-%m-%d")
    written = []

    snap = day_dir / f"{now.strftime('%H%M%S')}.csv.gz"
    write_csv_gz(snapshot_rows(feed, now), SNAPSHOT_FIELDS, snap)
    written.append(snap)

    stations = day_dir / "stations.csv.gz"
    if not stations.exists():
        write_csv_gz(station_rows(feed, now), STATION_FIELDS, stations)
        written.append(stations)

    print(f"{now:%F %T} 寫入 {len(feed.stations)} 站（略過 {feed.skipped}）→ {snap.relative_to(data_dir)}")
    return written


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    p.add_argument("--feed-file", type=Path)
    p.add_argument("--now", help="測試用，格式 YYYY-MM-DD HH:MM:SS（Asia/Taipei）")
    a = p.parse_args(argv)

    now = datetime.strptime(a.now, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TPE) if a.now else datetime.now(TPE)
    try:
        raw = a.feed_file.read_bytes() if a.feed_file else fetch(FEED_URL)
        collect(a.data_dir, raw, now)
    except FeedError as e:
        print(f"錯誤：{e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
