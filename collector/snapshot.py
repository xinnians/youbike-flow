"""把臺北市 YouBike 2.0 即時 JSON 轉成精簡的快照列。

只保留會隨時間變動的欄位；站名、座標等靜態資訊另存（每天第一次抓取時存一份）。
刻意只用標準函式庫，GitHub Actions 每 5 分鐘跑一次時不必安裝套件就能抓資料。
"""
from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

FEED_URL = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"

# 2026-09 實測整份約 1,800 站；低於這個數字多半是 API 回傳殘缺，寧可讓這次抓取失敗
MIN_STATIONS = 1000
# 缺必要欄位的站超過這個比例就視為格式變了，讓 workflow 以失敗告警
MAX_BAD_RATIO = 0.05

SNAPSHOT_FIELDS = [
    "fetched_at",        # 本機抓取時間（Asia/Taipei）
    "feed_update_time",  # 整份資料的 updateTime，所有站同值，只用來偵測 API 沒更新
    "sno",
    "rent_bikes",        # available_rent_bikes
    "return_slots",      # available_return_bikes
    "quantity",          # Quantity（舊格式叫 total）；不一定等於可借＋可還
    "act",               # 1 啟用、0 停用
    "info_time",         # 該站自己的資料時間，判斷站點是否失聯要看這個
]

STATION_FIELDS = [
    "fetched_at", "sno", "sna", "snaen", "sarea", "sareaen", "ar", "aren",
    "latitude", "longitude", "quantity", "act",
]

_REQUIRED = ("sno", "available_rent_bikes", "available_return_bikes", "infoTime")


class FeedError(Exception):
    """API 回傳的內容不能當作有效快照。"""


@dataclass
class ParsedFeed:
    stations: list[dict]
    skipped: int


def _quantity(entry: dict):
    return entry.get("Quantity", entry.get("total"))


def parse_feed(raw: bytes) -> ParsedFeed:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise FeedError(f"不是合法 JSON：{e}") from e
    if not isinstance(data, list):
        raise FeedError(f"預期是 list，實際是 {type(data).__name__}")
    good = [e for e in data if isinstance(e, dict) and all(e.get(k) is not None for k in _REQUIRED)]
    skipped = len(data) - len(good)
    if len(good) < MIN_STATIONS:
        raise FeedError(f"有效站點只有 {len(good)} 個（下限 {MIN_STATIONS}）")
    if skipped / len(data) > MAX_BAD_RATIO:
        raise FeedError(f"{skipped}/{len(data)} 站缺必要欄位，API 格式可能改了")
    return ParsedFeed(stations=good, skipped=skipped)


def snapshot_rows(feed: ParsedFeed, fetched_at: datetime) -> list[dict]:
    ts = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    return [
        {
            "fetched_at": ts,
            "feed_update_time": e.get("updateTime", ""),
            "sno": e["sno"],
            "rent_bikes": e["available_rent_bikes"],
            "return_slots": e["available_return_bikes"],
            "quantity": _quantity(e),
            "act": e.get("act", ""),
            "info_time": e["infoTime"],
        }
        for e in feed.stations
    ]


def station_rows(feed: ParsedFeed, fetched_at: datetime) -> list[dict]:
    ts = fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for e in feed.stations:
        row = {k: e.get(k, "") for k in STATION_FIELDS}
        row["fetched_at"] = ts
        row["quantity"] = _quantity(e)
        rows.append(row)
    return rows


def write_csv_gz(rows: list[dict], fields: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)
