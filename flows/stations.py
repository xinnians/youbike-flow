"""站點參照表與站名比對。

租借紀錄只有站名、沒有座標和站號，要對到即時 API 的站點清單。比對順序：
1. 完全相同（去掉 "YouBike2.0_" 前綴後）
2. data/station_aliases.csv 手動對照（改名、撤站）
3. 原始資料裡罕用字會掉成 "?"（例如「糖?文化園區」），把 ? 當任意一個字比對，唯一符合才採用
維修中心、維護所、放置場等不是站點的名稱標為 non_station。
"""
from __future__ import annotations

import csv
import json
import re
import ssl
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REF_PATH = ROOT / "raw" / "stations_ref.csv"
ALIAS_PATH = ROOT / "data" / "station_aliases.csv"

TPE_URL = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"
NTPC_URL = "https://data.ntpc.gov.tw/api/datasets/010e5b15-3823-4b20-b401-b1cf000550c5/json?page={page}&size=1000"
_PREFIX = "YouBike2.0_"
_NON_STATION = re.compile(r"(維修中心|維護所|放置場|客服中心|服務中心)")


@dataclass(frozen=True)
class Station:
    sno: str
    name: str
    city: str
    lat: float
    lon: float


def _get_json(url: str):
    ctx = ssl.create_default_context()
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT  # 部分政府站台憑證缺 SKI
    req = urllib.request.Request(url, headers={"User-Agent": "youbike-flow"})
    with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
        return json.load(r)


def fetch_reference() -> list[Station]:
    out = [
        Station(e["sno"], e["sna"].removeprefix(_PREFIX), "臺北市", float(e["latitude"]), float(e["longitude"]))
        for e in _get_json(TPE_URL)
    ]
    page = 0
    while batch := _get_json(NTPC_URL.format(page=page)):
        out += [
            Station(e["sno"], e["sna"].removeprefix(_PREFIX), "新北市", float(e["lat"]), float(e["lng"]))
            for e in batch
        ]
        page += 1
    return out


def save_reference(stations: list[Station], path: Path = REF_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sno", "name", "city", "lat", "lon"])
        w.writerows([s.sno, s.name, s.city, s.lat, s.lon] for s in stations)


def load_reference(path: Path = REF_PATH) -> list[Station]:
    with open(path, encoding="utf-8") as f:
        return [Station(r["sno"], r["name"], r["city"], float(r["lat"]), float(r["lon"])) for r in csv.DictReader(f)]


def load_aliases(path: Path = ALIAS_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return {r["old_name"]: r["new_name"] for r in csv.DictReader(f) if not r["old_name"].startswith("#")}


def resolve(names, stations: list[Station], aliases: dict[str, str]) -> dict[str, tuple[Station | None, str]]:
    """回傳 {租借紀錄站名: (對到的站或 None, 方法)}；方法為 exact/alias/wildcard/non_station/unmatched。"""
    by_name: dict[str, Station] = {}
    for s in stations:
        by_name.setdefault(s.name, s)  # 臺北市在前，同名時以臺北市為準
    result = {}
    for n in names:
        if n in by_name:
            result[n] = (by_name[n], "exact")
        elif n in aliases and aliases[n] in by_name:
            result[n] = (by_name[aliases[n]], "alias")
        elif "?" in n:
            rx = re.compile("^" + re.escape(n).replace(r"\?", ".") + "$")
            hits = [s for name, s in by_name.items() if rx.match(name)]
            result[n] = (hits[0], "wildcard") if len(hits) == 1 else (None, "unmatched")
        elif _NON_STATION.search(n):
            result[n] = (None, "non_station")
        else:
            result[n] = (None, "unmatched")
    return result


if __name__ == "__main__":
    ref = fetch_reference()
    save_reference(ref)
    by_city = {}
    for s in ref:
        by_city[s.city] = by_city.get(s.city, 0) + 1
    print(f"站點參照表 → {REF_PATH.relative_to(ROOT)}：{by_city}")
