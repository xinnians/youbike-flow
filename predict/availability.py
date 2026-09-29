"""全站無車／無位機率：每站 × 平日/假日 × 15 分鐘時段。常用站只是圖表頁預設顯示的焦點。

    git clone --branch data --single-branch https://github.com/xinnians/youbike-flow.git ~/youbike-data
    python -m predict.availability --data-dir ~/youbike-data

輸入：收集器的 data 分支（snapshots/、stations/ 合併檔 + raw/ 當天小檔）、data/my_stations.csv、data/calendar_*.csv
輸出：
  out/availability_15min.csv   全部站點：sno, station, day_type, slot, n_days, n_snapshots,
                               p_no_bike, p_no_bike_any, p_no_dock, p_no_dock_any
  out/availability.html        常用站的機率曲線預設顯示；搜尋框可叫出任何一站
定義：
- 無車＝rent_bikes = 0；無位＝return_slots = 0
- p_no_bike：該時段所有快照中無車的比例（隨機時間到站會遇到無車的機率）
- p_no_bike_any：該時段「任一次快照無車」的天數比例（較保守）
- 排除 act ≠ 1（停用）與 info_time 落後抓取時間超過 STALE_MINUTES 的快照（站點失聯）
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from collector.compact import _SNAPSHOT_TYPES, _STATION_TYPES
from flows.analyze import load_calendar

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
STATIONS_PATH = ROOT / "data" / "my_stations.csv"
TEMPLATE = Path(__file__).with_name("availability_template.html")
STALE_MINUTES = 30
MIN_DAYS = 3  # 少於這個天數的時段在圖上淡化，提醒樣本不足
SLOTS = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]


def load_my_stations(path: Path = STATIONS_PATH) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        return {r["sno"]: r["name"] for r in csv.DictReader(f)}


def _sources(data_dir: Path, parquet_glob: str, csv_glob: str, types: dict[str, str]) -> list[str]:
    cols = ", ".join(f"'{k}': '{v}'" for k, v in types.items())
    parts = []
    if list(data_dir.glob(parquet_glob)):
        parts.append(f"SELECT {', '.join(types)} FROM '{(data_dir / parquet_glob).as_posix()}'")
    if list(data_dir.glob(csv_glob)):
        parts.append(f"SELECT * FROM read_csv('{(data_dir / csv_glob).as_posix()}', header=true, columns={{{cols}}})")
    return parts


def load_snapshots(con: duckdb.DuckDBPyConnection, data_dir: Path) -> int:
    parts = _sources(data_dir, "snapshots/*/*.parquet", "raw/*/[0-9]*.csv.gz", _SNAPSHOT_TYPES)
    if not parts:
        raise SystemExit(f"{data_dir} 裡沒有快照（snapshots/ 或 raw/）")
    con.execute(f"CREATE OR REPLACE TABLE snaps AS {' UNION ALL '.join(parts)}")
    return con.execute("SELECT count(*) FROM snaps").fetchone()[0]


def load_station_names(con: duckdb.DuckDBPyConnection, data_dir: Path) -> dict[str, str]:
    """每站最新的站名（去掉 "YouBike2.0_" 前綴）。"""
    parts = _sources(data_dir, "stations/*/*.parquet", "raw/*/stations.csv.gz", _STATION_TYPES)
    if not parts:
        return {}
    return dict(con.execute(f"""
        SELECT sno, regexp_replace(arg_max(sna, fetched_at), '^YouBike2\\.0_', '')
        FROM ({' UNION ALL '.join(parts)}) GROUP BY sno
    """).fetchall())


def compute(con: duckdb.DuckDBPyConnection, names: dict[str, str]) -> list[dict]:
    con.execute("CREATE OR REPLACE TABLE names (sno VARCHAR, station VARCHAR)")
    con.executemany("INSERT INTO names VALUES (?, ?)", list(names.items()))
    con.execute(f"""
        CREATE OR REPLACE TABLE availability AS
        WITH s AS (
          SELECT sno, fetched_at::DATE AS d,
                 strftime(time_bucket(INTERVAL 15 MINUTE, fetched_at), '%H:%M') AS slot,
                 (rent_bikes = 0)::INT AS no_bike, (return_slots = 0)::INT AS no_dock
          FROM snaps
          WHERE act = '1' AND fetched_at - info_time <= INTERVAL {STALE_MINUTES} MINUTE
        ),
        per_day AS (
          SELECT sno, d, slot, count(*) AS n, sum(no_bike) AS nb, max(no_bike) AS ab,
                 sum(no_dock) AS nd, max(no_dock) AS ad
          FROM s GROUP BY ALL
        )
        SELECT p.sno, coalesce(names.station, p.sno) AS station, c.day_type, p.slot,
               count(*) AS n_days, sum(p.n) AS n_snapshots,
               round(sum(p.nb) / sum(p.n), 3) AS p_no_bike, round(avg(p.ab), 3) AS p_no_bike_any,
               round(sum(p.nd) / sum(p.n), 3) AS p_no_dock, round(avg(p.ad), 3) AS p_no_dock_any
        FROM per_day p JOIN calendar c ON p.d = c.d LEFT JOIN names USING (sno)
        GROUP BY ALL
        ORDER BY p.sno, c.day_type, p.slot
    """)
    cur = con.execute("SELECT * FROM availability")
    keys = [d[0] for d in cur.description]
    return [dict(zip(keys, r)) for r in cur.fetchall()]


def summary(con: duckdb.DuckDBPyConnection) -> dict:
    total, t0, t1, stations = con.execute(
        "SELECT count(*), min(fetched_at), max(fetched_at), count(DISTINCT sno) FROM snaps").fetchone()
    stale = con.execute(
        f"SELECT count(*) FROM snaps WHERE act <> '1' OR fetched_at - info_time > INTERVAL {STALE_MINUTES} MINUTE").fetchone()[0]
    days = dict(con.execute("""
        SELECT c.day_type, count(DISTINCT s.fetched_at::DATE) FROM snaps s JOIN calendar c ON s.fetched_at::DATE = c.d GROUP BY 1
    """).fetchall())
    return {"stations": stations, "snapshots": total, "excluded_stale_or_inactive": stale,
            "period": f"{t0:%Y-%m-%d %H:%M} ~ {t1:%Y-%m-%d %H:%M}" if t0 else "", "days": days}


def pack(rows: list[dict], names: dict[str, str]) -> dict:
    """全站資料壓成緊湊格式給圖表頁：{sno: [站名, {日型: [無車‰[96], 無位‰[96], 天數[96], 快照數[96]]}]}，缺資料為 null。"""
    idx = {s: i for i, s in enumerate(SLOTS)}
    out: dict[str, list] = {}
    for r in rows:
        entry = out.setdefault(r["sno"], [names.get(r["sno"], r["station"]), {}])
        arrs = entry[1].setdefault(r["day_type"], [[None] * 96 for _ in range(4)])
        i = idx[r["slot"]]
        arrs[0][i] = round(r["p_no_bike"] * 1000)
        arrs[1][i] = round(r["p_no_dock"] * 1000)
        arrs[2][i] = r["n_days"]
        arrs[3][i] = r["n_snapshots"]
    return out


def render_html(rows: list[dict], names: dict[str, str], featured: dict[str, str], meta: dict) -> str:
    data = {
        "stations": pack(rows, names),
        "featured": [s for s in featured],
        "featured_names": featured,
        "meta": {**meta, "min_days": MIN_DAYS, "generated": datetime.now().strftime("%Y-%m-%d %H:%M")},
    }
    # 站名來自資料，跳脫 </ 避免提前結束 <script>
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return TEMPLATE.read_text(encoding="utf-8").replace("/*__DATA__*/null", payload)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    a = p.parse_args(argv)
    data_dir = a.data_dir.expanduser()
    featured = load_my_stations()
    con = duckdb.connect()
    load_calendar(con, sorted((ROOT / "data").glob("calendar_*.csv")))
    load_snapshots(con, data_dir)
    names = load_station_names(con, data_dir)
    rows = compute(con, names)
    meta = summary(con)
    OUT.mkdir(exist_ok=True)
    con.execute(f"COPY availability TO '{(OUT / 'availability_15min.csv').as_posix()}' (HEADER)")
    html = render_html(rows, names, featured, meta)
    (OUT / "availability.html").write_text(html, encoding="utf-8")
    missing = [f"{n}（{s}）" for s, n in featured.items() if s not in {r["sno"] for r in rows}]
    print(meta)
    print(f"→ out/availability_15min.csv（{len(rows)} 列）、out/availability.html（{len(html) / 1e6:.1f} MB）")
    if missing:
        print(f"注意：常用站沒有資料：{', '.join(missing)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
