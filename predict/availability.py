"""常用站的無車／無位機率：每站 × 平日/假日 × 15 分鐘時段。

    git clone --branch data --single-branch https://github.com/xinnians/youbike-flow.git ~/youbike-data
    python -m predict.availability --data-dir ~/youbike-data

輸入：收集器的 data 分支（snapshots/ 合併檔 + raw/ 當天小檔）、data/my_stations.csv、data/calendar_*.csv
輸出：
  out/availability_15min.csv   sno, station, day_type, slot, n_days, n_snapshots,
                               p_no_bike, p_no_bike_any, p_no_dock, p_no_dock_any
  out/availability.html        每站的機率曲線（平日／假日並排），滑鼠移上去看數值與樣本數
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

from collector.compact import _SNAPSHOT_TYPES
from flows.analyze import load_calendar

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
STATIONS_PATH = ROOT / "data" / "my_stations.csv"
TEMPLATE = Path(__file__).with_name("availability_template.html")
STALE_MINUTES = 30
MIN_DAYS = 3  # 少於這個天數的時段在圖上淡化，提醒樣本不足


def load_my_stations(path: Path = STATIONS_PATH) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        return {r["sno"]: r["name"] for r in csv.DictReader(f)}


def load_snapshots(con: duckdb.DuckDBPyConnection, data_dir: Path, snos: list[str]) -> int:
    cols = ", ".join(f"'{k}': '{v}'" for k, v in _SNAPSHOT_TYPES.items())
    parts = []
    if list(data_dir.glob("snapshots/*/*.parquet")):
        parts.append(f"SELECT {', '.join(_SNAPSHOT_TYPES)} FROM '{(data_dir / 'snapshots/*/*.parquet').as_posix()}'")
    if list(data_dir.glob("raw/*/[0-9]*.csv.gz")):
        parts.append(f"SELECT * FROM read_csv('{(data_dir / 'raw/*/[0-9]*.csv.gz').as_posix()}', header=true, columns={{{cols}}})")
    if not parts:
        raise SystemExit(f"{data_dir} 裡沒有快照（snapshots/ 或 raw/）")
    in_list = ", ".join(f"'{s}'" for s in snos)
    con.execute(f"CREATE OR REPLACE TABLE snaps AS SELECT * FROM ({' UNION ALL '.join(parts)}) WHERE sno IN ({in_list})")
    return con.execute("SELECT count(*) FROM snaps").fetchone()[0]


def compute(con: duckdb.DuckDBPyConnection, stations: dict[str, str]) -> list[dict]:
    con.execute("CREATE OR REPLACE TABLE names (sno VARCHAR, station VARCHAR)")
    con.executemany("INSERT INTO names VALUES (?, ?)", list(stations.items()))
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
        SELECT p.sno, names.station, c.day_type, p.slot,
               count(*) AS n_days, sum(p.n) AS n_snapshots,
               round(sum(p.nb) / sum(p.n), 3) AS p_no_bike, round(avg(p.ab), 3) AS p_no_bike_any,
               round(sum(p.nd) / sum(p.n), 3) AS p_no_dock, round(avg(p.ad), 3) AS p_no_dock_any
        FROM per_day p JOIN calendar c ON p.d = c.d JOIN names USING (sno)
        GROUP BY ALL
        ORDER BY p.sno, c.day_type, p.slot
    """)
    cur = con.execute("SELECT * FROM availability")
    keys = [d[0] for d in cur.description]
    return [dict(zip(keys, r)) for r in cur.fetchall()]


def summary(con: duckdb.DuckDBPyConnection) -> dict:
    total, kept_min, kept_max = con.execute("SELECT count(*), min(fetched_at), max(fetched_at) FROM snaps").fetchone()
    stale = con.execute(
        f"SELECT count(*) FROM snaps WHERE act <> '1' OR fetched_at - info_time > INTERVAL {STALE_MINUTES} MINUTE").fetchone()[0]
    days = dict(con.execute("""
        SELECT c.day_type, count(DISTINCT s.fetched_at::DATE) FROM snaps s JOIN calendar c ON s.fetched_at::DATE = c.d GROUP BY 1
    """).fetchall())
    return {"snapshots": total, "excluded_stale_or_inactive": stale,
            "period": f"{kept_min:%Y-%m-%d %H:%M} ~ {kept_max:%Y-%m-%d %H:%M}" if kept_min else "",
            "days": days}


def render_html(rows: list[dict], stations: dict[str, str], meta: dict) -> str:
    data = {
        "stations": [{"sno": s, "name": n} for s, n in stations.items()],
        "rows": [{k: r[k] for k in ("sno", "day_type", "slot", "n_days", "n_snapshots", "p_no_bike", "p_no_dock")}
                 for r in rows],
        "meta": {**meta, "min_days": MIN_DAYS, "generated": datetime.now().strftime("%Y-%m-%d %H:%M")},
    }
    # </script> 不可能出現在資料裡（站名來自自己的設定檔），仍保險起見跳脫
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    return TEMPLATE.read_text(encoding="utf-8").replace("/*__DATA__*/null", payload)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", type=Path, required=True)
    a = p.parse_args(argv)
    stations = load_my_stations()
    con = duckdb.connect()
    load_calendar(con, sorted((ROOT / "data").glob("calendar_*.csv")))
    n = load_snapshots(con, a.data_dir.expanduser(), list(stations))
    if n == 0:
        print("常用站在快照裡沒有任何資料", file=sys.stderr)
        return 1
    rows = compute(con, stations)
    meta = summary(con)
    OUT.mkdir(exist_ok=True)
    con.execute(f"COPY availability TO '{(OUT / 'availability_15min.csv').as_posix()}' (HEADER)")
    (OUT / "availability.html").write_text(render_html(rows, stations, meta), encoding="utf-8")
    print(f"{meta}")
    print(f"→ out/availability_15min.csv（{len(rows)} 列）、out/availability.html")
    return 0


if __name__ == "__main__":
    sys.exit(main())
