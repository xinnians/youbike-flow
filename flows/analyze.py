"""站點流向分析：每站前 5 名去向／來源、每小時淨流量，平日與假日分開。

    python -m flows.analyze

輸入：raw/trips/*.parquet（flows.build_trips）、raw/stations_ref.csv（flows.stations）、
      data/calendar_*.csv（人事行政總處辦公日曆表，「是否放假=2」視為假日，含補班日判斷）
輸出（out/）：
  top_flows.csv        station, sno, day_type, direction(去向/來源), rank, other_station, other_sno, trips, trips_per_day, share_pct
  net_flow_hourly.csv  station, sno, day_type, hour, rents_per_day, returns_per_day, net_per_day（正值＝還入多於借出）
  station_match.csv    租借紀錄站名的比對結果與影響的借還次數
  summary.txt          資料期間、筆數、比對覆蓋率
限制：資料只含「在臺北市借出」的車（2026-05～07 實測），從新北騎進臺北的車不在其中，
      市界附近站點的「來源」與淨流量會偏低。借還時間只到小時。
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import duckdb

from flows.stations import load_aliases, load_reference, resolve

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
TOP_N = 5


def load_calendar(con: duckdb.DuckDBPyConnection, paths: list[Path]) -> None:
    files = ", ".join(f"'{p.as_posix()}'" for p in paths)
    con.execute(f"""
        CREATE OR REPLACE TABLE calendar AS
        SELECT strptime(西元日期, '%Y%m%d')::DATE AS d,
               CASE WHEN 是否放假 = '2' THEN '假日' ELSE '平日' END AS day_type
        FROM read_csv([{files}], header=true, all_varchar=true)
    """)


def build(con: duckdb.DuckDBPyConnection, trips_glob: str) -> dict:
    con.execute(f"CREATE OR REPLACE VIEW trips AS SELECT * FROM '{trips_glob}'")

    names = [r[0] for r in con.execute(
        "SELECT rent_station FROM trips UNION SELECT return_station FROM trips").fetchall() if r[0] is not None]
    matched = resolve(names, load_reference(), load_aliases())
    rows = [(n, s.sno if s else None, s.name if s else None, s.city if s else None, m) for n, (s, m) in matched.items()]
    con.execute("CREATE OR REPLACE TABLE match (raw_name VARCHAR, sno VARCHAR, name VARCHAR, city VARCHAR, method VARCHAR)")
    con.executemany("INSERT INTO match VALUES (?, ?, ?, ?, ?)", rows)

    # 統一站名：對得到就用參照表的正確名稱（修掉 ? 亂碼），對不到保留原名
    con.execute("""
        CREATE OR REPLACE TABLE t AS
        SELECT coalesce(mr.name, trips.rent_station) AS rent_station, mr.sno AS rent_sno, mr.city AS rent_city,
               coalesce(mt.name, trips.return_station) AS return_station, mt.sno AS return_sno,
               rent_time, return_time
        FROM trips
        LEFT JOIN match mr ON trips.rent_station = mr.raw_name
        LEFT JOIN match mt ON trips.return_station = mt.raw_name
        WHERE rent_time IS NOT NULL AND return_time IS NOT NULL
          AND trips.rent_station IS NOT NULL AND trips.return_station IS NOT NULL
    """)

    # 各日型的天數（分母）：只算資料期間內的日期
    con.execute("""
        CREATE OR REPLACE TABLE days AS
        SELECT day_type, count(*) AS n_days FROM calendar
        WHERE d BETWEEN (SELECT min(rent_time)::DATE FROM t) AND (SELECT max(rent_time)::DATE FROM t)
        GROUP BY 1
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE top_flows AS
        WITH legs AS (
          SELECT rent_station AS station, rent_sno AS sno, '去向' AS direction,
                 return_station AS other_station, return_sno AS other_sno, rent_time::DATE AS d
          FROM t WHERE rent_city = '臺北市'
          UNION ALL
          SELECT return_station, return_sno, '來源', rent_station, rent_sno, return_time::DATE
          FROM t WHERE return_sno IN (SELECT sno FROM match WHERE city = '臺北市')
        ),
        agg AS (
          SELECT station, sno, c.day_type, direction, other_station, other_sno, count(*) AS trips
          FROM legs JOIN calendar c ON legs.d = c.d
          GROUP BY ALL
        ),
        ranked AS (
          SELECT *, row_number() OVER w AS rank,
                 100.0 * trips / sum(trips) OVER (PARTITION BY station, day_type, direction) AS share_pct
          FROM agg WINDOW w AS (PARTITION BY station, day_type, direction ORDER BY trips DESC, other_station)
        )
        SELECT r.station, r.sno, r.day_type, r.direction, r.rank, r.other_station, r.other_sno, r.trips,
               round(r.trips / days.n_days, 2) AS trips_per_day, round(r.share_pct, 1) AS share_pct
        FROM ranked r JOIN days USING (day_type)
        WHERE r.rank <= {TOP_N}
        ORDER BY r.station, r.day_type, r.direction, r.rank
    """)

    con.execute("""
        CREATE OR REPLACE TABLE net_flow_hourly AS
        WITH ev AS (
          SELECT rent_station AS station, rent_sno AS sno, rent_time AS ts, 1 AS rent, 0 AS ret
          FROM t WHERE rent_city = '臺北市'
          UNION ALL
          SELECT return_station, return_sno, return_time, 0, 1
          FROM t WHERE return_sno IN (SELECT sno FROM match WHERE city = '臺北市')
        ),
        agg AS (
          SELECT station, sno, c.day_type, hour(ts) AS hour, sum(rent) AS rents, sum(ret) AS returns
          FROM ev JOIN calendar c ON ts::DATE = c.d
          GROUP BY ALL
        )
        SELECT station, sno, day_type, hour,
               round(rents / n_days, 2) AS rents_per_day,
               round(returns / n_days, 2) AS returns_per_day,
               round((returns - rents) / n_days, 2) AS net_per_day
        FROM agg JOIN days USING (day_type)
        ORDER BY station, day_type, hour
    """)

    con.execute("""
        CREATE OR REPLACE TABLE station_match AS
        WITH r AS (SELECT rent_station AS raw_name, count(*) AS rent_trips FROM trips GROUP BY 1),
             b AS (SELECT return_station AS raw_name, count(*) AS return_trips FROM trips GROUP BY 1)
        SELECT m.raw_name, m.method, m.name AS matched_name, m.sno, m.city,
               coalesce(r.rent_trips, 0) AS rent_trips, coalesce(b.return_trips, 0) AS return_trips
        FROM match m LEFT JOIN r USING (raw_name) LEFT JOIN b USING (raw_name)
        ORDER BY rent_trips + return_trips DESC
    """)

    total = con.execute("SELECT count(*) FROM trips").fetchone()[0]
    kept = con.execute("SELECT count(*) FROM t").fetchone()[0]
    rent_nomatch, ret_nomatch = con.execute(
        "SELECT count(*) FILTER (rent_sno IS NULL), count(*) FILTER (return_sno IS NULL) FROM t").fetchone()
    period = con.execute("SELECT min(rent_time)::DATE, max(rent_time)::DATE FROM t").fetchone()
    days = dict(con.execute("SELECT day_type, n_days FROM days").fetchall())
    methods = dict(con.execute(
        "SELECT method, sum(rent_trips + return_trips) FROM station_match GROUP BY 1").fetchall())
    return {
        "period": f"{period[0]} ~ {period[1]}", "days": days, "trips_total": total, "trips_used": kept,
        "trips_per_day": round(total / sum(days.values())),
        "rent_unmatched_pct": round(100 * rent_nomatch / kept, 2),
        "return_unmatched_pct": round(100 * ret_nomatch / kept, 2),
        "match_methods_trip_ends": methods,
        "stations_with_top_flows": con.execute("SELECT count(DISTINCT station) FROM top_flows").fetchone()[0],
    }


def export(con: duckdb.DuckDBPyConnection, summary: dict) -> None:
    OUT.mkdir(exist_ok=True)
    for table in ("top_flows", "net_flow_hourly", "station_match"):
        dest = OUT / f"{table}.csv"
        con.execute(f"COPY {table} TO '{dest.as_posix()}' (HEADER, DELIMITER ',')")
    with open(OUT / "summary.txt", "w", encoding="utf-8") as f:
        f.write("# 產生指令：python -m flows.analyze\n")
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")


def main() -> int:
    calendars = sorted((ROOT / "data").glob("calendar_*.csv"))
    if not calendars:
        print("缺 data/calendar_*.csv", file=sys.stderr)
        return 1
    con = duckdb.connect()
    load_calendar(con, calendars)
    summary = build(con, (ROOT / "raw" / "trips" / "*.parquet").as_posix())
    missing = con.execute(
        "SELECT count(DISTINCT rent_time::DATE) FROM t WHERE rent_time::DATE NOT IN (SELECT d FROM calendar)").fetchone()[0]
    if missing:
        print(f"錯誤：有 {missing} 天不在辦公日曆表內，請補 data/calendar_*.csv", file=sys.stderr)
        return 1
    export(con, summary)
    for k, v in summary.items():
        print(f"{k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
