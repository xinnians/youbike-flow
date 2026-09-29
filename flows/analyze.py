"""站點流向分析：每站前 5 名去向／來源、每小時淨流量，平日與假日分開。

    python -m flows.analyze

輸入：raw/trips/*.parquet（flows.build_trips）、raw/stations_ref.csv（flows.stations）、
      data/calendar_*.csv（人事行政總處辦公日曆表，「是否放假=2」視為假日，含補班日判斷）
輸出（out/）：
  top_flows.csv        station, sno, day_type, direction(去向/來源), rank, other_station, other_sno,
                       trips, n_days(分母), trips_per_day, share_pct（同站借還不列入排名與占比分母）
  net_flow_hourly.csv  station, sno, day_type, hour, n_days, rents_per_day, returns_per_day,
                       net_per_day（正值＝還入多於借出；先相減再四捨五入）
  station_match.csv    租借紀錄站名的比對結果與影響的借還次數
  summary.txt          資料期間、筆數、比對覆蓋率
定義：
- 去向／借出量用借車時間歸日與小時，來源／還入量用還車時間，兩表不能逐筆對帳（跨日型行程約 0.09%）
- 有任何一小時完全沒有借車紀錄的日子整天排除（資料缺漏或停止營運）；下雨等真實低量日保留
- 分母＝該站營運期間（第一筆到最後一筆紀錄）內的有效日數
- share_pct 的分母含另一端對不上站名的行程
- 同站借還不進 top_flows，但仍計入 net_flow_hourly（借出到還回之間車確實不在站上）
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
               coalesce(mt.name, trips.return_station) AS return_station, mt.sno AS return_sno, mt.city AS return_city,
               rent_time, return_time
        FROM trips
        LEFT JOIN match mr ON trips.rent_station = mr.raw_name
        LEFT JOIN match mt ON trips.return_station = mt.raw_name
        WHERE rent_time IS NOT NULL AND return_time IS NOT NULL
          AND trips.rent_station IS NOT NULL AND trips.return_station IS NOT NULL
    """)

    # 所有「站 × 事件」：去向用借車時間、來源用還車時間決定日期與小時
    con.execute("""
        CREATE OR REPLACE TABLE legs AS
        SELECT rent_station AS station, rent_sno AS sno, '去向' AS direction,
               return_station AS other_station, return_sno AS other_sno, rent_time AS ts
        FROM t WHERE rent_city = '臺北市'
        UNION ALL
        SELECT return_station, return_sno, '來源', rent_station, rent_sno, return_time
        FROM t WHERE return_city = '臺北市'
    """)

    # 有任何一個小時完全沒有借車紀錄的日子整天排除（不進分子也不進分母）。
    # 臺北市凌晨每小時也有上千筆，整小時 0 筆代表資料缺漏或停止營運（例如 2026-07-10 早上 8 點後、07-11 整天）。
    # 不用「總量偏低」判斷：梅雨、雷雨日總量可能只有平常 1/4，但那是真實狀況，應該留著。
    con.execute("""
        CREATE OR REPLACE TABLE day_stats AS
        WITH n AS (SELECT rent_time::DATE AS d, count(*) AS trips, count(DISTINCT hour(rent_time)) AS hours
                   FROM t GROUP BY 1)
        SELECT c.d, c.day_type, coalesce(n.trips, 0) AS trips, coalesce(n.hours, 0) AS hours_with_data
        FROM calendar c LEFT JOIN n USING (d)
        WHERE c.d BETWEEN (SELECT min(rent_time)::DATE FROM t) AND (SELECT max(rent_time)::DATE FROM t)
    """)
    con.execute("CREATE OR REPLACE TABLE valid_days AS SELECT d, day_type FROM day_stats WHERE hours_with_data = 24")

    # 分母：每站只算它實際有營運的期間（第一筆到最後一筆紀錄）內的有效日數，期間中開站／撤站的站才不會被低估
    con.execute("""
        CREATE OR REPLACE TABLE station_days AS
        WITH w AS (SELECT station, min(ts)::DATE AS first_d, max(ts)::DATE AS last_d FROM legs GROUP BY 1)
        SELECT w.station, v.day_type, count(*) AS n_days, any_value(w.first_d) AS first_d, any_value(w.last_d) AS last_d
        FROM w JOIN valid_days v ON v.d BETWEEN w.first_d AND w.last_d
        GROUP BY 1, 2
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE top_flows AS
        WITH agg AS (
          SELECT station, sno, v.day_type, direction, other_station, other_sno, count(*) AS trips
          FROM legs JOIN valid_days v ON legs.ts::DATE = v.d
          WHERE station <> other_station  -- 同站借還不列入排名，也不進 share_pct 分母
          GROUP BY ALL
        ),
        ranked AS (
          SELECT *, row_number() OVER w AS rank,
                 100.0 * trips / sum(trips) OVER (PARTITION BY station, day_type, direction) AS share_pct
          FROM agg WINDOW w AS (PARTITION BY station, day_type, direction ORDER BY trips DESC, other_station)
        )
        SELECT r.station, r.sno, r.day_type, r.direction, r.rank, r.other_station, r.other_sno, r.trips, sd.n_days,
               round(r.trips / sd.n_days, 2) AS trips_per_day, round(r.share_pct, 1) AS share_pct
        FROM ranked r JOIN station_days sd USING (station, day_type)
        WHERE r.rank <= {TOP_N}
        ORDER BY r.station, r.day_type, r.direction, r.rank
    """)

    con.execute("""
        CREATE OR REPLACE TABLE net_flow_hourly AS
        WITH agg AS (
          SELECT station, sno, v.day_type, hour(ts) AS hour,
                 count(*) FILTER (direction = '去向') AS rents,
                 count(*) FILTER (direction = '來源') AS returns
          FROM legs JOIN valid_days v ON legs.ts::DATE = v.d
          GROUP BY ALL
        )
        SELECT station, sno, day_type, hour, n_days,
               round(rents / n_days, 2) AS rents_per_day,
               round(returns / n_days, 2) AS returns_per_day,
               round((returns - rents) / n_days, 2) AS net_per_day
        FROM agg JOIN station_days USING (station, day_type)
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
    period = con.execute("SELECT min(d), max(d) FROM day_stats").fetchone()
    days = dict(con.execute("SELECT day_type, count(*) FROM valid_days GROUP BY 1 ORDER BY 1").fetchall())
    excluded = con.execute(
        "SELECT d::VARCHAR, day_type, trips, hours_with_data FROM day_stats ANTI JOIN valid_days USING (d) ORDER BY d").fetchall()
    trips_valid = con.execute("SELECT count(*) FROM t WHERE rent_time::DATE IN (SELECT d FROM valid_days)").fetchone()[0]
    partial = con.execute("""
        SELECT count(DISTINCT station) FROM station_days
        WHERE first_d > (SELECT min(d) FROM valid_days) OR last_d < (SELECT max(d) FROM valid_days)
    """).fetchone()[0]
    methods = dict(con.execute(
        "SELECT method, sum(rent_trips + return_trips) FROM station_match GROUP BY 1 ORDER BY 1").fetchall())
    return {
        "period": f"{period[0]} ~ {period[1]}", "valid_days": days,
        "excluded_days(date, day_type, trips, hours_with_data)": excluded,
        "trips_total": total, "trips_used": kept,
        "trips_per_valid_day": round(trips_valid / sum(days.values())),
        "rent_unmatched_pct": round(100 * rent_nomatch / kept, 2),
        "return_unmatched_pct": round(100 * ret_nomatch / kept, 2),
        "match_methods_trip_ends": methods,
        "stations_with_top_flows": con.execute("SELECT count(DISTINCT station) FROM top_flows").fetchone()[0],
        "stations_with_partial_window": partial,
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
