import zipfile
from datetime import datetime

import duckdb
import pytest

from flows import build_trips
from flows.build_trips import COLS, detect_encoding, map_header
from flows.stations import Station, resolve

REF = [
    Station("1", "糖廍文化園區", "臺北市", 25.0, 121.5),
    Station("2", "公舘承德路口", "臺北市", 25.1, 121.5),
    Station("3", "公館站", "臺北市", 25.0, 121.5),
    Station("4", "捷運淡水站", "新北市", 25.2, 121.4),
    Station("5", "新名稱站", "臺北市", 25.0, 121.6),
    Station("6", "金龍公園", "臺北市", 25.0, 121.6),
    Station("7", "金龍公園", "新北市", 25.0, 121.6),
]


def test_resolve_methods():
    r = resolve(
        ["糖廍文化園區", "糖?文化園區", "公?承德路口", "捷運淡水站", "舊名稱站", "蘆洲維修中心", "瑞光維護所", "不存在", "金龍公園"],
        REF, {"舊名稱站": "新名稱站"},
    )
    assert {k: (s.sno if s else None, m) for k, (s, m) in r.items()} == {
        "糖廍文化園區": ("1", "exact"),
        "糖?文化園區": ("1", "wildcard"),
        "公?承德路口": ("2", "wildcard"),
        "捷運淡水站": ("4", "exact"),
        "舊名稱站": ("5", "alias"),
        "蘆洲維修中心": (None, "non_station"),
        "瑞光維護所": (None, "non_station"),
        "不存在": (None, "unmatched"),
        "金龍公園": ("6", "exact_ambiguous"),
    }


def test_resolve_wildcard_must_be_unique():
    ref = REF + [Station("9", "公館承德路口", "臺北市", 25.1, 121.5)]
    s, m = resolve(["公?承德路口"], ref, {})["公?承德路口"]
    assert s is None and m == "unmatched"


def test_map_header_variants():
    assert map_header("rent_time,rent_station,return_time,return_station,rent,bike_type,infodate") == (COLS, True)
    assert map_header("﻿借車時間,借車場站,還車時間,還車場站,借用時長,車種,借用日期") == (COLS, True)
    # 欄位順序不同也要依名稱對應
    cols, header = map_header("借車場站,借車時間,還車時間,還車場站,借用時長,車種,借用日期")
    assert header and cols[:2] == ["rent_station", "rent_time"]
    assert map_header("2026-05-01 10:00:00,A,2026-05-01 10:00:00,B,00:10:00,一般車,2026-05-01") == (COLS, False)


def test_detect_encoding():
    assert detect_encoding("借車時間,站\n".encode("utf-8")) == "utf-8"
    assert detect_encoding("借車時間,站\n".encode("cp950")) == "cp950"
    # 取樣切在多位元組字元中間不應誤判
    assert detect_encoding("借車時間\n借".encode("utf-8")[:-1]) == "utf-8"


@pytest.mark.parametrize("encoding, header", [
    ("utf-8", "rent_time,rent_station,return_time,return_station,rent,bike_type,infodate"),
    ("cp950", "借車時間,借車場站,還車時間,還車場站,借用時長,車種,借用日期"),
    ("utf-8", None),
])
def test_build_month_tolerates_formats(tmp_path, monkeypatch, encoding, header):
    monkeypatch.setattr(build_trips, "ROOT", tmp_path)
    monkeypatch.setattr(build_trips, "OUT_DIR", tmp_path / "trips")
    (tmp_path / "raw").mkdir()
    lines = ([header] if header else []) + [
        "2026-05-01 10:00:00,捷運公館站,2026-05-01 11:00:00,公館站,01:02:03,一般車,2026-05-01",
        "2026/05/02 08:00,公館站,2026/05/02 08:00,捷運公館站,00:09:59,電輔車,2026/05/02",
    ]
    zp = tmp_path / "202605.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("x.csv", ("\n".join(lines) + "\n").encode(encoding))
    r = build_trips.build_month(duckdb.connect(), zp)
    assert r["rows"] == 2 and r["bad_time"] == 0 and r["encoding"] == encoding
    rows = duckdb.sql(f"SELECT rent_time, rent_station, duration_sec, info_date::VARCHAR FROM '{tmp_path}/trips/202605.parquet' ORDER BY rent_time").fetchall()
    assert rows == [
        (datetime(2026, 5, 1, 10), "捷運公館站", 3723, "2026-05-01"),
        (datetime(2026, 5, 2, 8), "公館站", 599, "2026-05-02"),
    ]


def test_analyze_excludes_gap_days_and_uses_station_window(tmp_path, monkeypatch):
    from flows import analyze

    monkeypatch.setattr(analyze, "load_reference", lambda: [
        Station("A", "甲站", "臺北市", 25.0, 121.5),
        Station("B", "乙站", "臺北市", 25.0, 121.5),
        Station("N", "新北站", "新北市", 25.0, 121.4),
    ])
    monkeypatch.setattr(analyze, "load_aliases", lambda: {})
    cal = tmp_path / "calendar_test.csv"
    cal.write_text("﻿西元日期,星期,是否放假,備註\n" + "".join(
        f"2026050{d},{w},{h},\n" for d, w, h in
        [(4, "一", 0), (5, "二", 0), (6, "三", 0), (7, "四", 0), (8, "五", 0), (9, "六", 2)]), encoding="utf-8")

    rows = []
    for day in range(4, 10):
        for hr in range(24):
            if day == 6 and hr >= 8:  # 5/6 早上 8 點後沒資料 → 整天排除
                continue
            rows.append((f"2026-05-0{day} {hr:02d}:00:00", "乙站", "乙站"))
    rows += [("2026-05-07 10:00:00", "甲站", "乙站")] * 3   # 甲站 5/7 才開始營運
    rows += [("2026-05-08 09:00:00", "甲站", "新北站")]
    con = duckdb.connect()
    con.execute("CREATE TABLE raw (ts VARCHAR, a VARCHAR, b VARCHAR)")
    con.executemany("INSERT INTO raw VALUES (?, ?, ?)", rows)
    trips = tmp_path / "trips.parquet"
    con.execute(f"""COPY (SELECT ts::TIMESTAMP AS rent_time, a AS rent_station, ts::TIMESTAMP AS return_time,
                    b AS return_station, 600 AS duration_sec, '一般車' AS bike_type, ts::DATE AS info_date FROM raw)
                    TO '{trips}' (FORMAT parquet)""")

    analyze.load_calendar(con, [cal])
    summary = analyze.build(con, trips.as_posix())

    assert summary["valid_days"] == {"假日": 1, "平日": 4}
    assert [d[0] for d in summary["excluded_days(date, day_type, trips, hours_with_data)"]] == ["2026-05-06"]

    top = con.execute("""SELECT station, direction, rank, other_station, trips, n_days, trips_per_day, share_pct
                         FROM top_flows WHERE day_type = '平日' ORDER BY station, direction, rank""").fetchall()
    assert top == [
        ("乙站", "來源", 1, "甲站", 3, 4, 0.75, 100.0),   # 乙站→乙站的同站借還不列入排名
        ("甲站", "去向", 1, "乙站", 3, 2, 1.5, 75.0),     # 分母只算甲站營運的 2 個平日
        ("甲站", "去向", 2, "新北站", 1, 2, 0.5, 25.0),   # 新北站本身不分析（只分析臺北市站）
    ]

    net = con.execute("""SELECT rents_per_day, returns_per_day, net_per_day FROM net_flow_hourly
                         WHERE station = '乙站' AND day_type = '平日' AND hour = 10""").fetchone()
    assert net == (1.0, 1.75, 0.75)  # 同站借還仍計入淨流量；5/6 的紀錄不算
