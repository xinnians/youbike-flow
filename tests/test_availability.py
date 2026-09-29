import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pytest

from collector.collect import collect
from collector.compact import compact
from flows.analyze import load_calendar
from predict import availability

TPE = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parents[1]
A, B = "500108171", "500108153"


def feed(now: datetime, state: dict[str, dict]) -> bytes:
    """state: {sno: {rent, ret, act?, info_time?}}；其餘補滿到 1,200 站的假站。"""
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"sno": f"5009{i:05d}", "sna": "x", "act": "1", "Quantity": 10, "available_rent_bikes": 5,
             "available_return_bikes": 5, "infoTime": ts, "updateTime": ts} for i in range(1200)]
    for sno, s in state.items():
        rows.append({"sno": sno, "sna": sno, "act": s.get("act", "1"), "Quantity": 20,
                     "available_rent_bikes": s["rent"], "available_return_bikes": s["ret"],
                     "infoTime": s.get("info_time", ts), "updateTime": ts})
    return json.dumps(rows).encode()


def snap(data_dir, y, mo, d, h, mi, state):
    now = datetime(y, mo, d, h, mi, tzinfo=TPE)
    collect(data_dir, feed(now, state), now)


@pytest.fixture
def data_dir(tmp_path):
    ok = {"rent": 5, "ret": 5}
    # 9/29（二）17:00 時段：3 筆快照中 1 筆無車；08:00 時段 1 筆無位
    snap(tmp_path, 2026, 9, 29, 17, 2, {A: ok, B: ok})
    snap(tmp_path, 2026, 9, 29, 17, 7, {A: {"rent": 0, "ret": 20}, B: ok})
    snap(tmp_path, 2026, 9, 29, 17, 12, {A: ok, B: ok})
    snap(tmp_path, 2026, 9, 29, 8, 2, {A: {"rent": 20, "ret": 0}, B: ok})
    # 9/30（三）17:00 時段：3 筆都無車；另有 1 筆失聯（info_time 落後 2 小時）、1 筆停用，都應排除
    snap(tmp_path, 2026, 9, 30, 17, 2, {A: {"rent": 0, "ret": 20}, B: ok})
    snap(tmp_path, 2026, 9, 30, 17, 7, {A: {"rent": 0, "ret": 20}, B: {"rent": 0, "ret": 20, "act": "0"}})
    snap(tmp_path, 2026, 9, 30, 17, 12, {A: {"rent": 0, "ret": 20},
                                          B: {"rent": 0, "ret": 20, "info_time": "2026-09-30 15:00:00"}})
    compact(tmp_path, today="2026-09-30")  # 9/29 合併成 Parquet，9/30 留在 raw/，兩種來源都要讀到
    # 9/28（一）教師節：週一但放假，應歸為假日
    snap(tmp_path, 2026, 9, 28, 17, 2, {A: {"rent": 0, "ret": 20}, B: ok})
    # 10/3（六）假日
    snap(tmp_path, 2026, 10, 3, 17, 2, {A: ok, B: ok})
    return tmp_path


def _load(data_dir):
    con = duckdb.connect()
    load_calendar(con, sorted((ROOT / "data").glob("calendar_*.csv")))
    n = availability.load_snapshots(con, data_dir)
    return con, n


def test_probabilities_cover_all_stations(data_dir):
    con, n = _load(data_dir)
    assert n == 9 * 1202                                        # 9 次抓取 × 全部站點
    names = availability.load_station_names(con, data_dir)
    assert names[A] == A and len(names) == 1202                 # 合併檔與當天小檔的站名都讀到
    rows = {(r["sno"], r["day_type"], r["slot"]): r for r in availability.compute(con, names)}
    assert {k[0] for k in rows} >= {A, B, "500900000"}          # 不只常用站

    r = rows[(A, "平日", "17:00")]
    assert (r["n_days"], r["n_snapshots"]) == (2, 6)
    assert r["p_no_bike"] == pytest.approx(4 / 6, abs=0.001)   # 快照比例
    assert r["p_no_bike_any"] == 1.0                            # 兩天都至少一次無車
    assert r["p_no_dock"] == 0

    r = rows[(A, "平日", "08:00")]
    assert (r["p_no_dock"], r["p_no_bike"], r["n_days"]) == (1.0, 0, 1)

    r = rows[(B, "平日", "17:00")]
    assert r["n_snapshots"] == 4 and r["p_no_bike"] == 0       # 停用與失聯的兩筆已排除

    r = rows[(A, "假日", "17:00")]                             # 10/3（六）有車、9/28 教師節無車
    assert (r["n_days"], r["p_no_bike"]) == (2, 0.5)
    s = availability.summary(con)
    assert s["excluded_stale_or_inactive"] == 2 and s["stations"] == 1202


def test_pack_and_render(data_dir):
    con, _ = _load(data_dir)
    names = {**availability.load_station_names(con, data_dir), A: "瑞光路316巷</script>"}
    rows = availability.compute(con, names)
    packed = availability.pack(rows, names)
    nb, nd, days, snaps = packed[A][1]["平日"]
    i17 = availability.SLOTS.index("17:00")
    assert (nb[i17], nd[i17], days[i17], snaps[i17]) == (667, 0, 2, 6)
    assert nb[availability.SLOTS.index("03:00")] is None      # 沒資料的時段是 null
    html = availability.render_html(rows, names, {A: "瑞光路316巷"}, availability.summary(con))
    assert "/*__DATA__*/" not in html
    assert "瑞光路316巷<\\/script>" in html and html.count("</script>") == 1


def test_backfill_is_included(data_dir):
    from tdx.backfill import convert
    head = "StationUID,StationID,ServiceStatus,ServiceType,AvailableRentBikes,AvailableReturnBikes,SrcUpdateTime,UpdateTime,GeneralBikes,ElectricBikes\n"
    body = (head + "".join(f"TPE{A},{A},1,2,0,20,2026-09-24T17:{m:02d}:00+08:00,x,0,0\n" for m in (1, 4, 7))).encode()
    convert(duckdb.connect(), body, data_dir / "backfill/tdx/date=2026-09-24/part-tdx.parquet")
    con, n = _load(data_dir)
    assert n == 9 * 1202 + 3
    rows = {(r["sno"], r["day_type"], r["slot"]): r for r in availability.compute(con, {})}
    r = rows[(A, "平日", "17:00")]
    assert (r["n_days"], r["n_snapshots"]) == (3, 9)          # 9/24 回補 3 筆全無車
    assert r["p_no_bike"] == pytest.approx(7 / 9, abs=0.001)
    assert availability.summary(con)["by_source"] == {"collector": 9 * 1202, "tdx": 3}
