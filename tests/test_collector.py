import gzip
import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pytest

from collector.collect import collect
from collector.compact import compact
from collector.snapshot import FeedError, parse_feed

TPE = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parents[1]


def station(i: int, **over) -> dict:
    e = {
        "sno": f"5001{i:05d}", "sna": f"YouBike2.0_測試站{i}", "sarea": "大安區",
        "mday": "2026-09-29 11:29:04", "ar": "地址", "sareaen": "Daan Dist.",
        "snaen": f"YouBike2.0_Test{i}", "aren": "addr", "act": "1",
        "srcUpdateTime": "2026-09-29 11:29:52", "updateTime": "2026-09-29 11:29:52",
        "infoTime": "2026-09-29 11:29:04", "infoDate": "2026-09-29", "Quantity": 20,
        "available_rent_bikes": i % 5, "latitude": 25.0, "longitude": 121.5,
        "available_return_bikes": 10,
    }
    e.update(over)
    return e


def feed_bytes(n: int = 1200, **over) -> bytes:
    return json.dumps([station(i, **over) for i in range(n)], ensure_ascii=False).encode()


def test_parse_ok():
    f = parse_feed(feed_bytes())
    assert len(f.stations) == 1200 and f.skipped == 0


def test_parse_accepts_old_total_field():
    data = [station(i) for i in range(1200)]
    for e in data:
        e["total"] = e.pop("Quantity")
    f = parse_feed(json.dumps(data).encode())
    assert f.stations[0]["total"] == 20


@pytest.mark.parametrize("raw, msg", [
    (b"<html>", "JSON"),
    (b'{"a": 1}', "list"),
    (feed_bytes(n=500), "下限"),
])
def test_parse_rejects(raw, msg):
    with pytest.raises(FeedError, match=msg):
        parse_feed(raw)


def test_parse_rejects_too_many_broken():
    data = [station(i) for i in range(1200)]
    for e in data[:100]:  # 8% 缺欄位
        del e["available_rent_bikes"]
    with pytest.raises(FeedError, match="格式"):
        parse_feed(json.dumps(data).encode())


def test_collect_writes_snapshot_and_daily_stations_once(tmp_path):
    t1 = datetime(2026, 9, 29, 8, 2, 0, tzinfo=TPE)
    t2 = datetime(2026, 9, 29, 8, 7, 0, tzinfo=TPE)
    w1 = collect(tmp_path, feed_bytes(), t1)
    w2 = collect(tmp_path, feed_bytes(), t2)
    assert [p.name for p in w1] == ["080200.csv.gz", "stations.csv.gz"]
    assert [p.name for p in w2] == ["080700.csv.gz"]
    with gzip.open(w1[0], "rt") as f:
        header = f.readline().strip()
    assert header == "fetched_at,feed_update_time,sno,rent_bikes,return_slots,quantity,act,info_time"


def test_compact_merges_past_days_only(tmp_path):
    collect(tmp_path, feed_bytes(), datetime(2026, 9, 28, 23, 57, tzinfo=TPE))
    collect(tmp_path, feed_bytes(), datetime(2026, 9, 28, 23, 52, tzinfo=TPE))
    collect(tmp_path, feed_bytes(), datetime(2026, 9, 29, 0, 2, tzinfo=TPE))
    out = compact(tmp_path, today="2026-09-29")

    assert not (tmp_path / "raw" / "2026-09-28").exists()
    assert (tmp_path / "raw" / "2026-09-29").exists()
    snap = tmp_path / "snapshots" / "date=2026-09-28" / "part-235200.parquet"
    assert snap in out
    n, first = duckdb.sql(f"SELECT count(*), min(fetched_at) FROM '{snap}'").fetchone()
    assert n == 2 * 1200
    assert first == datetime(2026, 9, 28, 23, 52)
    types = {r[0]: r[1] for r in duckdb.sql(f"DESCRIBE SELECT * FROM '{snap}'").fetchall()}
    assert types["rent_bikes"] == "INTEGER" and types["info_time"] == "TIMESTAMP"
    st = tmp_path / "stations" / "date=2026-09-28" / "part-235200.parquet"
    assert duckdb.sql(f"SELECT count(*) FROM '{st}'").fetchone()[0] == 1200


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


@pytest.mark.skipif(shutil.which("git") is None, reason="需要 git")
def test_push_script_end_to_end(tmp_path):
    """本機 bare repo 模擬 GitHub：第一次建 data 分支、同日再推、跨日合併。"""
    remote = tmp_path / "remote.git"
    _git(tmp_path, "init", "-q", "--bare", str(remote))
    feed = tmp_path / "feed.json"
    feed.write_bytes(feed_bytes())
    script = ROOT / "collector" / "push_to_data_branch.sh"

    def run(now, today, n):
        work = tmp_path / f"w{n}"
        work.mkdir()
        env = {
            "PATH": subprocess.os.environ["PATH"], "HOME": str(tmp_path),
            "DATA_REMOTE": remote.as_uri(), "WORK_DIR": str(work),
            "FEED_FILE": str(feed), "NOW": now, "TODAY": today,
            "PYTHON": subprocess.os.sys.executable,
        }
        r = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
        assert r.returncode == 0, r.stdout + r.stderr
        return r.stdout

    run("2026-09-28 23:52:00", "2026-09-28", 1)
    run("2026-09-28 23:57:00", "2026-09-28", 2)
    run("2026-09-29 00:02:00", "2026-09-29", 3)

    files = set(_git(remote, "ls-tree", "-r", "--name-only", "data").split())
    assert files == {
        "raw/2026-09-29/000200.csv.gz",
        "raw/2026-09-29/stations.csv.gz",
        "snapshots/date=2026-09-28/part-235200.parquet",
        "stations/date=2026-09-28/part-235200.parquet",
    }
    assert len(_git(remote, "log", "--oneline", "data").splitlines()) == 3


def test_coverage_counts_slots_and_longest_gap(tmp_path):
    from collector.coverage import coverage, fetch_times

    # 第一天 00:02 起每 5 分鐘一筆，但 10:00–11:59 整段缺；隔天只有一筆還沒合併的小檔
    day = datetime(2026, 9, 28)
    for m in range(2, 24 * 60, 5):
        t = day.replace(hour=m // 60, minute=m % 60)
        if not (10 <= t.hour < 12):
            collect(tmp_path, feed_bytes(), t.replace(tzinfo=TPE))
    compact(tmp_path, today="2026-09-29")
    collect(tmp_path, feed_bytes(), datetime(2026, 9, 29, 0, 7, tzinfo=TPE))

    times = fetch_times(tmp_path)
    assert datetime(2026, 9, 29, 0, 7) in times
    ratio, n, gap, gap_at = coverage(times, day, day.replace(day=29))
    assert n == 96
    assert ratio == pytest.approx(88 / 96)
    assert gap.total_seconds() == 2 * 3600 and gap_at == datetime(2026, 9, 28, 10, 0)
