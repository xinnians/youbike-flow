import pytest

from tdx import probe


def test_load_credentials_from_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("TDX_CLIENT_ID", raising=False)
    monkeypatch.delenv("TDX_CLIENT_SECRET", raising=False)
    env = tmp_path / ".env"
    env.write_text("# comment\nTDX_CLIENT_ID=abc-123\nTDX_CLIENT_SECRET='s3cret'\n")
    assert probe.load_credentials(env) == ("abc-123", "s3cret")


def test_load_credentials_missing_exits_without_leaking(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("TDX_CLIENT_ID", raising=False)
    monkeypatch.delenv("TDX_CLIENT_SECRET", raising=False)
    env = tmp_path / ".env"
    env.write_text("TDX_CLIENT_ID=abc-123\n")
    with pytest.raises(SystemExit) as e:
        probe.load_credentials(env)
    assert "abc-123" not in str(e.value)


def test_ledger_blocks_when_budget_exceeded(tmp_path):
    led = probe.Ledger(tmp_path / "u.json")
    led.check("2026-09", 0.2)
    # 24 次呼叫 + 0MB = 2.4 點
    assert led.record("2026-09", 24, 0, 0) == pytest.approx(2.4)
    with pytest.raises(SystemExit, match="拒絕執行"):
        led.check("2026-09", 0.15)
    led.check("2026-10", 0.15)  # 新的月份重新計算
    # 紀錄會寫回檔案
    assert probe.Ledger(tmp_path / "u.json").used("2026-09") == pytest.approx(2.4)


def test_ledger_counts_uncompressed_bytes(tmp_path):
    led = probe.Ledger(tmp_path / "u.json")
    # 1 次呼叫 + 解壓後 10MB（傳輸 1MB）→ 保守以解壓後計：0.1 + 0.5
    assert led.record("2026-09", 1, 1_000_000, 10_000_000) == pytest.approx(0.6)


def _csv(rows):
    head = "StationUID,StationID,ServiceStatus,ServiceType,AvailableRentBikes,AvailableReturnBikes,SrcUpdateTime,UpdateTime,GeneralBikes,ElectricBikes\n"
    return (head + "".join(
        f"TPE{s},{s},1,2,3,4,{t},{t},3,0\n" for s, t in rows)).encode()


def test_analyze_sample_by_station_order():
    rows = [(s, f"2026-09-28T00:{m:02d}:00+08:00") for s in ("500101001", "500101002") for m in range(0, 30, 5)]
    rows += [("500101003", "2026-09-28T00:00:00+08:00")]  # 被 $top 截斷的最後一站
    r = probe.analyze_sample(_csv(rows))
    assert r["rows"] == 13 and r["stations"] == 3
    assert r["median_interval_min"] == 5 and r["order"] == "依站點"
    assert r["rows_per_station"] == 6


def test_analyze_sample_by_time_order_and_extrapolate():
    rows = [(s, f"2026-09-28T00:{m:02d}:00+08:00") for m in (0, 1, 2) for s in ("500101002", "500101001", "500101003")]
    body = _csv(rows)
    r = probe.analyze_sample(body)
    assert r["median_interval_min"] == 1 and r["order"] == "依時間"
    ex = probe.extrapolate(r, raw_bytes=len(body) // 10, body_bytes=len(body))
    assert r["rows_per_station"] == 3
    assert ex["rows_per_day"] == probe.TAIPEI_STATIONS * 3
    assert ex["points_per_day_if_uncompressed_counted"] == pytest.approx(
        ex["mb_per_day_uncompressed"] / 20, abs=0.01)
