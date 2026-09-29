"""TDX 歷史車位 API 小探測：只呼叫 1 次、限制回傳筆數，量出資料間隔與壓縮前後大小。

    python -m tdx.probe [--date YYYY-MM-DD] [--top 5000]

金鑰：從環境變數或專案根目錄的 .env 讀取 TDX_CLIENT_ID、TDX_CLIENT_SECRET，程式不會印出。
計費（官方，2024-04-01 起）：歷史服務每 10 次 1 點、每 20MB 1 點，兩者合併計算；基礎會員每月 3 點。
      點數用完有 5% 緩衝，再用完當月停用，不會自動扣款（交通部收費要點第六點）。
用量護欄：本機 .tdx_usage.json 記錄每月估計點數（以未壓縮大小估，偏保守），
      這次呼叫的估計值加上去會超過 BUDGET_POINTS 就拒絕執行。
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import ssl
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
LEDGER_PATH = ROOT / ".tdx_usage.json"
TPE = ZoneInfo("Asia/Taipei")

AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
HIST_URL = "https://tdx.transportdata.tw/api/historical/v2/Historical/Bike/Availability/Taipei"

MONTHLY_POINTS = 3.0
BUDGET_POINTS = 2.5          # 自訂上限，留 0.5 點餘裕
POINTS_PER_CALL = 0.1        # 10 次 / 1 點
MB_PER_POINT = 20.0          # 20MB / 1 點
EST_BYTES_PER_ROW = 120      # 呼叫前估算用（CSV 一列約 90–120 bytes）
TAIPEI_STATIONS = 1808       # 2026-09-29 即時 API 站數
MAX_RAW_BYTES = 3_000_000    # 萬一 $top 沒生效回傳整天資料，讀到這個量就中斷


def load_credentials(env_path: Path = ENV_PATH) -> tuple[str, str]:
    values = {k: os.environ.get(k) for k in ("TDX_CLIENT_ID", "TDX_CLIENT_SECRET")}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                values[k.strip()] = values.get(k.strip()) or v.strip().strip("'\"")
    cid, secret = values.get("TDX_CLIENT_ID"), values.get("TDX_CLIENT_SECRET")
    if not cid or not secret:
        raise SystemExit(f"缺 TDX_CLIENT_ID / TDX_CLIENT_SECRET：請寫進 {env_path.name}（格式見 .env.example）")
    return cid, secret


class Ledger:
    """每月估計點數的本機紀錄。只是估計，實際扣點以 TDX 會員中心的使用統計為準。"""

    def __init__(self, path: Path = LEDGER_PATH):
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else {}

    def used(self, month: str) -> float:
        return self.data.get(month, {}).get("points", 0.0)

    def check(self, month: str, estimate: float) -> None:
        if self.used(month) + estimate > BUDGET_POINTS:
            raise SystemExit(
                f"拒絕執行：{month} 已估計用了 {self.used(month):.2f} 點，這次估計 {estimate:.2f} 點，"
                f"會超過自訂上限 {BUDGET_POINTS} 點（每月免費 {MONTHLY_POINTS} 點）")

    def record(self, month: str, calls: int, raw_bytes: int, body_bytes: int) -> float:
        points = calls * POINTS_PER_CALL + body_bytes / 1e6 / MB_PER_POINT
        m = self.data.setdefault(month, {"calls": 0, "raw_bytes": 0, "body_bytes": 0, "points": 0.0})
        m["calls"] += calls
        m["raw_bytes"] += raw_bytes
        m["body_bytes"] += body_bytes
        m["points"] = round(m["points"] + points, 4)
        self.path.write_text(json.dumps(self.data, indent=2))
        return points


class ResponseTooLarge(Exception):
    """回應超過 MAX_RAW_BYTES，已中斷讀取。"""


def _ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT  # 部分政府站台憑證缺 SKI
    return ctx


def get_token(cid: str, secret: str) -> str:
    body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "client_id": cid, "client_secret": secret}).encode()
    req = urllib.request.Request(AUTH_URL, data=body,
                                 headers={"content-type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30, context=_ctx()) as r:
        return json.load(r)["access_token"]


def fetch(token: str, date: str, top: int) -> tuple[int, bytes, dict]:
    """回傳 (傳輸的原始位元組數, 解壓後內容, 回應標頭)。"""
    q = urllib.parse.urlencode({"Dates": date, "$top": top, "$format": "CSV"}, safe="$")
    req = urllib.request.Request(f"{HIST_URL}?{q}", headers={
        "authorization": f"Bearer {token}", "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=120, context=_ctx()) as r:
        headers = dict(r.headers)
        raw = r.read(MAX_RAW_BYTES + 1)
    if len(raw) > MAX_RAW_BYTES:
        raise ResponseTooLarge(len(raw))
    body = gzip.decompress(raw) if headers.get("Content-Encoding", "").lower() == "gzip" else raw
    return len(raw), body, headers


def analyze_sample(body: bytes) -> dict:
    """從抽樣的 CSV 推出：筆數、站數、每站資料間隔、排序方式、每列大小。"""
    text = body.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return {"rows": 0}
    time_col = "SrcUpdateTime" if "SrcUpdateTime" in rows[0] else "UpdateTime"
    per_station: dict[str, list[datetime]] = {}
    for r in rows:
        per_station.setdefault(r["StationUID"], []).append(datetime.fromisoformat(r[time_col]))
    deltas = []
    for ts in per_station.values():
        ts.sort()
        deltas += [(b - a).total_seconds() / 60 for a, b in zip(ts, ts[1:]) if b > a]
    order = "依站點" if [r["StationUID"] for r in rows] == sorted(r["StationUID"] for r in rows) else \
            "依時間" if [r[time_col] for r in rows] == sorted(r[time_col] for r in rows) else "其他"
    return {
        "rows": len(rows),
        "columns": list(rows[0].keys()),
        "stations": len(per_station),
        "median_interval_min": statistics.median(deltas) if deltas else None,
        "order": order,
        "bytes_per_row": len(body) / len(rows),
        "time_range": (min(r[time_col] for r in rows), max(r[time_col] for r in rows)),
    }


def extrapolate(sample: dict, raw_bytes: int, body_bytes: int) -> dict | None:
    """推估查全臺北一天的資料量與點數（一次呼叫最多 7 天）。"""
    interval = sample.get("median_interval_min")
    if not interval:
        return None
    rows_per_day = TAIPEI_STATIONS * (24 * 60 / interval)
    mb_body = rows_per_day * body_bytes / sample["rows"] / 1e6
    mb_raw = rows_per_day * raw_bytes / sample["rows"] / 1e6
    return {
        "rows_per_day": round(rows_per_day),
        "mb_per_day_uncompressed": round(mb_body, 1),
        "mb_per_day_compressed": round(mb_raw, 1),
        "points_per_day_if_uncompressed_counted": round(mb_body / MB_PER_POINT, 2),
        "points_per_day_if_compressed_counted": round(mb_raw / MB_PER_POINT, 2),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=(datetime.now(TPE) - timedelta(days=1)).strftime("%Y-%m-%d"))
    p.add_argument("--top", type=int, default=5000)
    a = p.parse_args(argv)

    month = datetime.now(TPE).strftime("%Y-%m")
    ledger = Ledger()
    estimate = POINTS_PER_CALL + a.top * EST_BYTES_PER_ROW / 1e6 / MB_PER_POINT
    ledger.check(month, estimate)
    cid, secret = load_credentials()

    token = get_token(cid, secret)
    try:
        raw_bytes, body, headers = fetch(token, a.date, a.top)
    except urllib.error.HTTPError as e:
        ledger.record(month, 1, 0, 0)  # 呼叫已送出，保守起見照算次數
        print(f"TDX 回應 HTTP {e.code}：{e.read()[:300].decode('utf-8', 'replace')}", file=sys.stderr)
        return 1
    except ResponseTooLarge as e:
        ledger.record(month, 1, e.args[0], e.args[0] * 10)  # 不知道解壓後多大，保守估 10 倍
        print(f"回應超過 {MAX_RAW_BYTES:,} bytes，已中斷（$top 可能沒生效）。請到 TDX 會員中心確認實際扣點", file=sys.stderr)
        return 1
    points = ledger.record(month, 1, raw_bytes, len(body))

    out = ROOT / "raw" / f"tdx_probe_{a.date}.csv"
    out.parent.mkdir(exist_ok=True)
    out.write_bytes(body)

    sample = analyze_sample(body)
    print(f"查詢日期 {a.date}，$top={a.top}")
    print(f"傳輸 {raw_bytes:,} bytes（Content-Encoding: {headers.get('Content-Encoding', '無')}），解壓後 {len(body):,} bytes")
    print(f"抽樣：{json.dumps(sample, ensure_ascii=False, default=str)}")
    print(f"推估全臺北一天：{json.dumps(extrapolate(sample, raw_bytes, len(body)), ensure_ascii=False)}")
    print(f"本次估計 {points:.3f} 點；{month} 累計估計 {ledger.used(month):.3f} 點（上限 {BUDGET_POINTS}）")
    print(f"抽樣存到 {out.relative_to(ROOT)}。實際扣點請到 TDX【會員中心 > 資料服務 > 使用統計】核對")
    return 0


if __name__ == "__main__":
    sys.exit(main())
