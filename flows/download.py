"""從 data.taipei 的索引檔下載指定月份的租借紀錄 zip。

    python -m flows.download 2026-05 2026-06 2026-07
    python -m flows.download --latest 3
"""
from __future__ import annotations

import argparse
import csv
import io
import ssl
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX_URL = ("https://data.taipei/api/dataset/c5924c17-25db-4f1e-99c4-f8ada40f2445"
             "/resource/1105750a-e8f5-4b89-9f7f-006493bb6de7/download")
DEST = ROOT / "raw" / "rentals"


def _data_taipei_context() -> ssl.SSLContext:
    # data.taipei 憑證缺 Subject Key Identifier，Python 3.13+ 預設的 X509 嚴格模式會拒絕。
    # 只關嚴格模式，憑證鏈與主機名稱照常驗證。
    ctx = ssl.create_default_context()
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


def load_index() -> dict[str, str]:
    """回傳 {'2026-07': zip 網址}。索引檔舊列是 Big5、新列是 UTF-8，逐列解碼。"""
    raw = urllib.request.urlopen(INDEX_URL, timeout=60, context=_data_taipei_context()).read()
    out = {}
    for line in raw.splitlines()[1:]:
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError:
            text = line.decode("cp950", errors="replace")
        row = next(csv.reader(io.StringIO(text)), None)
        if not row or len(row) < 6:
            continue
        y, m, _ = row[4].split("/")
        out[f"{int(y):04d}-{int(m):02d}"] = row[5]
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("months", nargs="*")
    p.add_argument("--latest", type=int)
    a = p.parse_args(argv)
    index = load_index()
    months = sorted(index)[-a.latest:] if a.latest else a.months
    DEST.mkdir(parents=True, exist_ok=True)
    for m in months:
        if m not in index:
            print(f"{m} 不在索引檔（最新是 {max(index)}）", file=sys.stderr)
            return 1
        dest = DEST / f"{m.replace('-', '')}.zip"
        if dest.exists():
            print(f"{m} 已存在，略過")
            continue
        url = urllib.parse.quote(index[m], safe=":/")
        print(f"下載 {m} …")
        tmp = dest.with_suffix(".part")
        urllib.request.urlretrieve(url, tmp)
        tmp.replace(dest)
        print(f"  {dest.stat().st_size / 1e6:.0f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
