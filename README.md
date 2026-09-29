# youbike-flow

臺北市 YouBike 2.0 常用站的流向分析與無車預估（自用驗證版）。

要驗證的假設：「如果事先知道常用站何時可能沒車、車都流去哪，我會改變出門時間或選站。」

## 結構

```
collector/   每 5 分鐘抓即時車位 → data 分支（GitHub Actions）
  snapshot.py              JSON → 精簡快照列（只用標準函式庫）
  collect.py               抓一次、寫 raw/<日期>/<時分秒>.csv.gz
  compact.py               把今天以前的小檔合併成 Parquet
  push_to_data_branch.sh   Actions 用：sparse clone data 分支 → 抓 → 合併 → 推
  coverage.py              完成條件檢查：15 分鐘時段覆蓋率
flows/       租借紀錄流向分析（本機跑）
  download.py      下載租借紀錄 zip
  build_trips.py   zip → raw/trips/<年月>.parquet（容錯：編碼、欄位名、時間格式）
  stations.py      站點參照表（臺北市＋新北市即時 API）與站名比對
  analyze.py       前 5 名去向／來源、每小時淨流量（平日／假日）
predict/     常用站無車／無位機率（第 2 週）
  availability.py           data 分支快照 → 每站 × 平日/假日 × 15 分鐘的機率 CSV 與圖表頁
tdx/         TDX 歷史車位 API 小探測（有用量護欄）
  probe.py
data/        小型參考資料（進 git）
  calendar_115.csv       人事行政總處 115 年（2026）辦公日曆表
  station_aliases.csv    改名站點對照（附證據）
  my_stations.csv        常用站清單（機率圖只算這些站）
raw/, out/   下載檔與分析輸出（不進 git，可用指令重建）
```

## 收集器

### data 分支的檔案配置

```
raw/<日期>/<HHMMSS>.csv.gz              當天每次抓取一個檔（約 11KB）
raw/<日期>/stations.csv.gz              當天第一次抓取時存站點靜態資訊
snapshots/date=<日期>/part-*.parquet    隔天合併後的快照
stations/date=<日期>/part-*.parquet     隔天合併後的站點資訊
```

快照欄位：`fetched_at, feed_update_time, sno, rent_bikes, return_slots, quantity, act, info_time`。

- 判斷站點是否失聯要看 `info_time`（每站自己的時間）。`feed_update_time` 所有站都一樣，只能看出整份 API 有沒有更新
- `quantity` 不一定等於 `rent_bikes + return_slots`（2026-09-29 實測 1,808 站中有 631 站不等）

設計取捨：
- 資料放獨立的 `data` 分支：每 5 分鐘一個 commit，放 `main` 會讓本機的 main 永遠落後
- Actions 只 clone `raw/`（blobless + sparse），已合併的 Parquet 不下載，checkout 時間不隨資料量變長
- 每 5 分鐘一個小檔、隔天合併：直接反覆改寫同一個 Parquet 檔，git 歷史每次都會多存一份整檔
- 排程在每小時的 2、7、12…57 分，避開整點（官方文件：整點負載高，排隊的 job 可能被丟棄）。cron 用逗號列舉寫法、不用 `2-59/5`，以排除 GitHub 不支援「範圍加間隔」的可能
- git 歷史估計每月增加約 150MB `[推論]`（每次快照 11KB × 288 次/天，加上合併檔）。驗證期（4 週）沒問題；長期使用要定期把 data 分支壓成單一 commit

### 啟用步驟（要你操作）

1. 在 GitHub 建一個**公開** repo（私有 repo 的 Actions 額度每月 2,000 分鐘，每 5 分鐘跑一次會超過）
2. `git remote add origin <repo>` 後 push `main`
3. 到 repo 的 Actions 頁手動執行一次 `collect` workflow（`workflow_dispatch`），確認有建出 `data` 分支
4. 之後每 5 分鐘自動執行

### 完成條件

連續 3 天、≥ 95% 的 15 分鐘時段至少有 1 筆快照：

```bash
git clone --branch data --single-branch <repo> ../youbike-data
.venv/bin/python -m collector.coverage --data-dir ../youbike-data --start <第一個完整日> --days 3
```

## 流向分析

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m flows.download --latest 3     # 約 360MB
.venv/bin/python -m flows.build_trips             # → raw/trips/*.parquet、out/build_report.csv
.venv/bin/python -m flows.stations                # → raw/stations_ref.csv（抓當下的站點清單）
.venv/bin/python -m flows.analyze                 # → out/*.csv、out/summary.txt
```

輸出：
- `out/top_flows.csv`：每站 × 平日/假日 × 去向/來源的前 5 名（不含同站借還），含每日平均次數、占比、`n_days`（分母）
- `out/net_flow_hourly.csv`：每站 × 平日/假日 × 小時的平均借出、還入、淨流量（正值＝還入多於借出）
- `out/station_match.csv`：租借紀錄站名的比對結果與影響的借還次數；對不上的站名填進 `data/station_aliases.csv` 後重跑
- `out/summary.txt`：期間、筆數、比對覆蓋率

### 定義

- 平日／假日：依人事行政總處辦公日曆表，「是否放假=2」為假日（含 5/1 勞動節、6/19 端午節）
- 去向與借出量用**借車時間**決定日期與小時，來源與還入量用**還車時間**。兩張表不能逐筆對帳（跨日型的行程約 0.09%）
- **排除資料缺漏日**：當天有任何一個小時完全沒有借車紀錄，就整天排除。2026-05～07 排除了 7/10（8 點後沒資料）、7/11（整天沒資料）、7/12（7 點前沒資料），推測是颱風停止營運 `[不確定]`。下雨造成的低量日不排除
- **分母**＝該站營運期間（第一筆到最後一筆紀錄）內的有效日數。期間中才開站或撤站的站，分母只算它營運的日子
  - 已知缺點：零星幾筆紀錄會把營運期間拉長（例如樹德公園最後一筆借出在 7/9，但 7/25 有 2 筆還車，平日分母從 48 天變 58 天）；營運期間中間長期停用的日子也照算（考試院 5/9～6/24 沒有任何紀錄）。約十幾站受影響，主要是冷門或施工中的站。看自己常用站時，先確認 `n_days` 合不合理
- **同站借還不列入前 5 名**，也不進 `share_pct` 的分母；但仍計入每小時淨流量，因為借出到還回之間，車確實不在站上。同站借還約占全部行程 6.5%，其中不到 5 分鐘的只占 2.6%
- `share_pct` 的分母，包含另一端對不上站名的行程

### 資料限制（2026-05～07 實測）

- **借還時間只到小時**（分秒全為 0），流向只能做到每小時，不能做 15 分鐘
- **只含在臺北市借出的車**：借車站沒有任何一個是新北專屬站名，還車站則包含新北。從新北騎進臺北的車不在資料裡，靠近市界的站，「來源」與淨流量會偏低
- 罕用字（廍、舘、瑠…）在原始資料變成 `?`；這些字不在 Big5 字集，推測資料曾經過 Big5 轉換 `[推論]`。用萬用字元比對後，都能唯一對回正確站名
- 有效日日均約 24.5 萬筆，比原先估計的 8 萬高很多（來源：`out/summary.txt` 的 `trips_per_valid_day`）
- 臺北、新北有 5 個同名站（福壽公園、金龍公園、後港公園、中興公園、八德立體停車場），只看站名分不出還到哪一個，一律算到臺北市，`station_match.csv` 標為 `exact_ambiguous`。其中金龍公園兩站只相距 4.4 公里，還入量可能多算了約 2,000 趟 `[推論]`
- 改名站點已合併（`data/station_aliases.csv`，附證據）：`忠孝東路五段215巷口 → 富生公園`、`景勤二號公園 → 六張犁社會住宅1區`、`洲子二號公園 → 瑞光路583巷口`（以上三組新舊站號相同）、`六張犁社會住宅B基地 → 六張犁社會住宅2區`（依啟停時間與流向推論）

## 資料來源查證紀錄（2026-09-29）

| 來源 | 狀態 |
|---|---|
| 即時 API | 1,808 站；欄位名是 `Quantity`，不是 `total` |
| 租借紀錄（data.taipei） | 最新到 2026-07（索引檔 2026-09-07 更新）；data.taipei 憑證缺 SKI，Python 3.13+ 要關掉 `VERIFY_X509_STRICT` |
| 社群歷史資料（tses89214） | 2024-05-03～2025-06-22、每 10 分鐘一筆，**已停更**，補不了最近的資料 |
| TDX 歷史車位 | `/api/historical/v2/Historical/Bike/Availability/{City}?Dates=`，一次最多 7 天，最早 2021-06-01（依據第三方 R 套件 `ChiaJung-Yeh/NYCU_TDX` 的原始碼）；**粒度與最新可查日期未驗證，要先申請帳號** |
| 新北即時 API | `data.ntpc.gov.tw` 資料集 `010e5b15-…`，1,610 站，用來補還到新北的站點座標 |

## 常用站無車／無位機率

```bash
git clone --branch data --single-branch https://github.com/xinnians/youbike-flow.git ~/youbike-data   # 之後用 git -C ~/youbike-data pull 更新
.venv/bin/python -m predict.availability --data-dir ~/youbike-data
```

- 常用站在 `data/my_stations.csv`：目前是 `瑞光路316巷`（500108171）、`大港墘公園(洲子街)`（500108153）
- 輸出 `out/availability_15min.csv` 與 `out/availability.html`（每站平日／假日兩張圖，滑鼠移上去看數值與樣本數，樣本少於 3 天的時段淡色顯示）
- `p_no_bike`／`p_no_dock`：該時段所有快照中遇到 0 台／0 格的比例；`*_any`：該時段任一次快照為 0 的天數比例（較保守）
- 排除停用站（`act ≠ 1`）與資料時間落後超過 30 分鐘的快照
- **不回補歷史資料**（2026-09-29 決定），只用收集器從 2026-09-29 起的資料，樣本要累積到約 10/20 才夠
- 瑞光路316巷是上班目的地型的站：平日早上 8–9 點大量還入、傍晚 17–18 點大量借出，所以早上要看無位、傍晚要看無車

## TDX（2026-09-29 查證）

- **不會自動扣款**：基礎會員每月免費 3 點，點數用完後有 5% 緩衝，之後當月停用；要付費必須自己主動訂閱（[交通部收費要點](https://www.motc.gov.tw/ch/app/data/doc?id=14&module=news&detailNo=1107913081326931968&serno=44e2cc94-8a1b-4fc0-b70e-f07551a05e2a&type=s&preview=&aplistdn=)第五、六點）
- 歷史服務計費：每 10 次 1 點、每 20MB 1 點，兩者合併；基礎會員每把金鑰每分鐘最多 5 次（[訂閱收費](https://tdx.transportdata.tw/pricing)）
- 歷史車位 API（`/v2/Historical/Bike/Availability/{City}`）只有 `Dates`（一次最多 7 天）、`$top`、`$format`、`Meta` 參數，**不能篩選站點**，每次都回傳整個縣市的資料。資料從 2021-06 到昨天，每天早上 8 點更新
- 所以免費額度大概只夠查一天的全市資料 `[推論]`，不適合拿來回補

探測步驟（只呼叫 1 次，約扣 0.15 點）：

```bash
cp .env.example .env    # 填入 TDX_CLIENT_ID、TDX_CLIENT_SECRET；.env 不進 git
.venv/bin/python -m tdx.probe
```

`tdx/probe.py` 會把每月估計點數記在 `.tdx_usage.json`（不進 git），估計會超過 2.5 點就拒絕執行；如果 `$top` 沒生效、回應超過 3MB，會直接中斷。實際扣點以 TDX【會員中心 > 資料服務 > 使用統計】為準。

## 待辦

- [ ] 建立 `.env`，執行 `python -m tdx.probe`，確認資料時間間隔，並核對是按壓縮前還是壓縮後的大小扣點
- [ ] 確認 GitHub Actions 排程有正常觸發；3 天後執行 `collector.coverage`
- [ ] 約 10/20 樣本夠了之後，看 `out/availability.html`，進入第 3–4 週實際使用與記錄
