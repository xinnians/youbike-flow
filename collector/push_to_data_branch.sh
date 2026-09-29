#!/usr/bin/env bash
# 抓一次快照並推到 data 分支。在 GitHub Actions 裡跑；本機測試可設 DATA_REMOTE 指到本機 bare repo。
#
# 為什麼資料放獨立的 data 分支：每 5 分鐘 commit 一次，放 main 會讓你本機的 main 永遠落後，
# 每次 push 前都得先 pull。
# 為什麼用 blobless + sparse clone：只下載 raw/（當天還沒合併的小檔），
# 已合併的 Parquet 不下載，checkout 時間不會隨資料量變長。
set -euo pipefail

CODE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}/databranch"
REMOTE="${DATA_REMOTE:-https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git}"
BRANCH="${DATA_BRANCH:-data}"
PY="${PYTHON:-python3}"

if git ls-remote --exit-code --heads "$REMOTE" "$BRANCH" >/dev/null 2>&1; then
  git clone -q --depth 1 --filter=blob:none --no-checkout --branch "$BRANCH" "$REMOTE" "$WORK" 2>&1 \
    | grep -v "filtering not recognized by server" || true
  cd "$WORK"
  git sparse-checkout set --no-cone '/raw/'
  git checkout -q "$BRANCH"
else
  echo "遠端沒有 $BRANCH 分支，建立新的孤立分支"
  git init -q -b "$BRANCH" "$WORK"
  cd "$WORK"
  git remote add origin "$REMOTE"
  git sparse-checkout set --no-cone '/raw/'
fi

git config user.name "youbike-collector"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

(cd "$CODE_DIR" && "$PY" -m collector.collect --data-dir "$WORK" ${FEED_FILE:+--feed-file "$FEED_FILE"} ${NOW:+--now "$NOW"})
(cd "$CODE_DIR" && "$PY" -m collector.compact --data-dir "$WORK" ${TODAY:+--today "$TODAY"})

git add --sparse -A .
if git diff --cached --quiet; then
  echo "沒有變更"
  exit 0
fi
git commit -q -m "snapshot $(TZ=Asia/Taipei date '+%F %T')"

for i in 1 2 3; do
  if git push -q origin "HEAD:$BRANCH"; then
    echo "已推送到 $BRANCH"
    exit 0
  fi
  echo "推送失敗，第 $i 次重試"
  git pull -q --rebase origin "$BRANCH" || true
  sleep $((i * 5))
done
exit 1
