#!/usr/bin/env bash
# ATR 리밸런싱 전략 운영 명령 (호스트에서 실행). 사용법: ./atr.sh <명령> [인자]  — 명령 목록은 ./atr.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
docker cp "$HERE/atr_api.py" fin-ai-app:/tmp/atr_api.py >/dev/null      # 컨테이너를 다시 만들어도 항상 최신 도우미를 쓴다
cmd="${1:-}"; [ $# -gt 0 ] && shift
if [ "$cmd" = "upload" ]; then
  f="${1:?목표 파일 경로가 필요합니다: ./atr.sh upload <파일>}"
  [ -f "$f" ] || { echo "파일이 없습니다: $f" >&2; exit 1; }
  docker cp "$f" fin-ai-app:/tmp/atr_upload_file >/dev/null
  set -- /tmp/atr_upload_file "$(basename "$f")"
fi
docker exec -w /app -e PYTHONPATH=/app fin-ai-app python /tmp/atr_api.py "$cmd" "$@"
if [ "$cmd" = "dryrun" ]; then
  out="$HERE/plan_$(date +%Y%m%d_%H%M).csv"
  docker cp fin-ai-app:/tmp/plan.csv "$out" >/dev/null && echo "plan.csv 저장: $out"
fi
