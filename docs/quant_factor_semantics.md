# 퀀트 팩터 스코어 의미 정리 (factor_scores / valuation_metrics)

## composite_rank / composite_top_pct
- 같은 `strategy_version`, 같은 `as_of_date` 안에서 **WICS 섹터(wics_code) 내** 상대 순위다. 전체 시장 순위가 아니다.
- `composite_rank` 1 = 해당 섹터에서 composite_score 최고. 섹터마다 1위가 한 명씩 존재한다.
- `composite_top_pct = composite_rank / 섹터 내 종목수 * 100`. 값이 작을수록 섹터 내 상위.
- composite_v1 과 composite_v2 의미가 같다. (2026-07-31 v2 실DB 확인: 10개 섹터 모두 max(rank) = 섹터 종목수, top_pct 오차 0)
- 전체 시장 기준 순위가 필요하면 `composite_score` 로 직접 정렬한다.

## 날짜 체계와 valuation 짝짓기
- composite_v1: as_of_date = 월초(1일). composite_v2: as_of_date = 월말 마지막 거래일.
- valuation_metrics 는 월초(1일) 일자만 있다. /screen 은 `valuation_as_of_for()` 로 짝을 짓는다:
  월초는 같은 날짜, 그 외는 다음 달 1일 (예: 2026-07-31 -> 2026-08-01, 2025-12-31 -> 2026-01-01).
- 응답의 `valuation_as_of_date` 로 실제 사용된 valuation 일자를 확인할 수 있다.
- composite_score 는 성장 지표에 TTM 이력이 필요해 2025-03 이후부터 실질적으로 채워진다.
- valuation 의 가격 기준일: valuation_metrics 의 per/pbr 등은 그 행 `as_of_date`(월 1일) 종가 기준이다(1일이 휴장이면 직전 거래일 종가).
  composite_v2 점수는 월말 마지막 거래일 기준이므로 /screen 에서 붙는 valuation 은 점수일보다 최대 한 거래일 뒤 가격이다.
  따라서 `max_per` 등 valuation 필터에는 최대 한 거래일 정도의 시점 차이가 있다. 백테스트 점수 자체에는 영향이 없다(표시/필터에 한정).
  (B5 산출물의 per 는 직전 월말 종가 기준이라 DB 값과 일치하지 않는 행이 있다. 상세: api-test1-project `docs/valuation_b5_vs_db_reconciliation.md`)

## 기본 strategy_version 전환
- 코드 기본값: `composite_v1`.
- 환경변수 `QUANT_DEFAULT_STRATEGY_VERSION` 이 설정되면 그 값이 기본이 된다 (예: `composite_v2`).
- import 시점에 한 번 읽으므로 변경 후 프로세스 재시작이 필요하다.
- 영향 경로: `/{stock_code}/scores`, `/screen` 의 기본 쿼리값, `get_stock_snapshot` 의 factor score.
- 롤백: 환경변수 제거(또는 composite_v1 로 설정) 후 재시작.
