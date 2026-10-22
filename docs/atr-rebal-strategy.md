# 월초 ATR 리밸런싱 전략 (atr_rebal)

목표 포트폴리오 파일(종목코드·종목명·비중·ATR)에 맞춰 **매월 첫 거래일**에만, **정해 둔 투자금액 안에서** 시분할로
비중을 맞추고, 매 거래일 장중에 ATR 손절을 감시하는 자동매매. KIS 모의계좌(stock-coin-trade 게이트웨이)에서 먼저 운영한다.
참고 구현: 독립 프로그램 `auto_trading`(README 의 KIS 시행착오 포함).

## 진행 단계

| 단계 | 내용 | 상태 |
|---|---|---|
| 1 | st 게이트웨이 확인 (취소 필드·주문 유형·잔고·호출 제한) | 완료 |
| 2 | 기반: Wilder ATR, 거래일 달력, clientOrderId 태그, `live_orders.source`, trading 큐 | 완료 (`feat/atr-rebal-step2-foundation`) |
| 3 | 테이블·목표 로더·ATR 계산·계획 수립(dry-run)·auto_trade 차단 | 완료 (`feat/atr-rebal-step3-plan-dryrun`) |
| 4 | 시분할 주문 실행(5분 회차)·15:00 일괄 취소 | 예정 |
| 5 | 손절 감시·장부 동기화 | 예정 |
| 6 | reconciliation 반영·화면·장 마감 결과 | 예정 |
| 7 | `/api/quant/financials/screen` 상위 N개 연동 | 예정 |

## 확정된 규칙

- **계좌**: 기존 auto_trade 와 같은 KIS 모의계좌. 전략을 실주문 모드(`dry_run=false`)로 켜면 broker=kis·quant_mode=live 인
  auto_trade(시스템 배치 포함)를 모두 끄고, 켜져 있는 동안 다시 켜지 않는다(시작 API 409, Beat 사이클·KIS 배치에서 강제 해제).
- **장부**: 전략이 산 수량·평균가만 `strategy_positions` 로 관리. 시작 시 기존 보유분은 편입하지 않는다. 수동 매도가 생기면 장부를 계좌 수량에 맞춰 줄인다.
- **목표 파일**: 비중은 비율로 환산(없으면 동일비중). 종목 수 상한 기본 **20**(게이트웨이 잔고가 모의투자 20종목에서 잘림 — st 연속조회 전까지).
- **투자금액**: 실효 투자금액 = min(설정값, 목표 종목 장부 평가액 + 예수금). 줄어들면 경고 로그·실행 기록.
- **목표수량**: floor(실효 투자금액 × 비중 ÷ 현재가). 목표 파일에 없는 장부 종목은 건드리지 않는다.
- **시분할**: 5분 간격. 매도 09:10~14:30(65회), 매수 09:10~14:55(70회), 15:00 미체결 일괄 취소.
  종목별 회차 수 = min(남은 회차, 남은 주식 수), 회당 최소 1주. 회차 안의 종목들은 5분 안에 나눠 처리(호출 예산 분당 60회).
- **주문 유형**: 게이트웨이가 MARKET/LIMIT 만 받으므로 최유리지정가 대신 **LIMIT: 매수 현재가 + 1틱, 매도 현재가 − 1틱**(호가 단위 맞춤).
  미체결은 다음 회차에 취소 → 확인 → 남은 수량 재주문.
- **ATR**: 파일 값 우선, 비면 KIS 일봉(FHKST03010100, 수정주가)에서 **오늘 봉을 뺀** 완성 일봉으로 Wilder 14일. 리밸런싱 날 고정.
  ATR 이 없거나 계산 실패하면 % 손절(기본 −10%, 설정 가능), 매수 범위 제한 없음.
- **매수 범위**: 리밸런싱 날 시가 ± ATR×1. 범위 밖이면 그 회차 건너뜀.
- **손절**: 장부 평균가 − ATR×2 아래면 장부 수량 전량 시장가. 매 거래일 09:05~15:15. 같은 종목의 열린 주문을 먼저 취소.
  리밸런싱 날 손절되면 그 종목의 남은 회차 취소. 자동 익절 없음.
- **위험관리**: risk_guard 의 일손실 한도·일 주문 수·쿨다운은 이 전략의 **매수에만** 적용, 손절 매도는 막지 않는다(kill switch 만 예외).
  손절 주문이 열려 있거나 체결 확인 대기 중이면 새 손절을 내지 않는다. 전략 전용 일 주문 수 한도 `strategy_plans.max_orders_per_day`.

## API (3단계)

`/api/strategy/atr` — `GET|PUT /plan`, `POST /targets`(multipart `file`), `GET /targets`, `POST /dry-run`,
`GET /runs`, `GET /runs/{id}`, `GET /runs/{id}/plan.csv`, `POST /activate`(`{"dry_run": true}`), `POST /deactivate`.
`plan.csv` 의 앞 10개 컬럼은 참고 구현 `state/plan_YYYYMMDD.csv` 와 같다. 단 `보유수량`은 계좌가 아니라 **전략 장부** 수량,
`주문가능수량`은 min(장부, 계좌 보유) — 게이트웨이 잔고에 KIS 주문가능수량이 없어서다.

## 별도 작업 목록 (이번 범위 밖)

1. **[실전 전환 전] st 호가 엔드포인트** `/openapi/v1/kis/orderbook/{symbol}` (FHKST01010200) — LIMIT 대체를 매도1/매수1호가로 바꾸기. 또는 st 에 `BEST`(`ORD_DVSN=03`) 추가.
2. **st 잔고 연속조회**(`tr_cont`/`CTX_AREA_*`)와 주문가능수량·주문가능현금 필드 — 생기면 종목 수 상한 20 해제.
3. `rebalance.next_period_start` — 주말·휴장일에 실행돼 주문이 전부 skipped 여도 `next_run_at` 이 다음 달로 넘어가 **한 달이 통째로 사라지는** 버그.
4. 2027년 이후 KRX 휴장일 (`KRX_HOLIDAYS_2026` 다음 해 / `KRX_EXTRA_HOLIDAYS`). 없으면 리밸런싱 실행은 거부, dry-run 은 경고.
5. 기존 rebalance `_kis_symbol` 이 유니버스 밖 코드를 모두 `.KS` 로 둔다(코스닥 시세).
6. AWS(ECS·Ansible) 워커를 trading 전용으로 분리 — 지금은 단일 워커가 `--queues=celery,trading`.
7. `_place_live_order_via_gateway` 가 같은 clientOrderId 로 다시 불리면 `live_orders` UNIQUE 위반 — 태그 없이 같은 분에 같은 종목·방향을 두 번 내는 호출자 점검.
