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
| 4 | 시분할 주문 실행(5분 회차)·15:00 일괄 취소·수동 시작/재실행/중지·이어받기 | 완료 (`feat/atr-rebal-step4-slicing`) |
| 5 | 손절 감시·장부 동기화 | 완료 — 2026-10-23 실주문 승인(`026ef0f`)·첫 모의 운영 |
| 6 | reconciliation 반영·화면·장 마감 결과 | 예정 |
| 7 | `/api/quant/financials/screen` 상위 N개 연동 | 예정 |

## 확정된 규칙

- **계좌**: 기존 auto_trade 와 같은 KIS 모의계좌. 전략을 실주문 모드(`dry_run=false`)로 켜면 broker=kis·quant_mode=live 인
  auto_trade(시스템 배치 포함)를 모두 끄고, 켜져 있는 동안 다시 켜지 않는다(시작 API 409, Beat 사이클·KIS 배치에서 강제 해제).
- **장부**: 전략이 산 수량·평균가만 `strategy_positions` 로 관리. 시작 시 기존 보유분은 편입하지 않는다. 수동 매도가 생기면 장부를 계좌 수량에 맞춰 줄인다.
- **목표 파일**: 비중은 비율로 환산(없으면 동일비중). 종목 수 상한 기본 **20**(게이트웨이 잔고가 모의투자 20종목에서 잘림 — st 연속조회 전까지).
- **투자금액**: 실효 투자금액 = min(설정값, 장부 전체 평가액 + 예수금). 줄어들면 경고 로그·실행 기록.
  목표에서 빠진 장부 종목도 그날 팔아 현금이 되므로 평가액에 넣는다.
- **목표수량**: floor(실효 투자금액 × 비중 ÷ 현재가).
- **목표 파일 밖 종목 — 장부로 구분한다(2026-10-22 수정)**
  - **장부에 있는데 현재 목표 파일에서 빠진 종목**: 리밸런싱 날 **장부 수량 전량 매도**(목표수량 0). 매도 시분할·14:30 마감 규칙은 같다.
    매도가 끝날 때까지(장부 수량 0) 손절 감시 대상이다. `plan.csv` 에는 `목표제외=Y`, 비고 `목표 제외 → 전량 매도` 로 보인다.
  - **장부에 없는 종목(직접 산 종목)**: 리밸런싱·손절 모두 건드리지 않는다. 계획 행에도 나오지 않는다.
- **시분할**: 5분 간격 회차. 매도 09:10~14:30(65회), 매수 09:10~14:55(70회), 15:00 미체결 일괄 취소.
  종목별 주문 회차 수 = min(남은 회차, 남은 주식 수), 회당 최소 1주. **주문 회차는 남은 매매 시간에 고르게 배치한다**
  (2026-10-23 수정, 예: 09:10 에 4주 → 09:10·10:35·12:05·13:30). 회차 안의 종목들은 5분 안에 나눠 처리(호출 예산 분당 60회).
- **주문 유형**: 게이트웨이가 MARKET/LIMIT 만 받으므로 최유리지정가 대신 **LIMIT: 매수 현재가 + 1틱, 매도 현재가 − 1틱**(호가 단위 맞춤).
  미체결은 **다음 주문 회차**에 취소 → 확인 → 남은 수량 재주문(주문 회차 사이에는 열린 주문을 그대로 둔다).
- **ATR**: 파일 값 우선, 비면 KIS 일봉(FHKST03010100, 수정주가)에서 **오늘 봉을 뺀** 완성 일봉으로 Wilder 14일. 리밸런싱 날 고정.
  ATR 이 없거나 계산 실패하면 % 손절(기본 −10%, 설정 가능), 매수 범위 제한 없음.
- **매수 범위**: 리밸런싱 날 시가 ± ATR×1. 범위 밖이면 그 회차 건너뜀.
- **손절**: 장부 평균가 − ATR×2 아래면 장부 수량 전량 시장가. 매 거래일 09:05~15:15. 같은 종목의 열린 주문을 먼저 취소.
  리밸런싱 날 손절되면 그 종목의 남은 회차 취소. 자동 익절 없음.
- **위험관리**: risk_guard 의 일손실 한도·일 주문 수·쿨다운은 이 전략의 **매수에만** 적용, 손절 매도는 막지 않는다(kill switch 만 예외).
  손절 주문이 열려 있거나 체결 확인 대기 중이면 새 손절을 내지 않는다. 전략 전용 일 주문 수 한도 `strategy_plans.max_orders_per_day`.

## API

`/api/strategy/atr` — `GET|PUT /plan`, `POST /targets`(multipart `file`), `GET /targets`, `POST /dry-run`,
`GET /runs`, `GET /runs/{id}`, `GET /runs/{id}/plan.csv`, `POST /activate`(`{"dry_run": true}`), `POST /deactivate`,
`POST /rebalance/start`(`{"force": false}`), `GET /rebalance/today`, `POST /rebalance/stop`.
`plan.csv` 의 앞 10개 컬럼은 참고 구현 `state/plan_YYYYMMDD.csv` 와 같다. 단 `보유수량`은 계좌가 아니라 **전략 장부** 수량,
`주문가능수량`은 min(장부, 계좌 보유) — 게이트웨이 잔고에 KIS 주문가능수량이 없어서다. 뒤쪽 추가 컬럼에 `목표제외`(Y/N)가 있다.

## 4단계 실행 방식

- **승인 게이트**: `service.LIVE_EXECUTION_AVAILABLE = False` 인 동안 Beat tick·수동 시작·`dry_run=false` 활성화가 모두 거부된다.
  실제 계좌 dry-run 확인 후 이 값을 바꾸는 커밋이 승인 절차다.
- **Beat**: `strategy.rebalance_tick` 평일 09:00~14:59 매분 + 15:00~15:02·15:20~15:22 (trading 큐). 휴장일은 태스크 안에서 거른다.
  환경별 Redis 잠금(`strategy:acct-lock:kis:<env>`, 55초)으로 tick 이 겹치지 않는다.
- **주문 회차 배치**: 종목·방향마다 처음 할 일이 생긴 회차에 `plan_slots`(k = min(남은 회차, 남은 수량)개를 남은 회차에 고르게)로
  정해 `rows[].slots_BUY/SELL` 에 둔다. 마지막 주문 회차 뒤에도 남은 수량이 있으면 이후 매 회차 따라잡는다.
  회차 수량 = ceil(남은 수량 ÷ 남은 주문 회차 수).
- **회차**: 이번 회차가 주문 회차인 종목만 매도 먼저 tick 당 최대 4종목 → 많으면 같은 회차 안의 다음 tick 으로 넘어간다.
  할 일이 없는 tick 은 잔고를 조회하지 않는다.
  매도 마지막 회차(14:30)는 1분 창. 종목 처리 = 같은 종목 열린 주문 취소·재조회 → 장부에 체결 반영 → 남은 수량 재계산 →
  (매수) 일손실 한도·시가 ± ATR 범위·쿨다운 → 회차 수량 = ceil(남은 ÷ min(남은 회차, 남은 수량)), 1건 상한으로 자름 →
  LIMIT 매수 현재가+1틱 / 매도 현재가−1틱. 회차 기록을 **주문 전에** 커밋(재시작 시 같은 회차 중복 주문 방지).
- **이어받기**: 그날 `strategy_runs(kind=rebalance)` 가 있으면 새로 만들지 않고 남은 회차만 진행한다. 놓친 회차는 건너뛰고
  남은 수량을 남은 회차로 다시 나눈다. 주문 직전에 죽어 `planned` 로 남은 회차는 live_orders 와 연결하거나 실패로 닫는다.
- **수동 시작** `POST /rebalance/start` (참고 구현 `--rebalance`): 첫 거래일이 아니어도 오늘 시작. 이후 회차는 Beat 가 진행.
- **재실행** `POST /rebalance/start {"force": true}` (참고 구현 `--force`): 열린 주문 취소 → 장부 기준으로 계획을 다시 세우고
  attempt+1(새 clientOrderId 태그 `ATR<attempt>`). 이미 체결된 수량은 장부에 있으므로 빠진다.
- **중지** `POST /rebalance/stop`: 열린 주문 취소 → `skipped`. **진행 상황** `GET /rebalance/today`.
- **15:00** 열린 리밸런싱 주문 일괄 취소, **15:20** 장부 기준 목표 대비 결과(`summary.result`) 확정 → `done`.
  **15:00 까지 못 산(못 판) 물량은 다음 리밸런싱까지 그대로 둔다** — 다음 날 이어서 매수·매도하지 않는다.
  매수 범위 밖이라 건너뛴 회차 수는 `summary.result[].band_skips` 에 남는다.
- **위험관리 매핑(2026-10-22 승인)**: risk_guard 의 일손실 한도는 사용자 설정 그대로 매수에만 적용. 일 주문 수는 risk_guard 기본(20건)이
  아니라 전략 전용 `max_orders_per_day`(기본 1000)로 센다. 쿨다운은 risk_guard 값을 쓰되 `min(설정, 회차 간격 − 1분 = 4분)` 으로
  자른다 — 그대로(30분) 쓰면 5분 회차가 30분에 한 번으로 줄어든다.
- 비상 정지(kill switch)면 tick 이 아무것도 하지 않는다. 손절로 `rows[].halted` 가 켜진 종목은 남은 회차를 건너뛴다(5단계에서 설정).

## 5단계 손절 감시·장부 동기화

- **Beat** `strategy.stop_loss_watch`: 거래일 09:05~15:15 매분(trading 큐). 리밸런싱 실행 상태와 무관하게 돈다 —
  `POST /rebalance/stop` 이나 15:00 마감 뒤에도 계속 감시한다. **비상 정지(kill switch)일 때만 멈춘다.**
  (플랜을 끄면(`/deactivate`) 전략 자체가 꺼지므로 감시도 멈춘다. 실주문 모드 플랜을 끌 때 장부에 보유 종목이 있으면
  응답 `warnings` 에 "손절 감시 없이 남는다"는 경고가, 열린 전략 주문이 있으면 "자동으로 정리되지 않는다"는 경고가 들어간다.)
- **대상**: 장부에 수량이 남은 모든 종목 — 목표 종목과, 목표에서 빠져 매도 중인 종목(다 팔릴 때까지). 장부에 없는 계좌 종목은 대상이 아니다.
- **판정**: 잔고 1회 조회의 현재가 ≤ 손절가. 손절가 = 장부 평균가 − ATR×2(리밸런싱 날 고정한 ATR), ATR 없으면 평균가 −10%(설정).
- **실행 순서**: 손절 이벤트(`strategy_stop_events`)를 먼저 기록·커밋 → 종목 잠금 → 그 종목 리밸런싱 주문 **매수·매도 모두 취소 → 확인** →
  체결 반영 → min(장부, 계좌) 수량 **전량 시장가 매도**(태그 `SL`). 취소가 확인되지 않으면 `waiting` 으로 남기고 다음 감시에서 이어서 한다.
- **중복 방지**: 같은 종목에 손절 주문이 열려 있거나 체결 확인 대기(`submitted`/`planned`) 중이면 새 손절을 내지 않는다.
  주문이 거부·취소되면 같은 날 다시 시도(seq+1), 3회 실패하면 그날은 멈추고 알림.
- **위험관리 예외**: 손절 매도는 일손실 한도·일 주문 수·쿨다운·전략 일 주문 수 한도에 막히지 않는다.
- **EGW00215**(KIS 원장 초당 거래건수 초과, st 가 HTTP 500 으로 전달): 손절은 실패로 세지 않고 `waiting` → 다음 감시.
  리밸런싱 회차는 1.5초 뒤 같은 tick 안에서 새 clientOrderId(`ATR<attempt>R`)로 1회 재시도. 잔고 조회(공유 잔고·계획용)도 1회 재시도.
- **원장 조회 줄이기**: 리밸런싱 tick 과 손절 감시가 잔고를 15초 동안 공유(Redis `strategy:balance:kis:<env>`),
  손절 감시는 매분 **30초**에 실행(Beat countdown) — 정각의 리밸런싱 tick 과 같은 초에 겹치지 않게.
- **체결 반영(이중 반영 방지)**: 회차·손절 기록의 반영 수량을 비교 후 갱신(`UPDATE … WHERE filled_qty = 이전 값`)으로 차지한 쪽만
  장부에 더하고, 장부 행은 `SELECT … FOR UPDATE` 로 잠근다. 체결 1건 = 짧은 트랜잭션(기록 → 장부 순 잠금).
- **장부 평균가**: 모의투자는 체결가를 주지 않아 장부가 주문 지정가(현재가+1틱)로 평균을 낸다. 손절 감시가 KIS 매입평균가로 맞춘다 —
  그 종목의 계좌 보유가 전부 전략 몫(계좌 수량 = 장부 수량, `account_mixed=false`)이고 열린 주문·체결 미반영이 없을 때만.
  계좌에 전략 밖 보유분이 섞인 적이 있으면(`account_mixed=true`, 예: 직접 산 삼성전자 10주) KIS 평균법 평균가가 섞여 있으므로
  계좌·장부 보유가 모두 0이 될 때까지 맞추지 않는다. 감사 로그 `strategy.atr_rebal.ledger_avg_sync`.
- **게이트웨이 일시 오류**: 409 `ORDER_IN_PROGRESS`(st 가 같은 환경의 다른 주문을 처리 중)는 실패 3회에 세지 않고
  3초 뒤 **새 clientOrderId 로 승인(order-approval)부터 다시** 받아 최대 3번 재시도한다. 그래도 409 면 `waiting` 으로 두고 다음 감시에서 이어서 한다.
  429 `RATE_LIMITED` 도 실패로 세지 않고 바로 `waiting` → 다음 감시. 손절 주문은 `orderType=MARKET`, `price=0` 으로 나간다.
- **리밸런싱과의 관계**: 그날 손절 이벤트가 있거나 손절 주문이 열린 종목은 리밸런싱 tick 이 **건너뛴다(취소도 새 주문도 안 함)**.
  15:00 일괄 취소·`/rebalance/stop` 은 리밸런싱 주문(태그 `ATR`)만 취소하고 손절 주문은 건드리지 않는다.
  같은 종목을 동시에 건드리지 않도록 종목 잠금(`strategy:sym-lock:kis:<env>:<code>`)을 둘 다 잡는다.
- **`/rebalance/stop`**: 열린 리밸런싱 주문을 모두 취소한다. 취소 확인이 늦은 주문은 `summary.stop_cancel.still_open` 에 남고,
  Beat tick 이 실행이 `skipped`/`done` 이어도 남은 리밸런싱 주문을 계속 정리한다.
- **장부 동기화**: 매 감시와 리밸런싱 시작 전에, 계좌 보유수량이 장부보다 적으면 장부를 계좌 수량으로 줄이고 수동 매도로 감사 로그
  (`strategy.atr_rebal.ledger_sync`)에 남긴다. 그 종목에 매도 주문(리밸런싱·손절)이 열려 있으면 체결이 확인될 때까지 건너뛴다.
  체결 반영은 오늘 실행분만이 아니라 `submitted` 로 남은 모든 회차를 대상으로 한다(장 마감 뒤 늦은 체결 확인 포함).

## 게이트웨이 호출 예산 (API 키당 분당 60회)

종목 1개 리밸런싱 처리(최악) = 이전 주문 상태 1 + 취소 1 + 재조회 1 + 승인 1 + 주문 1 = 5회(시세 `/api/kis-chart` 는 한도 밖).
리밸런싱 tick = 잔고 1 + **4종목** × 5 = 21회. 같은 분의 다른 호출(최악): 손절 감시 ≈ 6, confirm_fills(2분, 열린 주문 1건당 1) ≤ 20,
reconcile(10분) ≈ 2 → 합계 ≈ 49 < 60. 6종목이면 59 로 여유가 없다. st 는 KIS 호출을 1.05초 간격으로 직렬화해 종목당 ≈ 8초 →
4종목 ≈ 32초 < tick 제한 55초. 20종목이면 한 회차를 5 tick 에 나눠 처리한다. 409 재시도가 몰리면 손절 1건이 승인·주문 최대 8회를 더 쓴다.

## 별도 작업 목록 (이번 범위 밖)

1. **[실전 전환 전] st 호가 엔드포인트** `/openapi/v1/kis/orderbook/{symbol}` (FHKST01010200) — LIMIT 대체를 매도1/매수1호가로 바꾸기. 또는 st 에 `BEST`(`ORD_DVSN=03`) 추가.
2. **st 잔고 연속조회**(`tr_cont`/`CTX_AREA_*`)와 주문가능수량·주문가능현금 필드 — 생기면 종목 수 상한 20 해제.
3. `rebalance.next_period_start` — 주말·휴장일에 실행돼 주문이 전부 skipped 여도 `next_run_at` 이 다음 달로 넘어가 **한 달이 통째로 사라지는** 버그.
4. 2027년 이후 KRX 휴장일 (`KRX_HOLIDAYS_2026` 다음 해 / `KRX_EXTRA_HOLIDAYS`). 없으면 리밸런싱 실행은 거부, dry-run 은 경고.
5. 기존 rebalance `_kis_symbol` 이 유니버스 밖 코드를 모두 `.KS` 로 둔다(코스닥 시세).
6. AWS(ECS·Ansible) 워커를 trading 전용으로 분리 — 지금은 단일 워커가 `--queues=celery,trading`.
7. `_place_live_order_via_gateway` 가 같은 clientOrderId 로 다시 불리면 `live_orders` UNIQUE 위반 — 태그 없이 같은 분에 같은 종목·방향을 두 번 내는 호출자 점검.
8. **[st] 원장 조회 간격 제한** — KIS 모의투자 원장 조회(잔고·주문) 한도가 st 의 전역 1.05초 간격보다 빡빡하다(2026-10-23 EGW00215 20건,
   전부 잔고 조회, 3초 안에 원장 조회 3건이 몰릴 때). 원장 계열 TR 전용 간격(≈2초) 또는 TR 별 버킷을 st `kis_request` 에 추가.
9. **[st] EGW00215 재시도** — st `kis_request` 는 EGW00201 만 재시도한다. EGW00215 도 짧게 대기 후 재시도.
10. **[st] 모의투자 체결가** — 체결 목록이 비어 보유수량 변화로 추정할 때 체결가를 LIMIT 주문가로 둔다(`kis_autotrade.py:678`).
    KIS 잔고의 매입평균가 변화로 실제 체결가를 역산하면 lumina 쪽 평균가 동기화가 필요 없어진다.
11. 409·429 로 실패한 시도마다 `live_orders` 에 ERROR 행이 남고 `notify_order_error` 알림이 간다(손절 재시도 시 알림이 여러 번). 일시 오류는 알림을 묶는 것을 검토.

## 진행 상황 (2026-10-22)

**완료**
- 1~5단계 코드: st 확인 → 기반(Wilder ATR·거래일·trading 큐·`live_orders.source`) → 계획·dry-run → 시분할 실행 → 손절·장부 동기화.
  보완: 목표 제외 장부 종목 전량 매도, `/deactivate` 경고, 손절 409·429 재시도, tick 당 4종목(호출 예산). pytest 465 통과.
- 로컬 검증: 마이그레이션 0017~0020 적용·`q0016_merge` 왕복, trading 워커 헬스체크, 큐 라우팅, 로그 에러 없음.
- st 연동(로컬): MariaDB·quant Postgres 백업(`~/backups/stock-coin-trade/`) → st `origin/main`(7d864c6) 재빌드
  (`kis_autotrade_order`·`kis_order_approval`·`api_key.scopes` 추가) → `crypto-mock-python` shared-net 연결 →
  KIS 모의 자격증명 입력 → API 키 발급(라벨 `lumina-autotrade-paper`, `scopes=kis:order`) →
  lumina `.env` `STOCK_COIN_TRADE_BASE_URL=http://crypto-mock-python:8200`, `KIS_ENVIRONMENT=paper` → 게이트웨이 잔고 200.
- 실제 계좌 dry-run(you1@example.com, 투자금액 1,000만, 참고 구현 portfolio.csv 5종목): 목표수량 1/6/9/11/12주로 참고 구현과 같음.
  ATR 5종목 모두 KIS 일봉(오늘 봉 제외, 기준일 10-21)으로 계산, 독립 계산과 일치. 매수 계획 39주 ≈ 887만 원.
  삼성전자는 계좌 기존 10주가 장부 밖이라 9주 신규 매수(참고 구현은 −1주). 주문 0건.
- 브랜치 `feat/atr-rebal-step5-stoploss`(2~5단계 포함). `LIVE_EXECUTION_AVAILABLE = False`.

**다음 할 일**
1. LIVE 승인 커밋: `service.LIVE_EXECUTION_AVAILABLE = True` (활성화 거부 문구도 "승인 전"으로 정리).
2. 첫 모의 운영: 1~2종목·소액으로 `/activate {"dry_run": false}` → `POST /rebalance/start` → `GET /rebalance/today` 로 회차 관찰.
3. 모의 운영 확인 후 이 브랜치 하나로 main PR (로컬 main 의 미푸시 커밋 10개 포함 여부 정리).

**미결정**
- 화면 시세 소스: `MARKET_DATA_SOURCE=kis` 이면 화면 시세가 st 의 KIS 호출 줄(1.05초 직렬)을 주문·손절과 공유해 지연될 수 있다.
  `/api/kis-chart` 는 API 키 한도(분당 60회) 밖이라 429 는 아니다. 선택지: 화면만 `yahoo` / st 에 시세용 KIS 앱키 분리.

## 진행 상황 (2026-10-23) — 첫 모의 운영

**한 것**
- 기존 rebalance 엔진 플랜 2개(목표 0개) 비활성화. LIVE 승인 커밋 `026ef0f`(`LIVE_EXECUTION_AVAILABLE=True`).
- 첫 운영(you1@example.com, 수동 시작 09:07): 삼성전자·KB금융 50:50, 투자금액 200만, ATR 매수범위 ×1·손절 ×2.
  결과 `done`: 목표 4주·6주 모두 체결(09:10~09:45, 당시 구현은 회차를 연속으로 냄). 손절 없음. 주문 13건 중 3건 EGW00215 실패 → 다음 회차에 채움.
- 발견 → 이 커밋에서 수정:
  - **체결 이중 반영**: 손절 감시·리밸런싱 tick 이 다른 프로세스에서 같은 체결을 동시에 반영 → KB금융 장부 한때 9주(실제 6주),
    계좌 기준 동기화가 수량만 6주로 되돌려 평균가 152,728.89 로 남음(KIS 매입평균가 152,508). 실제 PostgreSQL 경합 테스트로 재현·수정 확인.
  - **EGW00215** 20건(전부 잔고 조회): 매분 정각에 손절 감시·리밸런싱 tick·주문 전 잔고 확인이 3초 안에 몰림. 첫 `/rebalance/start` 409 도 이것.
  - 장부 평균가가 체결가가 아니라 주문 지정가(+1틱) 기준 → KIS 매입평균가 동기화(전부 전략 몫일 때만).
  - 회차가 하루에 고르게 퍼지지 않음 → 남은 매매 시간에 분산.
- 마이그레이션 0021 `strategy_positions.account_mixed`(+ 계획 스냅샷 기준 백필: 삼성전자 true, KB금융 false).

**다음**
- 10-26(월) 09:05:30 첫 손절 감시에서 KB금융 장부 평균가 152,508 자동 동기화 확인(감사 로그 `ledger_avg_sync`).
- 사용자가 HTS 로 삼성전자 직접 산 10주 매도 → 장부 4주·평균가 207,000 유지 확인.
- 11-02(월) 첫 거래일 자동 리밸런싱(회차 분산 적용 첫날) 관찰.
- 브랜치 하나로 main PR(로컬 main 미푸시 커밋 10개 정리 포함).

- `account_mixed` 유지 규칙 확인: 계획(`_freeze_atr`)은 true 로만 바꾸고, false 는 손절 감시의 "계좌·장부 보유 모두 0" 에서만.
  시나리오 테스트(14주 섞임 → HTS 10주 매도 → 11/2 계획 → true·207,000 유지) 추가.

**11/2(월) 첫 자동 리밸런싱에서 확인할 것**
1. **Beat 자동 시작** — 09:10 tick 이 실행을 만든다(`trigger=auto`, 수동 시작 불필요). 09:10 전에 시작되지 않는다.
   `select run_date, trigger, status, attempt, started_at at time zone 'Asia/Seoul' from strategy_runs where kind='rebalance' order by run_date desc limit 1`
   목표가 이미 맞으면(삼성전자 4·KB금융 6, 같은 파일·투자금액) 주문이 없을 수 있다 — 그 경우 `summary.buy_qty = 0` 이고 tick 은 `idle`.
2. **회차 분산** — `rows[].slots_BUY/SELL` 이 남은 시간에 고르게 잡히고, 주문(`strategy_slices`) 시각이 그 회차와 맞는다(연속 5분 간격이 아님).
   `select to_char(created_at at time zone 'Asia/Seoul','HH24:MI') t, symbol, side, seq, quantity, status from strategy_slices where run_id = '<run id>' order by created_at`
3. **평균가 동기화** — 매수 체결 뒤 손절 감시(매분 30초)가 KB금융처럼 전부 전략 몫인 종목만 KIS 매입평균가로 맞춘다(`ledger_avg_sync`),
   삼성전자(`account_mixed=true`)는 바뀌지 않는다.
   `select symbol, quantity, avg_price, account_mixed from strategy_positions order by symbol`
4. 그 밖에: EGW00215 가 줄었는지(st `api_usage_log` 의 `result_summary like '원장%'`), 장부 동기화(`ledger_sync`) 경고가 없는지(이중 반영 재발 여부),
   15:00 일괄 취소·15:20 `done`.

**미결정**
- 화면 시세 소스(10-22 와 같음). st 쪽 근본 해결(별도 작업 8~10)은 stock-coin-trade 저장소에서.
