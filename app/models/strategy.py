"""월초 ATR 리밸런싱 전략(atr_rebal) 모델.

KIS 모의계좌(stock-coin-trade 게이트웨이)를 기존 auto_trade 와 같이 쓰되, 이 전략이 산 수량·평균가는
전용 장부(StrategyPosition)로 따로 관리한다. 계좌 전체가 아니라 투자금액 한도 안에서만 비중을 맞추고,
목표 파일에 없는 계좌 보유 종목은 건드리지 않는다.

  StrategyPlan     유저당 1행. 투자금액·손절·ATR 배수·주문 한도 등 설정
  StrategyTarget   업로드한 목표 포트폴리오(종목·정규화 비중·파일 ATR)
  StrategyPosition 전략 전용 장부(수량·평균가·리밸런싱 날 고정한 ATR)
  StrategyRun      dry-run / 리밸런싱 1회의 계획 스냅샷(실효 투자금액·경고·종목별 계획 행)
  StrategySlice    리밸런싱 날 시분할 주문 1회차
  StrategyStopEvent 손절 1건(조건 충족 → 리밸런싱 주문 취소 → 장부 수량 시장가 매도). 리밸런싱 없는 날에도 생긴다
"""
from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, CreatedAtMixin, UpdatedAtMixin, UUIDPkMixin

STRATEGY_ATR_REBAL = "atr_rebal"
ATR_SOURCES = ("file", "kis", "none")          # none: 파일에 없고 KIS 계산도 실패 → % 손절로 대체
RUN_KINDS = ("dry_run", "rebalance")
RUN_STATUSES = ("planned", "running", "done", "failed", "skipped")
SLICE_STATUSES = ("planned", "submitted", "skipped", "cancelled", "filled", "failed")
# waiting: 같은 종목 리밸런싱 주문 취소 확인 대기(다음 감시에서 이어서) · planned: 주문 직전 기록
STOP_STATUSES = ("waiting", "planned", "submitted", "filled", "failed", "skipped")


class StrategyPlan(Base, UUIDPkMixin, CreatedAtMixin, UpdatedAtMixin):
    __tablename__ = "strategy_plans"

    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("users.id"), unique=True, nullable=False)
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default=STRATEGY_ATR_REBAL)
    name: Mapped[str] = mapped_column(String(60), nullable=False, default="월초 ATR 리밸런싱")
    environment: Mapped[str] = mapped_column(String(10), nullable=False, default="paper")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # dry_run 이면 계획만 만들고 주문은 내지 않는다. 기존 auto_trade 를 막는 것도 dry_run=False 로 켰을 때뿐.
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    invest_amount: Mapped[float] = mapped_column(Float, nullable=False, default=10_000_000)
    max_tickers: Mapped[int] = mapped_column(Integer, nullable=False, default=20)   # 게이트웨이 잔고가 모의투자 20종목에서 잘림
    atr_period: Mapped[int] = mapped_column(Integer, nullable=False, default=14)
    buy_band_atr_mult: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    stop_atr_mult: Mapped[float] = mapped_column(Float, nullable=False, default=2.0)
    stop_loss_pct: Mapped[float] = mapped_column(Float, nullable=False, default=-10.0)      # ATR 없을 때 대체 손절
    max_order_amount: Mapped[float] = mapped_column(Float, nullable=False, default=5_000_000)
    max_orders_per_day: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)  # risk_guard 와 별도인 전략 전용 한도

    target_source: Mapped[str] = mapped_column(String(10), nullable=False, default="file")  # file | screen(예정)
    target_file_name: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    targets_uploaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class StrategyTarget(Base, UUIDPkMixin, CreatedAtMixin):
    __tablename__ = "strategy_targets"
    __table_args__ = (UniqueConstraint("plan_id", "symbol", name="uq_strategy_targets_plan_symbol"),)

    plan_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)          # KRX 6자리
    name: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    weight: Mapped[float] = mapped_column(Float, nullable=False)             # 정규화 비중(0~1, 합 1)
    weight_input: Mapped[float | None] = mapped_column(Float, nullable=True)  # 파일에 적힌 원래 값(없으면 동일비중)
    atr_input: Mapped[float | None] = mapped_column(Float, nullable=True)     # 파일 ATR(원). 비면 KIS 일봉으로 계산
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # 파일 행 순서


class StrategyPosition(Base, UUIDPkMixin, UpdatedAtMixin):
    __tablename__ = "strategy_positions"
    __table_args__ = (UniqueConstraint("plan_id", "symbol", name="uq_strategy_positions_plan_symbol"),)

    plan_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    atr: Mapped[float | None] = mapped_column(Float, nullable=True)          # 리밸런싱 날 고정, 다음 리밸런싱까지 유지
    atr_source: Mapped[str] = mapped_column(String(4), nullable=False, default="none")
    atr_as_of: Mapped[date | None] = mapped_column(Date, nullable=True)       # ATR 계산에 쓴 마지막 완성 일봉 날짜
    # 계좌에 전략 밖 보유분이 섞인 적이 있으면 True. KIS 매입평균가는 매도해도 바뀌지 않는 평균법이라, 섞인 뒤에는
    # 계좌 수량이 장부와 같아져도 KIS 평균가로 장부를 맞추면 안 된다. 계좌 보유가 0이 되면(KIS 평균가 초기화) False 로 돌아간다.
    account_mixed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class StrategyRun(Base, UUIDPkMixin, CreatedAtMixin, UpdatedAtMixin):
    __tablename__ = "strategy_runs"
    __table_args__ = (
        Index("ix_strategy_runs_plan_created", "plan_id", "created_at"),
        # 리밸런싱은 하루 1회 (참고 구현 last_run.txt 대체). dry-run 은 여러 번 가능.
        Index("uq_strategy_runs_rebalance_once", "plan_id", "run_date", unique=True,
              postgresql_where=text("kind = 'rebalance'")),
    )

    plan_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    run_date: Mapped[date] = mapped_column(Date, nullable=False)              # KST 날짜
    kind: Mapped[str] = mapped_column(String(10), nullable=False)             # RUN_KINDS
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="planned")
    invest_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0)       # 설정값
    effective_budget: Mapped[float] = mapped_column(Float, nullable=False, default=0)    # min(설정값, 장부 평가액 + 예수금)
    cash_balance: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    ledger_eval: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    warnings: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    rows: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)              # 종목별 계획 행(plan_*.csv 와 대응)
    summary: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    note: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    # 리밸런싱 실행 상태(4단계). attempt 는 수동 재실행(force)마다 1 증가 → 새 clientOrderId·회차 기록.
    trigger: Mapped[str] = mapped_column(String(10), nullable=False, default="auto")   # auto(첫 거래일) | manual
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class StrategySlice(Base, UUIDPkMixin, CreatedAtMixin, UpdatedAtMixin):
    __tablename__ = "strategy_slices"
    __table_args__ = (
        Index("ix_strategy_slices_run_symbol", "run_id", "symbol"),
        # 회차 1개 = (실행, 시도, 종목, 방향, 회차번호) 1행 — 재시작해도 같은 회차를 두 번 처리하지 않는다
        UniqueConstraint("run_id", "attempt", "symbol", "side", "seq", name="uq_strategy_slices_round"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_runs.id", ondelete="CASCADE"), nullable=False)
    plan_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)              # BUY | SELL
    seq: Mapped[int] = mapped_column(Integer, nullable=False)                 # 회차 번호
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    order_type: Mapped[str] = mapped_column(String(6), nullable=False, default="LIMIT")
    price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("live_orders.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="planned")
    reason: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    filled_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)        # 장부에 이미 반영한 체결 수량
    avg_fill_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)


class StrategyStopEvent(Base, UUIDPkMixin, CreatedAtMixin, UpdatedAtMixin):
    __tablename__ = "strategy_stop_events"
    __table_args__ = (
        Index("ix_strategy_stop_events_plan_day_symbol", "plan_id", "trade_date", "symbol"),
        UniqueConstraint("plan_id", "trade_date", "symbol", "seq", name="uq_strategy_stop_events_seq"),
    )

    plan_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)          # KST
    symbol: Mapped[str] = mapped_column(String(6), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)    # 같은 날 같은 종목 재시도 번호
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="waiting")
    trigger_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)   # 조건을 판정한 현재가
    avg_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)       # 장부 평균가
    atr: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    basis: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    live_order_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("live_orders.id"), nullable=True)
    filled_qty: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_fill_price: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    reason: Mapped[str] = mapped_column(String(300), nullable=False, default="")
