"""AI_Quant 데이터 아키텍처: 정량 데이터 ORM 모델.

Alembic 마이그레이션 0009~0015에서 확정된 스키마를 그대로 반영한다.
컬럼 타입/제약은 schema_v1.sql 설계 + 이후 실측 기반 widen 마이그레이션
(amount_value, revenue_ttm 등 NUMERIC(30,2), per/pbr/pcr/psr NUMERIC(20,6),
composite_top_pct NUMERIC(10,4), financial_facts.context_ref TEXT)을 따른다.
"""
from __future__ import annotations

import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.models.base import Base


class Company(Base):
    __tablename__ = "companies"

    stock_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    corp_code: Mapped[str] = mapped_column(String(10), nullable=False)
    company_name: Mapped[str] = mapped_column(String(200), nullable=False)
    market: Mapped[Optional[str]] = mapped_column(String(20))
    listed_shares_latest: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now(), nullable=False
    )


class CompanySectorSnapshot(Base):
    __tablename__ = "company_sector_snapshots"
    __table_args__ = (PrimaryKeyConstraint("stock_code", "as_of_date"),)

    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    as_of_date: Mapped[datetime.date] = mapped_column(Date)
    sector_effective_date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    wics_code: Mapped[Optional[str]] = mapped_column(String(20))
    wics_name: Mapped[Optional[str]] = mapped_column(String(50))
    lookback_days: Mapped[Optional[int]] = mapped_column(Integer)
    source: Mapped[Optional[str]] = mapped_column(String(50))


class FinancialFact(Base):
    """XBRL canonical 레벨 원본. rcept_dt/rcept_no/version_seq/context_ref는
    PIT 백테스트 재현 및 자연키 유일성에 필수이므로 절대 제거 금지."""

    __tablename__ = "financial_facts"
    __table_args__ = (
        UniqueConstraint(
            "stock_code", "fiscal_year", "report_period", "fs_div", "statement_type",
            "account_id", "rcept_no", "version_seq", "context_ref",
            name="uq_financial_facts_natural_key",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    report_period: Mapped[str] = mapped_column(String(10), nullable=False)
    fs_div: Mapped[str] = mapped_column(String(3), nullable=False)
    statement_type: Mapped[str] = mapped_column(String(4), nullable=False)
    account_id: Mapped[str] = mapped_column(String(50), nullable=False)
    account_name: Mapped[Optional[str]] = mapped_column(String(200))
    amount_value: Mapped[Optional[float]] = mapped_column(Numeric(30, 2))
    period_type: Mapped[str] = mapped_column(String(10), nullable=False)
    period_start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    period_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    currency: Mapped[Optional[str]] = mapped_column(String(10))
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    rcept_no: Mapped[str] = mapped_column(String(20), nullable=False)
    rcept_dt: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    version_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    context_ref: Mapped[Optional[str]] = mapped_column(Text)


class FinancialMetric(Base):
    """서비스용 확정 재무 지표 (CFS 우선, OWNERS 계정 우선 적용 결과).
    as_of_date = 이 지표를 실제 사용 가능해진 날짜(공시 반영 이후),
    period_end_date = 재무제표 자체의 기준일. 둘은 항상 다를 수 있음(PIT 핵심)."""

    __tablename__ = "financial_metrics"
    __table_args__ = (PrimaryKeyConstraint("stock_code", "as_of_date"),)

    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    as_of_date: Mapped[datetime.date] = mapped_column(Date)
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    report_period: Mapped[str] = mapped_column(String(10), nullable=False)
    period_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    revenue_ttm: Mapped[Optional[float]] = mapped_column(Numeric(30, 2))
    net_income_ttm: Mapped[Optional[float]] = mapped_column(Numeric(30, 2))
    equity_latest: Mapped[Optional[float]] = mapped_column(Numeric(30, 2))
    operating_cash_flow_ttm: Mapped[Optional[float]] = mapped_column(Numeric(30, 2))
    source_lineage: Mapped[Optional[dict]] = mapped_column(JSONB)


class MarketPrice(Base):
    """KRX 일별 원시 시세(raw price). 액면분할 등 조정은 v1.1 보류."""

    __tablename__ = "market_prices"
    __table_args__ = (PrimaryKeyConstraint("stock_code", "trade_date"),)

    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    trade_date: Mapped[datetime.date] = mapped_column(Date)
    open_price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    high_price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    low_price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    close_price: Mapped[Optional[float]] = mapped_column(Numeric(15, 2))
    volume: Mapped[Optional[int]] = mapped_column(BigInteger)
    trade_value: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    market_cap: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    listed_shares: Mapped[Optional[int]] = mapped_column(BigInteger)


class ValuationMetric(Base):
    """financial_as_of_date로 이 valuation 계산에 쓰인 재무데이터 시점 추적 가능.
    주가/시총은 market_prices와 조인해서 사용(중복 저장 안 함)."""

    __tablename__ = "valuation_metrics"
    __table_args__ = (PrimaryKeyConstraint("stock_code", "as_of_date"),)

    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    as_of_date: Mapped[datetime.date] = mapped_column(Date)
    financial_as_of_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    per: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    pbr: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    pcr: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    psr: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))


class FactorScore(Base):
    """strategy_version으로 전략 로직 변경 이력 구분. Quality Score/ROE는
    원본 데이터 부재로 v1에서 제외."""

    __tablename__ = "factor_scores"
    __table_args__ = (PrimaryKeyConstraint("stock_code", "as_of_date", "strategy_version"),)

    stock_code: Mapped[str] = mapped_column(String(10), ForeignKey("companies.stock_code"))
    as_of_date: Mapped[datetime.date] = mapped_column(Date)
    strategy_version: Mapped[str] = mapped_column(
        String(30), server_default=text("'composite_v1'")
    )
    eps_growth_annual_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    peg: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    value_score: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    growth_score: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    composite_score: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    composite_rank: Mapped[Optional[int]] = mapped_column(Integer)
    composite_top_pct: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
