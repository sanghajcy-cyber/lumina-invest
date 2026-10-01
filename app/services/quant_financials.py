"""AI_Quant 정량 데이터 조회 서비스 레이어.

7개 PostgreSQL 테이블(companies, company_sector_snapshots, financial_facts,
financial_metrics, market_prices, valuation_metrics, factor_scores)에 대한
읽기 전용 조회를 담당한다. 질문분류(FastAPI) → 정량 질문 → 이 서비스를 거쳐
구조화된 dict를 반환하고, 최종 자연어 답변은 Ollama 레이어에서 생성한다.

financial_facts(원본 XBRL, 184만 행)는 여기서 직접 노출하지 않는다 — 서비스용
확정 지표는 financial_metrics/valuation_metrics이고, financial_facts는 필요
시(예: lineage 상세 조회) 별도 엔드포인트로 추가한다.
"""
from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import desc, func, nullslast, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Company,
    CompanySectorSnapshot,
    FactorScore,
    FinancialMetric,
    MarketPrice,
    ValuationMetric,
)

DEFAULT_STRATEGY_VERSION = "composite_v1"


def _num(v: Any) -> Any:
    """Decimal -> float (JSON 직렬화 용이하게). None은 그대로 통과."""
    if isinstance(v, Decimal):
        return float(v)
    return v


def _row_to_dict(row: Any, columns: list[str]) -> dict:
    return {c: _num(getattr(row, c)) for c in columns}


# ── companies / sector ──────────────────────────────────────────────

async def get_company(db: AsyncSession, stock_code: str) -> Optional[dict]:
    row = (await db.execute(
        select(Company).where(Company.stock_code == stock_code)
    )).scalar_one_or_none()
    if row is None:
        return None
    return {
        "stock_code": row.stock_code,
        "corp_code": row.corp_code,
        "company_name": row.company_name,
        "market": row.market,
        "listed_shares_latest": row.listed_shares_latest,
    }


async def get_latest_sector(db: AsyncSession, stock_code: str) -> Optional[dict]:
    row = (await db.execute(
        select(CompanySectorSnapshot)
        .where(CompanySectorSnapshot.stock_code == stock_code)
        .order_by(desc(CompanySectorSnapshot.as_of_date))
        .limit(1)
    )).scalar_one_or_none()
    if row is None:
        return None
    return {
        "as_of_date": row.as_of_date.isoformat(),
        "wics_code": row.wics_code,
        "wics_name": row.wics_name,
    }


async def _stock_codes_in_sector(db: AsyncSession, wics_code: str) -> set[str]:
    """각 종목의 '가장 최근' 섹터 스냅샷 기준으로 해당 WICS 섹터에 속하는
    종목코드 집합을 반환한다. (과거 특정 시점의 섹터 소속은 반영하지 않는
    단순화된 버전 — 필요 시 as_of_date 기준 as-of 조회로 확장 가능)"""
    subq = (
        select(
            CompanySectorSnapshot.stock_code,
            func.max(CompanySectorSnapshot.as_of_date).label("max_date"),
        )
        .group_by(CompanySectorSnapshot.stock_code)
        .subquery()
    )
    stmt = (
        select(CompanySectorSnapshot.stock_code)
        .join(
            subq,
            (CompanySectorSnapshot.stock_code == subq.c.stock_code)
            & (CompanySectorSnapshot.as_of_date == subq.c.max_date),
        )
        .where(CompanySectorSnapshot.wics_code == wics_code)
    )
    return set((await db.execute(stmt)).scalars().all())


# ── financial_metrics ───────────────────────────────────────────────

_METRICS_COLUMNS = [
    "fiscal_year", "report_period",
    "revenue_ttm", "net_income_ttm", "equity_latest", "operating_cash_flow_ttm",
]


async def list_financial_metrics(
    db: AsyncSession, stock_code: str,
    start: Optional[datetime.date] = None, end: Optional[datetime.date] = None,
    limit: int = 50,
) -> list[dict]:
    stmt = select(FinancialMetric).where(FinancialMetric.stock_code == stock_code)
    if start:
        stmt = stmt.where(FinancialMetric.as_of_date >= start)
    if end:
        stmt = stmt.where(FinancialMetric.as_of_date <= end)
    stmt = stmt.order_by(desc(FinancialMetric.as_of_date)).limit(limit)
    rows = (await db.execute(stmt)).scalars().all()
    out = []
    for r in rows:
        d = _row_to_dict(r, _METRICS_COLUMNS)
        d["as_of_date"] = r.as_of_date.isoformat()
        d["period_end_date"] = r.period_end_date.isoformat() if r.period_end_date else None
        out.append(d)
    return out


async def get_latest_financial_metric(db: AsyncSession, stock_code: str) -> Optional[dict]:
    rows = await list_financial_metrics(db, stock_code, limit=1)
    return rows[0] if rows else None


# ── valuation_metrics ───────────────────────────────────────────────

_VALUATION_COLUMNS = ["per", "pbr", "pcr", "psr"]


async def list_valuation_metrics(
    db: AsyncSession, stock_code: str,
    start: Optional[datetime.date] = None, end: Optional[datetime.date] = None,
    limit: int = 50,
) -> list[dict]:
    stmt = select(ValuationMetric).where(ValuationMetric.stock_code == stock_code)
    if start:
        stmt = stmt.where(ValuationMetric.as_of_date >= start)
    if end:
        stmt = stmt.where(ValuationMetric.as_of_date <= end)
    stmt = stmt.order_by(desc(ValuationMetric.as_of_date)).limit(limit)
    rows = (await db.execute(stmt)).scalars().all()
    out = []
    for r in rows:
        d = _row_to_dict(r, _VALUATION_COLUMNS)
        d["as_of_date"] = r.as_of_date.isoformat()
        d["financial_as_of_date"] = r.financial_as_of_date.isoformat() if r.financial_as_of_date else None
        out.append(d)
    return out


async def get_latest_valuation_metric(db: AsyncSession, stock_code: str) -> Optional[dict]:
    rows = await list_valuation_metrics(db, stock_code, limit=1)
    return rows[0] if rows else None


# ── market_prices ───────────────────────────────────────────────────

_PRICE_COLUMNS = ["open_price", "high_price", "low_price", "close_price",
                  "volume", "trade_value", "market_cap", "listed_shares"]


async def list_market_prices(
    db: AsyncSession, stock_code: str,
    start: Optional[datetime.date] = None, end: Optional[datetime.date] = None,
    limit: int = 252,
) -> list[dict]:
    stmt = select(MarketPrice).where(MarketPrice.stock_code == stock_code)
    if start:
        stmt = stmt.where(MarketPrice.trade_date >= start)
    if end:
        stmt = stmt.where(MarketPrice.trade_date <= end)
    stmt = stmt.order_by(desc(MarketPrice.trade_date)).limit(limit)
    rows = (await db.execute(stmt)).scalars().all()
    out = []
    for r in rows:
        d = _row_to_dict(r, _PRICE_COLUMNS)
        d["trade_date"] = r.trade_date.isoformat()
        out.append(d)
    return out


async def get_latest_market_price(db: AsyncSession, stock_code: str) -> Optional[dict]:
    rows = await list_market_prices(db, stock_code, limit=1)
    return rows[0] if rows else None


# ── factor_scores ───────────────────────────────────────────────────

_FACTOR_COLUMNS = ["strategy_version", "eps_growth_annual_pct", "peg",
                   "value_score", "growth_score", "composite_score", "composite_rank",
                   "composite_top_pct"]


async def list_factor_scores(
    db: AsyncSession, stock_code: str,
    strategy_version: str = DEFAULT_STRATEGY_VERSION,
    start: Optional[datetime.date] = None, end: Optional[datetime.date] = None,
    limit: int = 50,
) -> list[dict]:
    stmt = select(FactorScore).where(
        FactorScore.stock_code == stock_code,
        FactorScore.strategy_version == strategy_version,
    )
    if start:
        stmt = stmt.where(FactorScore.as_of_date >= start)
    if end:
        stmt = stmt.where(FactorScore.as_of_date <= end)
    stmt = stmt.order_by(desc(FactorScore.as_of_date)).limit(limit)
    rows = (await db.execute(stmt)).scalars().all()
    out = []
    for r in rows:
        d = _row_to_dict(r, _FACTOR_COLUMNS)
        d["as_of_date"] = r.as_of_date.isoformat()
        out.append(d)
    return out


async def get_latest_factor_score(
    db: AsyncSession, stock_code: str, strategy_version: str = DEFAULT_STRATEGY_VERSION,
) -> Optional[dict]:
    rows = await list_factor_scores(db, stock_code, strategy_version=strategy_version, limit=1)
    return rows[0] if rows else None


# ── 종합 스냅샷 (질문분류 -> 정량 질문 라우팅의 기본 응답 단위) ────────

async def get_stock_snapshot(db: AsyncSession, stock_code: str) -> Optional[dict]:
    """한 종목의 최신 상태(기업정보+섹터+재무지표+밸류에이션+팩터스코어+시세)를
    한 번에 조회한다. '삼성전자 재무 어때?' 같은 단일 종목 정량 질문에 대응."""
    company = await get_company(db, stock_code)
    if company is None:
        return None
    return {
        "company": company,
        "sector": await get_latest_sector(db, stock_code),
        "financial_metrics": await get_latest_financial_metric(db, stock_code),
        "valuation_metrics": await get_latest_valuation_metric(db, stock_code),
        "factor_score": await get_latest_factor_score(db, stock_code),
        "latest_price": await get_latest_market_price(db, stock_code),
    }


# ── 스크리닝 (복합 조건 랭킹 질문) ───────────────────────────────────

async def screen_by_composite_score(
    db: AsyncSession,
    strategy_version: str = DEFAULT_STRATEGY_VERSION,
    as_of_date: Optional[datetime.date] = None,
    sector_wics_code: Optional[str] = None,
    max_per: Optional[float] = None,
    max_peg: Optional[float] = None,
    top_n: int = 20,
) -> dict:
    """composite_score 기준 상위 종목 스크리닝.
    as_of_date 미지정 시 factor_scores의 가장 최근 as_of_date를 자동 사용."""
    if as_of_date is None:
        as_of_date = (await db.execute(
            select(FactorScore.as_of_date)
            .where(FactorScore.strategy_version == strategy_version)
            .order_by(desc(FactorScore.as_of_date))
            .limit(1)
        )).scalar_one_or_none()
        if as_of_date is None:
            return {"as_of_date": None, "strategy_version": strategy_version, "items": []}

    stmt = (
        select(FactorScore, Company, ValuationMetric)
        .join(Company, Company.stock_code == FactorScore.stock_code)
        .outerjoin(
            ValuationMetric,
            (ValuationMetric.stock_code == FactorScore.stock_code)
            & (ValuationMetric.as_of_date == FactorScore.as_of_date),
        )
        .where(
            FactorScore.strategy_version == strategy_version,
            FactorScore.as_of_date == as_of_date,
        )
    )
    if sector_wics_code:
        codes = await _stock_codes_in_sector(db, sector_wics_code)
        if not codes:
            return {"as_of_date": as_of_date.isoformat(), "strategy_version": strategy_version, "items": []}
        stmt = stmt.where(FactorScore.stock_code.in_(codes))
    if max_peg is not None:
        stmt = stmt.where(FactorScore.peg.isnot(None), FactorScore.peg <= max_peg)
    if max_per is not None:
        stmt = stmt.where(ValuationMetric.per.isnot(None), ValuationMetric.per <= max_per)

    # composite_score가 NULL인 종목(계산 불가)은 랭킹에서 제외.
    # PostgreSQL은 DESC 정렬 시 기본적으로 NULL을 맨 앞에 두므로 nullslast로 명시.
    stmt = stmt.where(FactorScore.composite_score.isnot(None))
    stmt = stmt.order_by(nullslast(desc(FactorScore.composite_score))).limit(top_n)
    rows = (await db.execute(stmt)).all()

    items = []
    for factor, company, valuation in rows:
        items.append({
            "stock_code": company.stock_code,
            "company_name": company.company_name,
            "composite_score": _num(factor.composite_score),
            "composite_rank": factor.composite_rank,
            "peg": _num(factor.peg),
            "per": _num(valuation.per) if valuation else None,
            "pbr": _num(valuation.pbr) if valuation else None,
        })
    return {"as_of_date": as_of_date.isoformat(), "strategy_version": strategy_version, "items": items}
