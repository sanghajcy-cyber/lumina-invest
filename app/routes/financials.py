"""AI_Quant 정량 데이터 조회 API: PostgreSQL 7개 테이블(companies ~ factor_scores) 기반.

질문분류(FastAPI) → 정량 질문 → 이 라우터의 엔드포인트를 거쳐 조회된 결과가
Ollama 자연어 답변 생성 단계의 입력으로 쓰인다.
"""
from __future__ import annotations

import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.postgres import get_pg_session
from app.lib.session import get_current_user
from app.services import quant_financials as svc

router = APIRouter(prefix="/api/quant/financials")


def _parse_date(value: str | None, field: str) -> datetime.date | None:
    if value is None:
        return None
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        raise HTTPException(422, f"{field}는 YYYY-MM-DD 형식이어야 합니다: {value}")


@router.get("/{stock_code}/snapshot")
async def stock_snapshot(
    stock_code: str,
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """종목 1건의 최신 정량 데이터 종합 스냅샷(기업정보/섹터/재무지표/밸류에이션/팩터스코어/시세)."""
    result = await svc.get_stock_snapshot(db, stock_code)
    if result is None:
        raise HTTPException(404, f"companies에 없는 종목코드: {stock_code}")
    return result


@router.get("/{stock_code}/metrics")
async def stock_financial_metrics(
    stock_code: str,
    start: str | None = Query(None, description="YYYY-MM-DD"),
    end: str | None = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=500),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """재무지표(financial_metrics) 시계열. as_of_date 내림차순."""
    rows = await svc.list_financial_metrics(
        db, stock_code, start=_parse_date(start, "start"), end=_parse_date(end, "end"), limit=limit,
    )
    return {"stock_code": stock_code, "items": rows}


@router.get("/{stock_code}/valuation")
async def stock_valuation_metrics(
    stock_code: str,
    start: str | None = Query(None, description="YYYY-MM-DD"),
    end: str | None = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=500),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """밸류에이션(valuation_metrics: PER/PBR/PCR/PSR) 시계열."""
    rows = await svc.list_valuation_metrics(
        db, stock_code, start=_parse_date(start, "start"), end=_parse_date(end, "end"), limit=limit,
    )
    return {"stock_code": stock_code, "items": rows}


@router.get("/{stock_code}/prices")
async def stock_market_prices(
    stock_code: str,
    start: str | None = Query(None, description="YYYY-MM-DD"),
    end: str | None = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(252, ge=1, le=3000),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """KRX 일별 시세(market_prices) 시계열."""
    rows = await svc.list_market_prices(
        db, stock_code, start=_parse_date(start, "start"), end=_parse_date(end, "end"), limit=limit,
    )
    return {"stock_code": stock_code, "items": rows}


@router.get("/{stock_code}/scores")
async def stock_factor_scores(
    stock_code: str,
    strategy_version: str = Query(svc.DEFAULT_STRATEGY_VERSION),
    start: str | None = Query(None, description="YYYY-MM-DD"),
    end: str | None = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(50, ge=1, le=500),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """Value/Growth/PEG/Composite 팩터 스코어(factor_scores) 시계열."""
    rows = await svc.list_factor_scores(
        db, stock_code, strategy_version=strategy_version,
        start=_parse_date(start, "start"), end=_parse_date(end, "end"), limit=limit,
    )
    return {"stock_code": stock_code, "items": rows}


@router.get("/screen")
async def screen_stocks(
    strategy_version: str = Query(svc.DEFAULT_STRATEGY_VERSION),
    as_of_date: str | None = Query(None, description="YYYY-MM-DD, 미지정 시 최신"),
    sector_wics_code: str | None = Query(None),
    max_per: float | None = Query(None, ge=0),
    max_peg: float | None = Query(None, ge=0),
    top_n: int = Query(20, ge=1, le=200),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_pg_session),
):
    """composite_score 기준 상위 종목 스크리닝 (선택적 섹터/PER/PEG 필터)."""
    return await svc.screen_by_composite_score(
        db,
        strategy_version=strategy_version,
        as_of_date=_parse_date(as_of_date, "as_of_date"),
        sector_wics_code=sector_wics_code,
        max_per=max_per,
        max_peg=max_peg,
        top_n=top_n,
    )
