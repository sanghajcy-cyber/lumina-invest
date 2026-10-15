"""screen_by_composite_score 의 factor_scores <-> valuation_metrics 날짜 짝짓기 (DB 없이 검증).

valuation_metrics 는 월초 일자, composite_v2 는 월말 일자다. 같은 as_of_date 로 조인하면 v2 의 매칭률이 0% 였다.
"""
from __future__ import annotations

import asyncio
import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from app.services import quant_financials as qf

D = datetime.date


@pytest.mark.parametrize("factor_date, expected", [
    (D(2026, 8, 1), D(2026, 8, 1)),     # v1: 월초는 그대로
    (D(2026, 7, 31), D(2026, 8, 1)),    # v2: 월말 -> 다음 달 1일
    (D(2025, 12, 31), D(2026, 1, 1)),   # 연말 -> 다음 해 1월 1일
    (D(2024, 2, 29), D(2024, 3, 1)),    # 윤년 2월 말
    (D(2023, 1, 31), D(2023, 2, 1)),
])
def test_valuation_as_of_for(factor_date, expected):
    assert qf.valuation_as_of_for(factor_date) == expected


def _run_screen(strategy_version, as_of):
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    out = asyncio.run(qf.screen_by_composite_score(db, strategy_version=strategy_version, as_of_date=as_of))
    compiled = db.execute.call_args.args[0].compile(dialect=postgresql.dialect())
    return out, str(compiled), list(compiled.params.values())


def test_screen_v2_joins_next_month_first_valuation():
    out, sql, params = _run_screen("composite_v2", D(2026, 7, 31))
    assert D(2026, 7, 31) in params and D(2026, 8, 1) in params
    assert "valuation_metrics.as_of_date = factor_scores.as_of_date" not in sql
    assert out["as_of_date"] == "2026-07-31" and out["valuation_as_of_date"] == "2026-08-01"
    assert out["strategy_version"] == "composite_v2"


def test_screen_v1_keeps_same_date_join():
    out, sql, params = _run_screen("composite_v1", D(2026, 8, 1))
    assert params.count(D(2026, 8, 1)) >= 2   # factor 필터 + valuation 조인 모두 같은 월초
    assert out["valuation_as_of_date"] == "2026-08-01"
