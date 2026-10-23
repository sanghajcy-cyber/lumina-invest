"""월초 ATR 리밸런싱 전략 3단계: 목표 파일 · ATR · 실효 투자금액 · 계획 행 · plan_*.csv · 활성화 · auto_trade 차단.

순수 함수(targets·planner)는 직접, service 는 가짜 DB·패치로, 라우트는 TestClient 로 검증한다.
"""
import asyncio
import io
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.database.postgres import get_pg_session
from app.lib.jwt_auth import get_current_user_any
from app.models import StrategyRun
from app.routes import atr_strategy as routes
from app.services import kis_market_data as kmd
from app.services import ta_utils as ta
from app.services.atr_strategy import planner, service as svc
from app.services.atr_strategy.targets import TargetFileError, parse_target_file

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
OTHER = uuid.UUID("11111111-2222-3333-4444-555555555555")

# 참고 구현 portfolio.csv / state/plan_20261016.csv (2026-10-16 dry-run, 투자금액 1,000만 원, 삼성전자 10주 보유)
REFERENCE_PORTFOLIO = "종목코드,종목명,비중,ATR\n005930,삼성전자,20,\n000660,SK하이닉스,20,\n005380,현대차,20,\n035420,NAVER,20,\n105560,KB금융,20,\n"
REFERENCE_PRICES = {"005930": 215_500, "000660": 1_273_000, "005380": 312_500, "035420": 179_900, "105560": 160_300}
REFERENCE_PLAN_CSV = """종목코드,종목명,비중,ATR,현재가,보유수량,주문가능수량,목표수량,매매수량,예상금액
005930,삼성전자,0.2,,215500,10,10,9,-1,-215500
000660,SK하이닉스,0.2,,1273000,0,0,1,1,1273000
005380,현대차,0.2,,312500,0,0,6,6,1875000
035420,NAVER,0.2,,179900,0,0,11,11,1978900
105560,KB금융,0.2,,160300,0,0,12,12,1923600
"""


# ── 목표 파일 ─────────────────────────────────────────────────────────────


def test_parse_reference_portfolio_csv():
    rows = parse_target_file(REFERENCE_PORTFOLIO.encode("utf-8-sig"), "portfolio.csv")
    assert [r["symbol"] for r in rows] == ["005930", "000660", "005380", "035420", "105560"]
    assert all(r["weight"] == pytest.approx(0.2) and r["atr_input"] is None for r in rows)
    assert rows[0]["name"] == "삼성전자" and rows[0]["weight_input"] == 20


def test_parse_normalizes_codes_weights_aliases_and_cp949():
    csv = "code,name,weight(%),atr\n5930,삼성전자,1,5200\n000660.KS,하이닉스,1,\n035720.0,카카오,2,\"1,250\"\n"
    csv = csv.replace("weight(%)", "비중(%)")
    rows = parse_target_file(csv.encode("cp949"), "p.csv")
    assert [r["symbol"] for r in rows] == ["005930", "000660", "035720"]
    assert [round(r["weight"], 4) for r in rows] == [0.25, 0.25, 0.5]
    assert [r["atr_input"] for r in rows] == [5200.0, None, 1250.0]


def test_parse_xlsx_and_equal_weight_when_no_weight_column():
    buf = io.BytesIO()
    pd.DataFrame({"종목코드": [5930, 660], "종목명": ["삼성전자", "SK하이닉스"]}).to_excel(buf, index=False)
    rows = parse_target_file(buf.getvalue(), "portfolio.xlsx")
    assert [r["symbol"] for r in rows] == ["005930", "000660"] and all(r["weight"] == 0.5 for r in rows)


@pytest.mark.parametrize("content,match", [
    ("종목명,비중\n삼성,1\n", "종목코드"),
    ("종목코드,비중\n005930,1\n005930,2\n", "중복"),
    ("종목코드,비중\nABC123,1\n", "형식"),
    ("종목코드,비중\n005930,1\n000660,\n", "비중"),
    ("종목코드,비중\n005930,-1\n", "비중"),
    ("종목코드,ATR\n005930,0\n", "ATR"),
    ("종목코드,ATR\n005930,abc\n", "ATR"),
])
def test_parse_rejects_bad_files(content, match):
    with pytest.raises(TargetFileError, match=match):
        parse_target_file(content.encode(), "p.csv")


def test_parse_enforces_max_tickers_default_20():
    body = "종목코드\n" + "".join(f"{i:06d}\n" for i in range(1, 22))
    with pytest.raises(TargetFileError, match="상한\\(20\\)"):
        parse_target_file(body.encode(), "p.csv")
    assert len(parse_target_file(body.encode(), "p.csv", max_tickers=21)) == 21


# ── ATR ──────────────────────────────────────────────────────────────────


def _daily(n, end: date, spike_today=False):
    out = []
    for i in range(n):
        d = end - timedelta(days=n - 1 - i)
        ts = int(datetime(d.year, d.month, d.day, tzinfo=KST).timestamp())
        out.append({"time": ts, "open": 100 + i, "high": 105 + i, "low": 95 + i, "close": 100 + i})
    if spike_today:
        out[-1].update(high=10_000, low=1)
    return out


def test_atr_from_daily_excludes_today_and_matches_wilder():
    today = date(2026, 11, 2)
    candles = _daily(40, today, spike_today=True)          # 마지막 봉 = 오늘(미완성, 비정상 고저)
    value, as_of = planner.atr_from_daily(candles, today, 14)
    assert as_of == today - timedelta(days=1)
    df = pd.DataFrame(candles[:-1])
    expected = ta.atr(df["high"].astype(float), df["low"].astype(float), df["close"].astype(float), 14, method="wilder").iloc[-1]
    assert value == pytest.approx(expected) and value < 50   # 오늘 봉(고저 9,999)이 섞이지 않았다


def test_atr_from_daily_needs_period_plus_one_completed_bars():
    today = date(2026, 11, 2)
    with pytest.raises(planner.AtrUnavailable):
        planner.atr_from_daily(_daily(15, today), today, 14)   # 오늘 제외 14개
    planner.atr_from_daily(_daily(16, today), today, 14)       # 오늘 제외 15개면 된다


# ── 투자금액 · 계획 행 ────────────────────────────────────────────────────


def test_effective_budget_caps_to_ledger_plus_cash_with_warning():
    assert planner.effective_budget(10_000_000, 0, 50_000_000) == (10_000_000, [])
    budget, warn = planner.effective_budget(10_000_000, 2_000_000, 3_000_000)
    assert budget == 5_000_000 and "5,000,000원으로 줄였습니다" in warn[0]


def test_slice_schedule_rounds():
    assert planner.SELL_ROUNDS == 65 and planner.BUY_ROUNDS == 70       # 09:10~14:30 / 09:10~14:55, 5분
    assert planner.rounds_for(3, "BUY") == 3 and planner.rounds_for(500, "BUY") == 70 and planner.rounds_for(-500, "SELL") == 65


def _build(targets, ledger=None, account=None, quotes=None, atrs=None, budget=10_000_000, **kw):
    params = dict(stop_atr_mult=2.0, stop_loss_pct=-10.0, buy_band_atr_mult=1.0, max_order_amount=5_000_000)
    params.update(kw)
    return planner.build_rows(targets, ledger or {}, account or {}, quotes or {}, atrs or {}, budget, **params)


def test_rows_reproduce_reference_plan_csv():
    """참고 구현 plan_20261016.csv 와 같은 입력(투자금액 1,000만, 삼성전자 10주)이면 앞 10개 컬럼이 그대로 같다."""
    targets = parse_target_file(REFERENCE_PORTFOLIO.encode(), "portfolio.csv")
    quotes = {s: {"price": p} for s, p in REFERENCE_PRICES.items()}
    ledger = {"005930": {"quantity": 10, "avg_price": 210_000, "name": "삼성전자"}}
    rows, _ = _build(targets, ledger=ledger, account={"005930": 10}, quotes=quotes,
                     atrs={s: {"atr": None, "source": "none"} for s in REFERENCE_PRICES})
    ours = planner.to_plan_csv(rows)
    assert ours.startswith("﻿")
    got = pd.read_csv(io.StringIO(ours.lstrip("﻿")), dtype=str).iloc[:, :10].fillna("")
    want = pd.read_csv(io.StringIO(REFERENCE_PLAN_CSV), dtype=str).fillna("")
    pd.testing.assert_frame_equal(got.sort_values("종목코드").reset_index(drop=True),
                                  want.sort_values("종목코드").reset_index(drop=True))
    assert list(got.columns) == list(want.columns)


def test_rows_sell_capped_by_ledger_and_account_and_excluded_ledger_symbol_full_sell():
    targets = [{"symbol": "005930", "name": "삼성전자", "weight": 0.1}]
    ledger = {"005930": {"quantity": 50, "avg_price": 70_000},
              "000660": {"quantity": 3, "avg_price": 100_000, "name": "하이닉스", "atr": 4_000.0, "atr_source": "kis"}}
    rows, _ = _build(targets, ledger=ledger, account={"005930": 20, "000660": 3, "035720": 9},     # 035720: 장부에 없는 직접 산 종목
                     quotes={"005930": {"price": 70_000}, "000660": {"price": 120_000}},
                     atrs={"005930": {"atr": 1_500.0, "source": "kis", "as_of": "2026-10-30"}}, budget=1_400_000)
    sam = next(r for r in rows if r["symbol"] == "005930")
    assert sam["target_qty"] == 2 and sam["sellable_qty"] == 20 and sam["trade_qty"] == -20   # 48주 팔아야 하지만 계좌엔 20주
    assert sam["stop_price"] == 70_000 - 2 * 1_500 and sam["stop_basis"] == "평균가 − ATR×2" and sam["excluded"] is False
    hy = next(r for r in rows if r["symbol"] == "000660")
    assert (hy["in_portfolio"], hy["excluded"], hy["target_qty"], hy["trade_qty"], hy["amount"], hy["rounds"]) == (False, True, 0, -3, -360_000, 3)
    assert hy["note"] == "목표 제외 → 전량 매도" and hy["stop_price"] == 100_000 - 2 * 4_000      # 매도 끝날 때까지 손절 기준
    assert "035720" not in {r["symbol"] for r in rows}                                         # 장부에 없는 종목은 다루지 않는다
    assert rows[0]["symbol"] == "005930"                                                       # 매도 먼저(큰 매도부터)


def test_excluded_row_capped_by_account_and_csv_marks_it():
    rows, _ = _build([], ledger={"000660": {"quantity": 5, "avg_price": 100_000, "name": "하이닉스"}}, account={"000660": 2},
                     quotes={"000660": {"price": 90_000}})
    assert rows[0]["trade_qty"] == -2 and "매도 가능 2주" in rows[0]["note"]
    csv_text = planner.to_plan_csv(rows).lstrip("\ufeff")
    df = pd.read_csv(io.StringIO(csv_text), dtype=str)
    assert df.loc[0, "목표수량"] == "0" and df.loc[0, "매매수량"] == "-2" and df.loc[0, "목표제외"] == "Y"
    assert df.loc[0, "비고"].startswith("목표 제외 → 전량 매도") and df.loc[0, "포트폴리오포함"] == "N"


def test_ledger_eval_includes_excluded_symbols_for_budget_cap():
    targets = [{"symbol": "005930"}]
    ledger = {"005930": {"quantity": 1, "avg_price": 100}, "000660": {"quantity": 2, "avg_price": 50}}
    assert planner.ledger_eval_for(targets, ledger, {"005930": {"price": 110}}) == 110 + 2 * 50   # 현재가 없으면 평균가


def test_rows_percent_stop_band_missing_price_and_order_cap_warning():
    targets = [{"symbol": "005930", "name": "A", "weight": 0.5}, {"symbol": "000660", "name": "B", "weight": 0.5}]
    rows, warn = _build(targets, ledger={"000660": {"quantity": 1, "avg_price": 100_000}},
                        quotes={"005930": {"error": "timeout"}, "000660": {"price": 100_000, "open": 101_000}},
                        atrs={"000660": {"atr": None, "source": "none"}}, budget=1_000_000_000, max_order_amount=1_000_000)
    a = next(r for r in rows if r["symbol"] == "005930")
    assert a["target_qty"] is None and a["trade_qty"] == 0 and "현재가 조회 실패" in a["note"]
    b = next(r for r in rows if r["symbol"] == "000660")
    assert b["stop_price"] == pytest.approx(90_000) and b["stop_basis"] == "평균가 -10%"
    assert b["band_low"] is None and "ATR 없음" in b["note"]                               # ATR 없으면 매수 범위 제한 없음
    assert any("현재가를 가져오지 못해" in w for w in warn) and any("1건 상한" in w for w in warn)
    rows2, _ = _build([targets[1]], quotes={"000660": {"price": 100_000, "open": 101_000}},
                      atrs={"000660": {"atr": 2_000.0, "source": "file"}})
    assert (rows2[0]["band_low"], rows2[0]["band_high"]) == (99_000, 103_000)


# ── service ──────────────────────────────────────────────────────────────


def test_fetch_market_uses_one_daily_call_per_symbol_and_file_atr_wins(monkeypatch):
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_BASE_URL", "https://sct.test")
    today = date(2026, 11, 2)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        sym = request.url.params["symbol"]
        calls.append(sym)
        if sym == "035720":
            return httpx.Response(502, json={"ok": False, "message": "KIS down"})
        candles = [{**c, "time": datetime.fromtimestamp(c["time"], KST).strftime("%Y-%m-%d")} for c in _daily(40, today)]
        n = 10 if sym == "000660" else 40
        return httpx.Response(200, json={"ok": True, "summary": {"price": 1000, "open": 990, "name": sym}, "candles": candles[-n:]})

    kmd.set_transport(httpx.MockTransport(handler))
    try:
        targets = [{"symbol": "005930", "atr_input": None}, {"symbol": "000660", "atr_input": None},
                   {"symbol": "035720", "atr_input": 777.0}, {"symbol": "105560", "atr_input": 1234.0}]
        quotes, atrs = asyncio.run(svc.fetch_market(targets, today, 14))
    finally:
        kmd.set_transport(None)
    assert calls == ["005930", "000660", "035720", "105560"]
    assert atrs["005930"]["source"] == "kis" and atrs["005930"]["as_of"] == "2026-11-01"
    assert atrs["000660"]["source"] == "none" and "15개 이상" in atrs["000660"]["error"]     # 일봉 부족 → % 손절 대체
    assert atrs["035720"] == {"atr": 777.0, "source": "file", "as_of": None} and "KIS down" in quotes["035720"]["error"]
    assert atrs["105560"]["source"] == "file" and quotes["105560"]["price"] == 1000


class FakeDb:
    def __init__(self, results=None):
        self.added, self.commits, self.results = [], 0, list(results or [])
    def add(self, obj): self.added.append(obj)
    async def flush(self): pass
    async def commit(self): self.commits += 1
    async def rollback(self): pass
    async def refresh(self, obj): pass
    async def execute(self, *_a, **_k):
        return self.results.pop(0) if self.results else One(None)


class One:
    def __init__(self, v): self.v = v
    def scalar_one_or_none(self): return self.v
    def scalars(self): return self
    def all(self): return self.v if isinstance(self.v, list) else []


def _plan(**over):
    base = dict(id=uuid.uuid4(), user_id=UID, invest_amount=10_000_000.0, max_tickers=20, atr_period=14, buy_band_atr_mult=1.0,
                stop_atr_mult=2.0, stop_loss_pct=-10.0, max_order_amount=5_000_000.0, max_orders_per_day=1000,
                target_file_name="portfolio.csv", is_active=False, dry_run=True, name="월초 ATR 리밸런싱", strategy="atr_rebal",
                environment="paper", target_source="file", targets_uploaded_at=None)
    base.update(over)
    return SimpleNamespace(**base)


def _patched_inputs(plan, targets, cash=495_331_590, account=None, ledger=None, quotes=None, atrs=None):
    return [
        patch.object(svc, "get_plan", AsyncMock(return_value=plan)),
        patch.object(svc, "list_targets", AsyncMock(return_value=[SimpleNamespace(**t) for t in targets])),
        patch.object(svc, "account_snapshot", AsyncMock(return_value=(cash, account or {}))),
        patch.object(svc, "load_ledger", AsyncMock(return_value=ledger or {})),
        patch.object(svc, "fetch_market", AsyncMock(return_value=(quotes or {}, atrs or {}))),
    ]


def _run_build(patches, db, kind="dry_run", today=date(2026, 10, 16)):
    for p in patches:
        p.start()
    try:
        return asyncio.run(svc.build_plan(db, UID, kind, today))
    finally:
        for p in patches:
            p.stop()


def test_build_plan_dry_run_records_run_with_budget_and_day_warnings():
    targets = [dict(t, weight_input=t["weight_input"]) for t in parse_target_file(REFERENCE_PORTFOLIO.encode(), "p.csv")]
    quotes = {s: {"price": p} for s, p in REFERENCE_PRICES.items()}
    db = FakeDb()
    run = _run_build(_patched_inputs(_plan(invest_amount=600_000_000.0), targets, quotes=quotes,
                                     atrs={s: {"atr": None, "source": "none"} for s in quotes}), db)
    assert isinstance(run, StrategyRun) and db.added == [run] and run.kind == "dry_run" and run.status == "planned"
    assert run.effective_budget == pytest.approx(495_331_590) and run.invest_amount == 600_000_000
    assert any("줄였습니다" in w for w in run.warnings)
    assert any("리밸런싱 날이 아닙니다" in w and "2026-10-01" in w for w in run.warnings)      # 10/16 은 첫 거래일 아님
    assert any(w.startswith("ATR 없음") for w in run.warnings)
    assert run.summary["first_trading_day"] == "2026-10-01" and run.summary["is_rebalance_day"] is False
    assert run.summary["buy_symbols"] == 5 and len(run.rows) == 5


def test_build_plan_requires_targets_and_refuses_unknown_holiday_year_for_rebalance(monkeypatch):
    monkeypatch.setattr(settings, "KRX_EXTRA_HOLIDAYS", "", raising=False)
    with pytest.raises(svc.StrategyError, match="비어 있습니다"):
        _run_build(_patched_inputs(_plan(), []), FakeDb())
    targets = [{"symbol": "005930", "name": "A", "weight": 1.0, "weight_input": None, "atr_input": None}]
    with pytest.raises(svc.StrategyError, match="2027년 KRX 휴장일"):
        _run_build(_patched_inputs(_plan(), targets), FakeDb(), kind="rebalance", today=date(2027, 1, 4))
    run = _run_build(_patched_inputs(_plan(), targets, quotes={"005930": {"price": 1000}}), FakeDb(), today=date(2027, 1, 4))
    assert any("2027년 KRX 휴장일" in w for w in run.warnings)                              # dry-run 은 경고만


def test_account_snapshot_requires_gateway_and_sums_holdings(monkeypatch):
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_BASE_URL", "")
    with pytest.raises(svc.StrategyError, match="게이트웨이"):
        asyncio.run(svc.account_snapshot())
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_BASE_URL", "https://sct.test")
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_API_KEY", "key")
    bal = {"cashBalance": 1000, "holdings": [{"symbol": "005930", "quantity": 3}, {"symbol": "BAD", "quantity": 1},
                                             {"symbol": "000660", "quantity": 0}]}
    with patch.object(svc.gateway, "get_balance", AsyncMock(return_value=bal)):
        assert asyncio.run(svc.account_snapshot()) == (1000.0, {"005930": 3})


def test_apply_plan_update_validates_ranges():
    p = _plan()
    svc.apply_plan_update(p, {"invest_amount": 5_000_000, "stop_loss_pct": -7, "max_orders_per_day": 300})
    assert (p.invest_amount, p.stop_loss_pct, p.max_orders_per_day) == (5_000_000, -7, 300)
    for bad in ({"max_tickers": 21}, {"stop_loss_pct": 5}, {"max_order_amount": 20_000_000}, {"invest_amount": "x"}):
        with pytest.raises(svc.StrategyError):
            svc.apply_plan_update(p, bad)


def _activate(db, plan, targets=True, **kw):
    with patch.object(svc, "get_plan", AsyncMock(return_value=plan)), \
         patch.object(svc, "list_targets", AsyncMock(return_value=[object()] if targets else [])), \
         patch.object(svc, "audit", AsyncMock()), \
         patch.object(svc, "force_off_live_kis_auto_trade", AsyncMock(return_value=["u1"])) as force:
        out = asyncio.run(svc.set_active(db, UID, True, **kw))
    return out, force


def test_deactivate_live_plan_warns_about_unwatched_ledger_and_open_orders():
    plan = _plan(is_active=True, dry_run=False)
    open_orders = [object(), object()]
    with patch.object(svc, "get_plan", AsyncMock(return_value=plan)), \
         patch.object(svc, "load_ledger", AsyncMock(return_value={"005930": {"quantity": 3, "name": "삼성전자"}})), \
         patch.object(svc, "audit", AsyncMock()):
        out = asyncio.run(svc.set_active(FakeDb([One(open_orders)]), UID, False))
    assert plan.is_active is False and len(out["warnings"]) == 2
    assert "손절 감시 없이 남습니다" in out["warnings"][0] and "005930 삼성전자 3주" in out["warnings"][0]
    assert "열린 전략 주문 2건" in out["warnings"][1]
    plan2 = _plan(is_active=True, dry_run=True)                                       # dry-run 플랜은 실제 주문이 없으니 경고 없음
    with patch.object(svc, "get_plan", AsyncMock(return_value=plan2)), patch.object(svc, "audit", AsyncMock()):
        assert asyncio.run(svc.set_active(FakeDb(), UID, False))["warnings"] == []


def test_activate_dry_run_does_not_touch_auto_trade():
    plan = _plan()
    out, force = _activate(FakeDb(), plan, dry_run=True)
    assert plan.is_active is True and plan.dry_run is True and out["stopped_auto_trade"] == []
    force.assert_not_awaited()


def test_activate_live_refused_until_step4_then_forces_auto_trade_off(monkeypatch):
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", False)
    with pytest.raises(svc.StrategyError, match="승인되지 않았습니다"):
        _activate(FakeDb(), _plan(), dry_run=False)
    with pytest.raises(svc.StrategyError, match="비어 있어"):
        _activate(FakeDb(), _plan(), targets=False, dry_run=True)
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", True)
    with pytest.raises(svc.StrategyError, match="다른 사용자"):
        _activate(FakeDb([One(_plan(user_id=OTHER, is_active=True, dry_run=False))]), _plan(), dry_run=False)
    plan = _plan()
    out, force = _activate(FakeDb([One(None)]), plan, dry_run=False)
    assert plan.dry_run is False and out["stopped_auto_trade"] == ["u1"]
    force.assert_awaited_once()


def test_block_reason_and_force_off_only_live_kis_rows():
    assert asyncio.run(svc.auto_trade_block_reason(FakeDb([One(None)]))) is None
    live_plan = _plan(is_active=True, dry_run=False)
    assert "실행 중" in asyncio.run(svc.auto_trade_block_reason(FakeDb([One(live_plan)])))
    rows = [SimpleNamespace(user_id=UID, broker="kis", quant_mode="live", quant_auto_enabled=True),
            SimpleNamespace(user_id=OTHER, broker="kis", quant_mode="paper", quant_auto_enabled=True),
            SimpleNamespace(user_id=uuid.uuid4(), broker="mock", quant_mode="live", quant_auto_enabled=True)]
    db = FakeDb([One(rows)])
    with patch.object(svc, "audit", AsyncMock()) as aud:
        stopped = asyncio.run(svc.force_off_live_kis_auto_trade(db, "test"))
    assert stopped == [str(UID)] and [r.quant_auto_enabled for r in rows] == [False, True, True] and db.commits == 1
    aud.assert_awaited_once()


# ── auto_trade · KIS 배치 차단 ────────────────────────────────────────────


def test_start_auto_trade_blocked_for_live_kis_but_not_paper():
    from app.services import auto_trade
    live_row = SimpleNamespace(broker="kis", quant_mode="live")
    with patch.object(auto_trade, "is_enabled", AsyncMock(return_value=False)), \
         patch.object(svc, "auto_trade_block_reason", AsyncMock(return_value="ATR 전략 실행 중")), \
         patch.object(auto_trade, "set_enabled", AsyncMock()) as set_enabled:
        with pytest.raises(auto_trade.AutoTradeBlocked, match="ATR 전략"):
            asyncio.run(auto_trade.start_auto_trade(FakeDb([One(live_row)]), str(UID)))
        set_enabled.assert_not_awaited()
        with patch.object(auto_trade, "_run_once", AsyncMock()), \
             patch.object(auto_trade.notification, "notify_auto_trade_started", AsyncMock()), \
             patch.object(auto_trade, "audit", AsyncMock()):
            async def go():
                return await auto_trade.start_auto_trade(FakeDb([One(SimpleNamespace(broker="kis", quant_mode="paper"))]), str(UID))
            assert asyncio.run(go()) is True
        set_enabled.assert_awaited_once()


def test_run_cycle_forces_off_and_keeps_batch_off_when_blocked(monkeypatch):
    from app.services import auto_trade, kis_batch

    class Db:
        async def execute(self, *_a, **_k): return One([])
        async def rollback(self): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(auto_trade, "get_session_factory", lambda: (lambda: Db()))
    ensure = AsyncMock(return_value={"running": False, "reason": "atr_strategy_active"})
    with patch.object(svc, "auto_trade_block_reason", AsyncMock(return_value="ATR 전략 실행 중")), \
         patch.object(svc, "force_off_live_kis_auto_trade", AsyncMock(return_value=[])) as force, \
         patch.object(kis_batch, "ensure_system_batch", ensure):
        out = asyncio.run(auto_trade.run_cycle_for_enabled_users())
    force.assert_awaited_once()
    assert ensure.await_args.kwargs["blocked_reason"] == "ATR 전략 실행 중"
    assert out["blocked_by_strategy"] == "ATR 전략 실행 중" and out["ran"] == 0


def test_kis_batch_stays_off_when_blocked():
    from app.services import kis_batch
    row = SimpleNamespace(user_id=uuid.uuid4(), broker="kis", quant_mode="live", quant_auto_enabled=True)
    db = FakeDb()
    with patch.object(kis_batch, "_row", AsyncMock(return_value=row)), patch.object(kis_batch, "audit", AsyncMock()) as aud:
        out = asyncio.run(kis_batch.ensure_system_batch(db, blocked_reason="ATR 전략 실행 중"))
    assert row.quant_auto_enabled is False and out["reason"] == "atr_strategy_active" and out["action"] == "disabled"
    aud.assert_awaited_once()


# ── 라우트 ───────────────────────────────────────────────────────────────


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(routes.router)
    db = FakeDb()
    app.dependency_overrides[get_current_user_any] = lambda: {"id": str(UID)}
    app.dependency_overrides[get_pg_session] = lambda: db
    return TestClient(app), db


def test_upload_targets_route_ok_and_bad_file(client):
    c, _ = client
    plan = _plan()
    with patch.object(svc, "get_plan", AsyncMock(return_value=plan)), \
         patch.object(svc, "replace_targets", AsyncMock(side_effect=[[{"symbol": "005930"}], svc.StrategyError("중복된 종목코드")])):
        r = c.post("/api/strategy/atr/targets", files={"file": ("portfolio.csv", REFERENCE_PORTFOLIO.encode(), "text/csv")})
        assert r.status_code == 200 and r.json()["count"] == 1
        r = c.post("/api/strategy/atr/targets", files={"file": ("p.csv", b"x", "text/csv")})
        assert r.status_code == 400 and "중복" in r.json()["detail"]
    r = c.post("/api/strategy/atr/targets", files={"file": ("big.csv", b"0" * (routes.MAX_UPLOAD_BYTES + 1), "text/csv")})
    assert r.status_code == 413


def test_dry_run_route_and_plan_csv_download(client):
    c, _ = client
    run = StrategyRun(id=uuid.uuid4(), plan_id=uuid.uuid4(), user_id=UID, run_date=date(2026, 11, 2), kind="dry_run", status="planned",
                      invest_amount=1.0, effective_budget=1.0, cash_balance=1.0, ledger_eval=0.0, warnings=[], summary={}, note="",
                      rows=[{"symbol": "005930", "name": "삼성전자", "weight": 1.0, "price": 70000.0, "ledger_qty": 0, "sellable_qty": 0,
                             "target_qty": 1, "trade_qty": 1, "amount": 70000.0, "in_portfolio": True}])
    with patch.object(svc, "build_plan", AsyncMock(return_value=run)):
        r = c.post("/api/strategy/atr/dry-run")
    assert r.status_code == 200 and r.json()["rows"][0]["trade_qty"] == 1
    with patch.object(svc, "build_plan", AsyncMock(side_effect=svc.StrategyError("게이트웨이 미설정"))):
        assert c.post("/api/strategy/atr/dry-run").status_code == 409
    with patch.object(svc, "get_run", AsyncMock(return_value=run)):
        r = c.get(f"/api/strategy/atr/runs/{run.id}/plan.csv")
    assert r.status_code == 200 and 'filename="plan_20261102_dry_run.csv"' in r.headers["content-disposition"]
    assert r.text.lstrip("﻿").splitlines()[0].startswith("종목코드,종목명,비중,ATR,현재가,보유수량,주문가능수량,목표수량,매매수량,예상금액,")
    assert c.get("/api/strategy/atr/runs/not-a-uuid").status_code == 400
