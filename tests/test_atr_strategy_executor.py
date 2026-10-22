"""월초 ATR 리밸런싱 4단계: 시분할 회차 실행 · 이어받기 · 수동 시작/재실행/중지 · 15:00 취소 · 15:20 확정.

DB 는 executor.Repo 와 같은 메서드를 가진 메모리 저장소, 게이트웨이·시세·주문 경로는 가짜로 바꿔 끼운다.
"""
import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.database.postgres import get_pg_session
from app.lib.jwt_auth import get_current_user_any
from app.models import LiveOrder, StrategyPosition, StrategyRun, StrategySlice
from app.routes import atr_strategy as routes
from app.services import auto_trade, kis_market_data as kmd, risk_guard
from app.services.atr_strategy import executor as ex
from app.services.atr_strategy import planner, service as svc
from app.services.brokers import stock_coin_trade_gateway as gw
from tests.atr_strategy_fakes import MemRepo, FakeBroker, _plan, _row, _fields, env  # noqa: F401  (env: fixture)

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
DAY = date(2026, 11, 2)           # 11월 첫 거래일(월)
MID = date(2026, 11, 10)          # 첫 거래일 아님


def at(hh, mm, ss=0, day=DAY):
    return datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=KST)


# ── 순수 함수 ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("hh,mm,side,want", [
    (9, 9, "BUY", None), (9, 10, "BUY", (0, 70)), (9, 14, "BUY", (0, 70)), (9, 15, "BUY", (1, 69)),
    (14, 55, "BUY", (69, 1)), (14, 59, "BUY", (69, 1)), (15, 0, "BUY", None),
    (9, 10, "SELL", (0, 65)), (14, 25, "SELL", (63, 2)), (14, 29, "SELL", (63, 2)),
    (14, 30, "SELL", (64, 1)), (14, 31, "SELL", None),                   # 매도는 14:30 까지만
])
def test_current_round(hh, mm, side, want):
    assert ex.current_round(at(hh, mm), side) == want


def test_slice_quantity_rule_and_order_cap():
    assert ex.slice_quantity(10, 70, 50_000, 5_000_000) == (1, "")        # min(70,10)=10회 → 회당 1주
    assert ex.slice_quantity(150, 70, 10_000, 5_000_000) == (3, "")       # ceil(150/70)=3
    assert ex.slice_quantity(150, 2, 10_000, 5_000_000) == (75, "")       # 남은 회차가 적으면 크게
    assert ex.slice_quantity(150, 2, 100_000, 5_000_000) == (50, "")      # 1건 상한 500만 ÷ 10만 = 50주
    qty, why = ex.slice_quantity(1, 1, 6_000_000, 5_000_000)
    assert qty == 0 and "1건 상한" in why


@pytest.mark.parametrize("price,side,want", [
    (70_000, "BUY", 70_100), (70_000, "SELL", 69_900), (19_990, "BUY", 20_000), (20_000, "SELL", 19_990),
    (1_999, "BUY", 2_000), (500_000, "SELL", 499_500), (70_050.0, "BUY", 70_200),
])
def test_limit_price_one_tick(price, side, want):
    assert ex.limit_price(price, side) == want


# ── 메모리 저장소 · 가짜 게이트웨이 ─────────────────────────────────────────


def run(coro): return asyncio.run(coro)


def start(e, now=at(9, 5), manual=True, force=False):
    return run(ex.start_rebalance(e.repo.db, UID, manual=manual, force=force, now=now, repo=e.repo))


def tick(e, now): return run(ex.tick_plan(e.repo, e.plan, now))


# ── 시작 · 이어받기 · 재실행 ──────────────────────────────────────────────


def test_start_requires_approval_live_mode_and_first_day(env, monkeypatch):
    e = env([_row("005930", 10, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", False)
    with pytest.raises(svc.StrategyError, match="승인"):
        start(e)
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", True)
    e.plan.dry_run = True
    with pytest.raises(svc.StrategyError, match="dry_run=false"):
        start(e)
    e.plan.dry_run = False
    with pytest.raises(svc.StrategyError, match="첫 거래일이 아닙니다"):
        start(e, now=at(9, 5, day=MID), manual=False)
    with pytest.raises(svc.StrategyError, match="거래일이 아닙니다"):
        start(e, now=at(9, 5, day=date(2026, 11, 7)))                      # 토요일
    with pytest.raises(svc.StrategyError, match="남은 매매 회차"):
        start(e, now=at(14, 56))
    e.repo.broker = SimpleNamespace(risk_kill_switch=True, risk_halt_reason="테스트")
    with pytest.raises(svc.StrategyError, match="비상 정지"):
        start(e)


def test_manual_start_mid_month_freezes_atr_and_resume_returns_same_run(env):
    e = env([_row("005930", 10, 50_000, atr=1_200.0)], {"005930": {"price": 50_000, "open": 50_000}})
    r1 = start(e, now=at(9, 5, day=MID))
    assert r1.status == "running" and r1.trigger == "manual" and r1.attempt == 0 and r1.run_date == MID
    pos = e.repo.positions_[0]
    assert (pos.symbol, pos.quantity, pos.atr, pos.atr_source, pos.atr_as_of) == ("005930", 0, 1_200.0, "kis", date(2026, 10, 30))
    r2 = start(e, now=at(10, 0, day=MID))
    assert r2 is r1 and e.compute.await_count == 1 and len(e.repo.runs) == 1            # 이어받기: 다시 계산하지 않는다


def test_force_restart_cancels_open_orders_and_replans_from_ledger(env):
    e = env([_row("005930", 10, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    r = start(e)
    tick(e, at(9, 10))
    first = e.fb.placed[0]
    e.fb.fill(e.repo.orders[0].order_no, 1)                                            # 1주 체결
    tick(e, at(9, 15))                                                                  # 체결 반영
    assert e.repo.positions_[0].quantity == 1
    open_before = [o for o in e.repo.orders if o.status in ex.LIVE_ORDER_OPEN_STATUSES]
    r2 = start(e, now=at(9, 17), force=True)
    assert r2 is r and r2.attempt == 1 and e.compute.await_count == 2
    assert all(o.status not in ex.LIVE_ORDER_OPEN_STATUSES for o in open_before)       # 열린 주문 취소
    tick(e, at(9, 20))
    assert e.fb.placed[-1][4].split(":")[4] == "ATR1" and e.fb.placed[-1][4] != first[4]   # 새 attempt 태그


def test_auto_start_only_on_first_trading_day_inside_window(env):
    e = env([_row("005930", 10, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    assert tick(e, at(9, 10, day=MID)) == {"skipped": "no_run"}
    assert tick(e, at(9, 9)) == {"skipped": "no_run"}
    out = tick(e, at(9, 10))
    assert e.repo.runs[0].trigger == "auto" and out["processed"][0]["status"] == "submitted"


# ── 회차 처리 ───────────────────────────────────────────────────────────


def test_round_places_limit_plus_one_tick_and_does_not_repeat_within_round(env):
    e = env([_row("005930", 10, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    start(e)
    out = tick(e, at(9, 10))
    assert out["processed"] == [{"symbol": "005930", "side": "BUY", "seq": 0, "status": "submitted", "qty": 1, "reason": ""}]
    sym, side, qty, price, cid = e.fb.placed[0]
    assert (side, qty, price) == ("BUY", 1, 50_100) and cid.endswith(":ATR0:0")
    assert tick(e, at(9, 12))["processed"] == []                                         # 같은 회차 재처리 없음
    sl = e.repo.slices_[0]
    assert sl.live_order_id == e.repo.orders[0].id and sl.client_order_id == cid


def test_unfilled_order_is_cancelled_then_reordered_and_fills_go_to_ledger(env):
    e = env([_row("005930", 3, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    start(e)
    tick(e, at(9, 10))
    o1 = e.repo.orders[0]
    tick(e, at(9, 15))                                                                  # 미체결 → 취소 → 재주문
    assert o1.status == "CANCELLED" and o1.order_no in e.fb.cancelled and len(e.fb.placed) == 2
    o2 = e.repo.orders[1]
    e.fb.fill(o2.order_no, 1)
    tick(e, at(9, 20))
    pos = e.repo.positions_[0]
    assert pos.quantity == 1 and pos.avg_price == 50_100                               # 장부 = 체결 수량·LIMIT 가
    e.fb.fill(e.repo.orders[2].order_no, 1)
    tick(e, at(9, 25))
    e.fb.fill(e.repo.orders[3].order_no, 1)
    tick(e, at(9, 30))
    assert e.repo.positions_[0].quantity == 3
    assert tick(e, at(9, 35))["processed"] == []                                        # 목표 도달 → 더 내지 않는다


def test_cancel_not_confirmed_skips_round(env):
    e = env([_row("005930", 5, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    start(e)
    tick(e, at(9, 10))
    e.fb.cancel_sticks.add(e.repo.orders[0].order_no)
    out = tick(e, at(9, 15))
    assert out["processed"][0]["status"] == "skipped" and "취소 확인 대기" in out["processed"][0]["reason"]
    assert len(e.fb.placed) == 1


def test_partial_fill_then_cancel_counts_filled_part(env):
    e = env([_row("005930", 4, 50_000)], {"005930": {"price": 50_000, "open": 50_000}})
    start(e)
    tick(e, at(14, 50))                                                                 # 남은 회차 2 → 회당 2주
    o1 = e.repo.orders[0]
    assert o1.quantity == 2
    o1.filled_quantity, o1.status = 1, "PARTIALLY_FILLED"
    tick(e, at(14, 55))                                                                 # 부분체결분 반영 후 남은 3주 한 번에
    assert e.repo.positions_[0].quantity == 1 and e.repo.orders[1].quantity == 3
    assert e.repo.slices_[0].status == "filled"


def test_buy_band_skip_and_atr_missing_means_no_band(env):
    e = env([_row("005930", 5, 100_000, atr=1_000.0), _row("000660", 5, 100_000)],
            {"005930": {"price": 102_000, "open": 100_000}, "000660": {"price": 150_000, "open": 100_000}})
    start(e)
    out = {p["symbol"]: p for p in tick(e, at(9, 10))["processed"]}
    assert out["005930"]["status"] == "skipped" and "매수 범위 밖" in out["005930"]["reason"]
    assert out["000660"]["status"] == "submitted"                                      # ATR 없음 → 범위 제한 없음
    row = next(r for r in e.repo.runs[0].rows if r["symbol"] == "005930")
    assert (row["open"], row["band_low"], row["band_high"], row["band_skips"]) == (100_000, 99_000, 101_000, 1)
    e.fb.quotes["005930"]["price"] = 100_500
    out = {p["symbol"]: p for p in tick(e, at(9, 15))["processed"]}
    assert out["005930"]["status"] == "submitted"                                      # 다음 회차에 범위 안이면 산다


def test_open_price_missing_skips_buy(env):
    e = env([_row("005930", 5, 50_000)], {"005930": {"price": 50_000, "open": 0}})
    start(e)
    assert "시가 없음" in tick(e, at(9, 10))["processed"][0]["reason"]


def test_sells_first_capped_by_account_and_spread_over_ticks(env):
    rows = [_row(f"{i:06d}", 0 if i <= 4 else 5, 10_000) for i in range(1, 9)]          # 4종목 매도, 4종목 매수
    quotes = {r["symbol"]: {"price": 10_000, "open": 10_000} for r in rows}
    bal = {"cashBalance": 1e8, "totalEvalAmount": 1e8, "holdings": [{"symbol": f"{i:06d}", "quantity": 3} for i in range(1, 5)]}
    e = env(rows, quotes, balance=bal)
    start(e)
    for i in range(1, 5):
        e.repo.add(StrategyPosition(plan_id=e.plan.id, symbol=f"{i:06d}", name="", quantity=10, avg_price=9_000.0, atr_source="none"))
    out = tick(e, at(9, 10))
    sides = [p["side"] for p in out["processed"]]
    assert sides == ["SELL"] * 4 + ["BUY"] * 2 and out["deferred"] == 2               # 매도 먼저, tick 당 6종목
    sell = next(p for p in e.fb.placed if p[1] == "SELL")
    assert sell[2] == 1 and sell[3] == 9_990                                           # 계좌 3주 한도, min(65,3)=3회 → 1주, 현재가 −1틱
    out2 = tick(e, at(9, 11))
    assert [p["side"] for p in out2["processed"]] == ["BUY", "BUY"] and out2["deferred"] == 0


def test_daily_loss_blocks_buys_not_sells_and_cooldown_is_clamped(env):
    rows = [_row("000001", 0, 10_000), _row("000002", 5, 10_000)]
    quotes = {r["symbol"]: {"price": 10_000, "open": 10_000} for r in rows}
    bal = {"cashBalance": 1e8, "totalEvalAmount": 96_000_000, "holdings": [{"symbol": "000001", "quantity": 5}]}
    broker = SimpleNamespace(risk_kill_switch=False, risk_daily_loss_limit_pct=3.0, risk_max_position_pct=30.0,
                             risk_max_orders_per_day=20, risk_cooldown_min=30)
    e = env(rows, quotes, broker_row=broker, balance=bal)
    start(e)
    e.repo.add(StrategyPosition(plan_id=e.plan.id, symbol="000001", name="", quantity=5, avg_price=10_000.0, atr_source="none"))
    run(risk_guard.day_start_equity(f"{UID}:live:paper", 100_000_000))                 # 당일 시작 1억 → 지금 9,600만(−4%)
    out = {p["symbol"]: p for p in tick(e, at(9, 10))["processed"]}
    assert out["000001"]["status"] == "submitted"                                      # 매도는 막지 않는다
    assert out["000002"]["status"] == "skipped" and "일손실 한도" in out["000002"]["reason"]
    assert ex.buy_cooldown_min(broker) == 4                                             # 30분 → 회차 간격 − 1분


def test_strategy_daily_order_limit(env):
    e = env([_row("005930", 50, 10_000)], {"005930": {"price": 10_000, "open": 10_000}}, plan=_plan(max_orders_per_day=1))
    start(e)
    tick(e, at(9, 10))
    out = tick(e, at(9, 15))
    assert "전략 일 주문 수 한도 1" in out["processed"][0]["reason"] and len(e.fb.placed) == 1


def test_halted_symbol_is_skipped(env):
    e = env([_row("005930", 5, 10_000, halted=True)], {"005930": {"price": 10_000, "open": 10_000}})
    start(e)
    assert tick(e, at(9, 10))["processed"] == []


def test_restart_links_or_closes_interrupted_planned_slices(env):
    e = env([_row("005930", 5, 10_000), _row("000660", 5, 10_000)], {s: {"price": 10_000, "open": 10_000} for s in ("005930", "000660")})
    r = start(e)
    a = StrategySlice(run_id=r.id, plan_id=e.plan.id, symbol="005930", side="BUY", seq=0, attempt=0, status="planned", client_order_id="cid-a")
    b = StrategySlice(run_id=r.id, plan_id=e.plan.id, symbol="000660", side="BUY", seq=0, attempt=0, status="planned", client_order_id="cid-b")
    e.repo.add(a); e.repo.add(b)
    e.repo.add(LiveOrder(user_id=UID, client_order_id="cid-a", environment="paper", symbol="005930", side="BUY", quantity=1,
                         price=10_010.0, order_no="0000000099", status="FILLED", filled_quantity=1, avg_filled_price=10_010.0, source="ATR_REBAL"))
    out = tick(e, at(9, 12))
    assert a.status == "filled" and b.status == "failed" and "중단" in b.reason
    assert out["processed"] == []                                                       # 같은 회차는 다시 내지 않는다
    assert e.repo.positions_[0].quantity == 1


# ── 15:00 취소 · 15:20 확정 · 중지 ────────────────────────────────────────


def test_cancel_all_at_15_and_finalize_at_1520(env):
    e = env([_row("005930", 5, 10_000)], {"005930": {"price": 10_000, "open": 10_000}})
    r = start(e)
    tick(e, at(14, 55))
    e.fb.fill(e.repo.orders[0].order_no, 2)
    e.repo.orders[0].status = "PARTIALLY_FILLED"
    out = tick(e, at(15, 0))
    assert out["cancel_all"] == {"closed": 1, "still_open": 0} and r.status == "running" and "cancelled_at" in r.summary
    assert tick(e, at(15, 1)).get("cancel_all") is None                                 # 한 번만
    out = tick(e, at(15, 20))
    assert out["finalized"] and r.status == "done" and r.finished_at is not None
    assert r.summary["result"] == [{"symbol": "005930", "name": "005930", "target_qty": 5, "ledger_qty": 2, "diff": 3,
                                    "band_skips": 0, "halted": False}]
    assert tick(e, at(15, 21)) == {"skipped": "run_done"}


def test_stop_rebalance_cancels_and_marks_skipped_then_force_restarts(env):
    e = env([_row("005930", 5, 10_000)], {"005930": {"price": 10_000, "open": 10_000}})
    r = start(e)
    tick(e, at(9, 10))
    run(ex.stop_rebalance(e.repo.db, UID, now=at(9, 12), repo=e.repo))
    assert r.status == "skipped" and e.repo.orders[0].status == "CANCELLED"
    with pytest.raises(svc.StrategyError, match="force"):
        start(e, now=at(9, 13))
    assert start(e, now=at(9, 13), force=True).status == "running" and r.attempt == 1


def test_kill_switch_stops_tick(env):
    e = env([_row("005930", 5, 10_000)], {"005930": {"price": 10_000, "open": 10_000}})
    start(e)
    e.repo.broker = SimpleNamespace(risk_kill_switch=True, risk_halt_reason="x")
    assert tick(e, at(9, 10)) == {"skipped": "kill_switch"} and e.fb.placed == []


# ── Beat 진입점 · 잠금 ───────────────────────────────────────────────────


def test_rebalance_tick_gate_and_lock(env, monkeypatch):
    e = env([_row("005930", 5, 10_000)], {"005930": {"price": 10_000, "open": 10_000}})

    class Session:
        async def __aenter__(self): return SimpleNamespace(rollback=AsyncMock())
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ex, "Repo", lambda db: e.repo)
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", False)
    assert run(ex.rebalance_tick(at(9, 10), lambda: Session())) == {"skipped": "live_execution_not_approved"}
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", True)
    assert run(ex.rebalance_tick(at(9, 10, day=date(2026, 11, 7)), lambda: Session())) == {"skipped": "not_trading_day"}
    run(ex._acquire_lock(ex.lock_key("paper")))                                          # 다른 tick 이 잡고 있음
    assert run(ex.rebalance_tick(at(9, 10), lambda: Session()))[str(e.plan.id)] == {"skipped": "locked"}
    run(ex._release_lock(ex.lock_key("paper")))
    out = run(ex.rebalance_tick(at(9, 10), lambda: Session()))[str(e.plan.id)]
    assert out["processed"][0]["status"] == "submitted"
    assert run(ex._acquire_lock(ex.lock_key("paper")))                                   # tick 이 끝나면 풀린다


def test_beat_schedule_and_routing():
    from app.celery_app import celery_app
    import app.tasks.strategy_tasks  # noqa: F401
    assert "strategy.rebalance_tick" in celery_app.tasks
    assert celery_app.amqp.router.route({}, "strategy.rebalance_tick")["queue"].name == "trading"
    tick_s = celery_app.conf.beat_schedule["strategy-rebalance-tick"]["schedule"]
    close_s = celery_app.conf.beat_schedule["strategy-rebalance-close"]["schedule"]
    assert tick_s.hour == set(range(9, 15)) and tick_s.day_of_week == {1, 2, 3, 4, 5} and len(tick_s.minute) == 60
    assert close_s.hour == {15} and {0, 20} <= close_s.minute


# ── 라우트 ───────────────────────────────────────────────────────────────


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(routes.router)
    db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    app.dependency_overrides[get_current_user_any] = lambda: {"id": str(UID)}
    app.dependency_overrides[get_pg_session] = lambda: db
    return TestClient(app)


def _run_obj():
    return StrategyRun(id=uuid.uuid4(), plan_id=uuid.uuid4(), user_id=UID, run_date=DAY, kind="rebalance", status="running",
                       invest_amount=1.0, effective_budget=1.0, cash_balance=1.0, ledger_eval=0.0, warnings=[], rows=[], summary={},
                       note="", trigger="manual", attempt=1)


def test_rebalance_routes(client):
    run_obj = _run_obj()
    with patch.object(ex, "start_rebalance", AsyncMock(return_value=run_obj)) as st:
        r = client.post("/api/strategy/atr/rebalance/start", json={"force": True})
    assert r.status_code == 200 and r.json()["attempt"] == 1 and st.await_args.kwargs == {"manual": True, "force": True}
    with patch.object(ex, "start_rebalance", AsyncMock(side_effect=svc.StrategyError("승인되지 않았습니다"))):
        r = client.post("/api/strategy/atr/rebalance/start")
    assert r.status_code == 409 and "승인" in r.json()["detail"]
    with patch.object(ex, "stop_rebalance", AsyncMock(side_effect=svc.StrategyError("진행 중인 오늘 리밸런싱이 없습니다."))):
        assert client.post("/api/strategy/atr/rebalance/stop").status_code == 409
    fake_repo = SimpleNamespace(plan_for=AsyncMock(return_value=None))
    with patch.object(ex, "Repo", lambda db: fake_repo):
        assert client.get("/api/strategy/atr/rebalance/today").json() == {"run": None, "slices": []}
