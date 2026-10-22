"""월초 ATR 리밸런싱 5단계: 손절 감시 · 장부 동기화 · 리밸런싱과의 상호작용 · 중지 시 주문 취소."""
import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.models import LiveOrder, StrategyPosition, StrategySlice, StrategyStopEvent
from app.services import risk_guard
from app.services.atr_strategy import executor as ex
from app.services.atr_strategy import service as svc
from app.services.atr_strategy import stoploss as sl
from tests.atr_strategy_fakes import _plan, _row, env  # noqa: F401  (env: fixture)

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
DAY = date(2026, 11, 2)
MID = date(2026, 11, 10)


def at(hh, mm, day=DAY):
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=KST)


def run(c): return asyncio.run(c)


def bal(**holdings):
    """bal(s005930=(qty, price), ...)"""
    hs = [{"symbol": k[1:], "quantity": q, "currentPrice": p} for k, (q, p) in holdings.items()]
    return {"cashBalance": 50_000_000, "totalEvalAmount": 100_000_000, "holdings": hs}


def pos(e, symbol, qty, avg, atr=None):
    """장부 행 설정. start_rebalance 가 ATR 고정용으로 만든 행이 있으면 그 행을 쓴다(실제 테이블은 (plan, symbol) UNIQUE)."""
    p = next((x for x in e.repo.positions_ if x.symbol == symbol), None)
    if p is None:
        p = StrategyPosition(plan_id=e.plan.id, symbol=symbol, name=symbol)
        e.repo.add(p)
    p.quantity, p.avg_price, p.atr, p.atr_source = qty, float(avg), atr, "kis" if atr else "none"
    return p


def watch(e, now=at(10, 0)): return run(sl.watch_plan(e.repo, e.plan, now))


def open_order(e, symbol, side, cid_tag="ATR0", qty=5, price=10_000.0, status="ACCEPTED"):
    e.fb.n += 1
    lo = LiveOrder(user_id=UID, client_order_id=f"x:{symbol}:{side[0]}:202611020910:{cid_tag}:0", environment="paper", broker="kis",
                   symbol=symbol, name=symbol, side=side, order_type="LIMIT", quantity=qty, price=price, order_no=f"{e.fb.n:010d}",
                   status=status, filled_quantity=0, avg_filled_price=0.0, message="", raw="", source="ATR_REBAL")
    e.repo.add(lo)
    return lo


# ── 손절 판정 ─────────────────────────────────────────────────────────────


def test_evaluate_stop_atr_and_percent():
    assert sl.evaluate_stop(96_000, 100_000, 2_000, 2.0, -10) == (True, 96_000, "평균가 − ATR×2")
    assert sl.evaluate_stop(96_100, 100_000, 2_000, 2.0, -10)[0] is False
    assert sl.evaluate_stop(90_000, 100_000, None, 2.0, -10) == (True, 90_000, "평균가 -10%")     # ATR 없으면 % 대체
    assert sl.evaluate_stop(0, 100_000, None, 2.0, -10)[0] is False                              # 현재가 없음 → 판단 안 함


# ── 손절 실행 ─────────────────────────────────────────────────────────────


def test_stop_cancels_both_rebalance_sides_then_market_sells_ledger_qty(env):
    e = env([], {}, balance=bal(s005930=(8, 95_000)))
    pos(e, "005930", 10, 100_000, atr=2_000)                       # 계좌 8주(2주는 수동 매도했다고 가정)
    buy, sell = open_order(e, "005930", "BUY"), open_order(e, "005930", "SELL")
    out = watch(e)
    assert buy.status == "CANCELLED" and sell.status == "CANCELLED"
    # 매도 주문이 열려 있던 동안은 장부를 줄이지 않는다(체결 미확인일 수 있음) → 매도 수량은 min(장부 10, 계좌 8)
    assert e.repo.positions_[0].quantity == 10 and out["adjustments"] == []
    sym, side, qty, price, cid = e.fb.placed[-1]
    assert (sym, side, qty) == ("005930", "SELL", 8) and ":SL:0" in cid
    lo = e.repo.orders[-1]
    assert lo.order_type == "MARKET" and lo.source == "ATR_REBAL"
    ev = e.repo.stops[0]
    assert (ev.status, ev.quantity, ev.stop_price, ev.basis, ev.seq) == ("submitted", 8, 96_000, "평균가 − ATR×2", 0)
    assert out["stops"][0]["status"] == "submitted"
    lo.status, lo.filled_quantity = "FILLED", 8
    e.fb.balance = bal()
    out = watch(e, at(10, 1))                                                       # 체결 반영(10→2) 후 계좌 0 → 장부 0
    assert ev.status == "filled" and e.repo.positions_[0].quantity == 0 and out["adjustments"][0]["manual_sold"] == 2


def test_stop_waits_when_rebalance_cancel_not_confirmed_then_continues(env):
    e = env([], {}, balance=bal(s005930=(10, 85_000)))
    pos(e, "005930", 10, 100_000)
    buy = open_order(e, "005930", "BUY")
    e.fb.cancel_sticks.add(buy.order_no)
    watch(e)
    ev = e.repo.stops[0]
    assert ev.status == "waiting" and "취소 확인 대기" in ev.reason and e.fb.placed == []
    e.fb.cancel_sticks.clear()
    e.fb.balance = bal(s005930=(10, 99_000))                       # 가격이 회복돼도 시작한 손절은 끝낸다
    watch(e, at(10, 1))
    assert ev.status == "submitted" and e.fb.placed[-1][1:3] == ("SELL", 10) and len(e.repo.stops) == 1


def test_no_new_stop_while_stop_order_open_or_unconfirmed_and_fill_goes_to_ledger(env):
    e = env([], {}, balance=bal(s005930=(10, 85_000)))
    pos(e, "005930", 10, 100_000)
    watch(e)
    watch(e, at(10, 1))
    assert len(e.fb.placed) == 1 and len(e.repo.stops) == 1                   # 열린 손절 주문 → 새 손절 없음
    lo = e.repo.orders[-1]
    lo.status, lo.filled_quantity, lo.avg_filled_price = "FILLED", 10, 84_900.0
    e.fb.balance = bal()
    watch(e, at(10, 2))
    ev = e.repo.stops[0]
    assert ev.status == "filled" and ev.filled_qty == 10 and e.repo.positions_[0].quantity == 0 and len(e.fb.placed) == 1


def test_failed_stop_retries_until_limit_then_notifies(env):
    e = env([], {}, balance=bal(s005930=(10, 85_000)))
    pos(e, "005930", 10, 100_000)
    with patch("app.services.notification.notify_order_error", AsyncMock()) as notify:
        for i in range(5):
            watch(e, at(10, i))
            e.repo.orders[-1].status = "REJECTED"
    assert [s.status for s in e.repo.stops] == ["failed", "failed", "failed"]
    assert len(e.fb.placed) == sl.MAX_STOP_FAILURES and [s.seq for s in e.repo.stops] == [0, 1, 2]
    notify.assert_awaited_once()


def test_stop_ignores_risk_limits_and_strategy_order_limit(env):
    broker = SimpleNamespace(risk_kill_switch=False, risk_halt_reason="", risk_daily_loss_limit_pct=1.0, risk_max_position_pct=30.0,
                             risk_max_orders_per_day=0, risk_cooldown_min=30)
    e = env([], {}, plan=_plan(max_orders_per_day=0), broker_row=broker, balance=bal(s005930=(5, 80_000)))
    pos(e, "005930", 5, 100_000)
    run(risk_guard.day_start_equity(f"{UID}:live:paper", 200_000_000))           # 일손실 −50%
    watch(e)
    assert e.fb.placed and e.fb.placed[-1][1] == "SELL"


def test_stop_covers_all_ledger_symbols_but_not_account_only_and_kill_switch(env):
    e = env([], {}, balance=bal(s005930=(5, 10_000), s000660=(5, 10_000), s035720=(9, 10)))
    pos(e, "005930", 5, 100_000)                                                   # 목표 종목
    pos(e, "000660", 5, 100_000)                                                   # 목표에서 빠져 매도 중인 장부 종목 → 감시 계속
    watch(e)                                                                       # 035720: 장부에 없는 직접 산 종목 → 손대지 않음
    assert sorted(p[0] for p in e.fb.placed) == ["000660", "005930"]
    sold = next(o for o in e.repo.orders if o.symbol == "000660")
    sold.status, sold.filled_quantity = "FILLED", 5
    e.fb.balance = bal(s005930=(5, 10_000), s035720=(9, 10))
    n = len(e.fb.placed)
    watch(e, at(10, 1))
    assert next(p for p in e.repo.positions_ if p.symbol == "000660").quantity == 0 and len(e.fb.placed) == n   # 다 팔면 감시 끝
    e2 = env([], {}, broker_row=SimpleNamespace(risk_kill_switch=True, risk_halt_reason="x"), balance=bal(s005930=(5, 10_000)))
    pos(e2, "005930", 5, 100_000)
    assert watch(e2) == {"skipped": "kill_switch"} and e2.fb.placed == []


# ── 장부 동기화 ───────────────────────────────────────────────────────────


def test_ledger_sync_clamps_to_account_but_not_while_sell_open(env):
    e = env([], {}, balance=bal(s005930=(3, 100_000), s000660=(0, 0)))
    a = pos(e, "005930", 10, 100_000)
    b = pos(e, "000660", 4, 50_000)
    open_order(e, "005930", "SELL")                                                 # 매도 체결 확인 전 → 건너뜀
    with patch.object(sl, "audit", AsyncMock()) as aud:
        out = watch(e)
    assert a.quantity == 10 and b.quantity == 0 and b.avg_price == 0.0
    assert out["adjustments"] == [{"symbol": "000660", "name": "000660", "ledger_before": 4, "account": 0, "manual_sold": 4}]
    assert any(c.args[2] == "strategy.atr_rebal.ledger_sync" for c in aud.await_args_list)


def test_late_fill_from_previous_day_is_applied(env):
    e = env([], {}, balance=bal(s005930=(5, 100_000)))
    old = SimpleNamespace(id=uuid.uuid4())
    lo = open_order(e, "005930", "BUY", qty=5, status="FILLED")
    lo.filled_quantity, lo.avg_filled_price = 5, 99_000.0
    e.repo.add(StrategySlice(run_id=old.id, plan_id=e.plan.id, symbol="005930", side="BUY", seq=3, attempt=0, status="submitted",
                             quantity=5, live_order_id=lo.id, filled_qty=0, client_order_id=lo.client_order_id))
    watch(e)
    assert e.repo.positions_[0].quantity == 5 and e.repo.positions_[0].avg_price == 99_000


# ── 리밸런싱과의 상호작용 ─────────────────────────────────────────────────


def _start(e, now=at(9, 5)):
    return run(ex.start_rebalance(e.repo.db, UID, manual=True, now=now, repo=e.repo))


def test_rebalance_tick_skips_stopped_symbol_without_cancelling_stop_order(env):
    e = env([_row("005930", 20, 100_000), _row("000660", 5, 10_000)],
            {"005930": {"price": 85_000, "open": 100_000}, "000660": {"price": 10_000, "open": 10_000}},
            balance=bal(s005930=(10, 85_000)))
    _start(e)
    pos(e, "005930", 10, 100_000)
    watch(e, at(9, 10))
    stop_order = e.repo.orders[-1]
    assert ":SL:" in stop_order.client_order_id and stop_order.status == "ACCEPTED"
    out = run(ex.tick_plan(e.repo, e.plan, at(9, 11)))
    assert [p["symbol"] for p in out["processed"]] == ["000660"]                      # 손절 종목은 건너뜀
    assert stop_order.status == "ACCEPTED" and stop_order.order_no not in e.fb.cancelled
    stop_order.status, stop_order.filled_quantity = "FILLED", 10
    out = run(ex.tick_plan(e.repo, e.plan, at(9, 15)))
    assert "005930" not in [p["symbol"] for p in out["processed"]]                    # 그날 손절된 종목 → 이후 회차도 매수 안 함
    run(ex.tick_plan(e.repo, e.plan, at(15, 0)))
    assert stop_order.order_no not in e.fb.cancelled                                  # 15:00 일괄 취소도 손절 주문은 제외


def test_symbol_lock_defers_the_other_side(env):
    e = env([_row("005930", 9, 10_000)], {"005930": {"price": 10_000, "open": 10_000}}, balance=bal(s005930=(5, 5_000)))
    _start(e)
    pos(e, "005930", 5, 10_000)
    run(ex.acquire_symbol_lock("paper", "005930"))
    out = run(ex.tick_plan(e.repo, e.plan, at(9, 10)))
    assert out["processed"][0]["status"] == "deferred" and e.fb.placed == []
    assert watch(e, at(9, 10))["stops"][0]["status"] == "deferred" and e.repo.stops == []
    run(ex.release_symbol_lock("paper", "005930"))
    assert watch(e, at(9, 11))["stops"][0]["status"] == "submitted"


def test_rebalance_stop_cancels_open_buy_and_sell_but_not_stop_orders_and_watch_keeps_running(env):
    e = env([_row("005930", 20, 10_000), _row("000660", 0, 10_000)],
            {"005930": {"price": 10_000, "open": 10_000}, "000660": {"price": 10_000, "open": 10_000}},
            balance=bal(s000660=(10, 10_000), s035720=(5, 1_000)))
    r = _start(e)
    pos(e, "000660", 10, 10_000)
    pos(e, "035720", 5, 10_000)                                                       # 손절 대상
    run(ex.tick_plan(e.repo, e.plan, at(9, 10)))                                      # 매수 1건 + 매도 1건
    watch(e, at(9, 10))
    rebal_open = [o for o in e.repo.orders if ":ATR" in o.client_order_id]
    stop_open = [o for o in e.repo.orders if ":SL:" in o.client_order_id]
    assert {o.side for o in rebal_open} == {"BUY", "SELL"} and len(stop_open) == 1
    e.fb.cancel_sticks.add(rebal_open[0].order_no)                                    # 하나는 취소 확인이 늦다
    run(ex.stop_rebalance(e.repo.db, UID, now=at(9, 12), repo=e.repo))
    assert r.status == "skipped" and r.summary["stop_cancel"] == {"closed": 1, "still_open": 1}
    assert stop_open[0].status == "ACCEPTED"                                          # 손절 주문은 그대로
    e.fb.cancel_sticks.clear()
    out = run(ex.tick_plan(e.repo, e.plan, at(9, 13)))                                # 중지 뒤에도 잔여 리밸런싱 주문 정리
    assert out["cleanup"] == {"closed": 1, "still_open": 0}
    assert all(o.status not in ex.LIVE_ORDER_OPEN_STATUSES for o in rebal_open)
    e.fb.balance = bal(s000660=(10, 8_000), s035720=(5, 1_000))
    pos_row = next(p for p in e.repo.positions_ if p.symbol == "000660")
    watch(e, at(9, 20))                                                               # 리밸런싱 중지와 무관하게 손절 감시
    assert any(o.symbol == "000660" and ":SL:" in o.client_order_id for o in e.repo.orders) and pos_row.quantity == 10


# ── Beat 진입점 ───────────────────────────────────────────────────────────


def test_stop_loss_watch_gate_window_and_schedule(env, monkeypatch):
    e = env([], {}, balance=bal(s005930=(5, 10_000)))
    pos(e, "005930", 5, 100_000)

    class Session:
        async def __aenter__(self): return SimpleNamespace(rollback=AsyncMock())
        async def __aexit__(self, *a): return False

    monkeypatch.setattr(ex, "Repo", lambda db: e.repo)
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", False)
    assert run(sl.stop_loss_watch(at(10, 0), lambda: Session())) == {"skipped": "live_execution_not_approved"}
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", True)
    assert run(sl.stop_loss_watch(at(9, 4), lambda: Session())) == {"skipped": "outside_window"}
    assert run(sl.stop_loss_watch(at(15, 16), lambda: Session())) == {"skipped": "outside_window"}
    assert run(sl.stop_loss_watch(at(10, 0, day=date(2026, 11, 7)), lambda: Session())) == {"skipped": "not_trading_day"}
    out = run(sl.stop_loss_watch(at(15, 15, day=MID), lambda: Session()))
    assert out[str(e.plan.id)]["stops"][0]["status"] == "submitted"

    from app.celery_app import celery_app
    import app.tasks.strategy_tasks  # noqa: F401
    assert "strategy.stop_loss_watch" in celery_app.tasks
    assert celery_app.amqp.router.route({}, "strategy.stop_loss_watch")["queue"].name == "trading"
    sched = celery_app.conf.beat_schedule["strategy-stop-loss-watch"]["schedule"]
    assert sched.hour == set(range(9, 16)) and len(sched.minute) == 60


def test_restart_resolves_planned_stop_event(env):
    e = env([], {}, balance=bal(s005930=(5, 10_000)))
    pos(e, "005930", 5, 100_000)
    a = StrategyStopEvent(plan_id=e.plan.id, user_id=UID, trade_date=DAY, symbol="005930", name="", seq=0, status="planned",
                          client_order_id="cid-missing", quantity=5, filled_qty=0, trigger_price=10_000, reason="")
    e.repo.add(a)
    watch(e, at(10, 0))
    assert a.status == "failed" and "중단" in a.reason
    assert e.repo.stops[-1].seq == 1 and e.repo.stops[-1].status == "submitted"       # 같은 날 새 시도
