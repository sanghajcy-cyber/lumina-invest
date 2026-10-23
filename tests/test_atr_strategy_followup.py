"""첫 모의 운영(2026-10-23) 후속: 체결 이중 반영 방지 · EGW00215 · 잔고 공유 · 회차 분산 · 장부 평균가 동기화 · 거부 사유 로그."""
import asyncio
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.models import LiveOrder, StrategyPosition, StrategySlice, StrategyStopEvent
from app.services import auto_trade
from app.services.atr_strategy import executor as ex
from app.services.atr_strategy import planner, service as svc
from app.services.atr_strategy import stoploss as sl
from app.services.brokers import stock_coin_trade_gateway as gw
from tests.atr_strategy_fakes import _plan, _row, env  # noqa: F401  (env: fixture)

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
DAY = date(2026, 11, 2)
EGW = "한국투자증권 [paper] 잔고 조회 실패 (HTTP 500, EGW00215): 원장에서 허용 가능한 초당 거래건수를 초과하였습니다."


def at(hh, mm, day=DAY):
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=KST)


def run(c): return asyncio.run(c)


def bal(**holdings):
    """bal(s105560=(qty, price, avg), ...)"""
    hs = [{"symbol": k[1:], "quantity": v[0], "currentPrice": v[1], "avgPrice": v[2] if len(v) > 2 else 0} for k, v in holdings.items()]
    return {"cashBalance": 50_000_000, "totalEvalAmount": 100_000_000, "holdings": hs}


def pos(e, symbol, qty, avg, atr=None, mixed=False):
    p = next((x for x in e.repo.positions_ if x.symbol == symbol), None)
    if p is None:
        p = StrategyPosition(plan_id=e.plan.id, symbol=symbol, name=symbol)
        e.repo.add(p)
    p.quantity, p.avg_price, p.atr, p.atr_source, p.account_mixed = qty, float(avg), atr, "kis" if atr else "none", mixed
    return p


def filled_slice(e, symbol, qty, price, run_id=None):
    lo = LiveOrder(user_id=UID, client_order_id=f"x:{symbol}:B:202611020910:ATR0:{uuid.uuid4().hex[:4]}", environment="paper",
                   symbol=symbol, name=symbol, side="BUY", order_type="LIMIT", quantity=qty, price=price, order_no=uuid.uuid4().hex[:10],
                   status="FILLED", filled_quantity=qty, avg_filled_price=price, message="", raw="", source="ATR_REBAL")
    e.repo.add(lo)
    s = StrategySlice(run_id=run_id or uuid.uuid4(), plan_id=e.plan.id, symbol=symbol, side="BUY", seq=0, attempt=0, status="submitted",
                      quantity=qty, price=price, live_order_id=lo.id, filled_qty=0, client_order_id=lo.client_order_id)
    e.repo.add(s)
    return s, lo


# ── A. 체결 이중 반영 ─────────────────────────────────────────────────────


class SessionView:
    """다른 프로세스의 DB 세션처럼, 회차·손절 기록을 각자 복사본으로 읽고 반영 수량 갱신(claim_fill)만 공유 저장소에서 비교한다.

    장부 행(StrategyPosition)은 공유한다(실제로는 SELECT … FOR UPDATE 로 직렬화된다).
    """

    def __init__(self, repo, store, cas=True):
        self.repo, self.store, self.cas = repo, store, cas

    def __getattr__(self, name):
        return getattr(self.repo, name)

    def _copy(self, row):
        c = type(row)()
        for k in ("id", "run_id", "plan_id", "user_id", "trade_date", "symbol", "name", "side", "seq", "attempt", "status", "quantity",
                  "price", "live_order_id", "client_order_id", "avg_fill_price", "trigger_price", "reason"):
            if hasattr(row, k):
                setattr(c, k, getattr(row, k))
        c.filled_qty = self.store[row.id]
        return c

    async def pending_slices(self, plan):
        return [self._copy(r) for r in await self.repo.pending_slices(plan)]

    async def pending_stop_events(self, plan):
        return [self._copy(r) for r in await self.repo.pending_stop_events(plan)]

    async def claim_fill(self, row, old, new):
        await asyncio.sleep(0)                # DB 왕복 — 다른 프로세스가 끼어들 수 있다
        if self.cas and self.store[row.id] != old:
            return False
        self.store[row.id] = new
        return True

    async def refresh(self, obj):
        obj.filled_qty = self.store[obj.id]

    async def commit(self):
        await asyncio.sleep(0)


def _race(e, fn, rows, cas=True):
    """두 프로세스가 같은 미반영 체결을 동시에 읽고 반영한다(2026-10-23 손절 감시·리밸런싱 tick 동시 실행 재현)."""
    store = {r.id: int(r.filled_qty or 0) for r in rows}

    async def both():
        await asyncio.gather(fn(SessionView(e.repo, store, cas), e.plan), fn(SessionView(e.repo, store, cas), e.plan))
    run(both())
    for r in rows:
        r.filled_qty = store[r.id]


def test_concurrent_fill_sync_applies_once(env):
    """손절 감시(sync_all_pending)와 리밸런싱 tick(sync_fills)이 같은 체결을 동시에 반영해도 장부는 1주만 늘어난다."""
    e = env([], {})
    s, _ = filled_slice(e, "105560", 1, 152_600.0)
    _race(e, ex.sync_all_pending, [s])
    p = e.repo.positions_[0]
    assert (p.quantity, p.avg_price, s.filled_qty) == (1, 152_600.0, 1)


def test_race_reproduces_double_count_without_compare_and_set(env):
    """비교 후 갱신을 빼면(이전 동작) 같은 경합에서 장부가 2주가 된다 — 테스트가 실제로 경합을 만든다는 확인."""
    e = env([], {})
    s, _ = filled_slice(e, "105560", 1, 152_600.0)
    _race(e, ex.sync_all_pending, [s], cas=False)
    assert e.repo.positions_[0].quantity == 2                      # 2026-10-23 KB금융 장부 9주(실제 6주)와 같은 현상


def test_concurrent_stop_fill_applies_once(env):
    e = env([], {})
    pos(e, "105560", 6, 152_508)
    lo = LiveOrder(user_id=UID, client_order_id="x:105560:S:202611021000:SL:0", environment="paper", symbol="105560", name="",
                   side="SELL", order_type="MARKET", quantity=6, price=0.0, order_no="9", status="FILLED", filled_quantity=6,
                   avg_filled_price=140_000.0, message="", raw="", source="ATR_REBAL")
    e.repo.add(lo)
    ev = StrategyStopEvent(plan_id=e.plan.id, user_id=UID, trade_date=DAY, symbol="105560", name="", seq=0, status="submitted",
                           quantity=6, live_order_id=lo.id, filled_qty=0, client_order_id=lo.client_order_id, trigger_price=140_000)
    e.repo.add(ev)
    _race(e, sl.apply_stop_fills, [ev])
    assert e.repo.positions_[0].quantity == 0 and ev.filled_qty == 6       # 6주 매도가 한 번만 반영(두 번이면 음수 → 0 이지만 경고 없이 묻힘)
    # 같은 경합에서 비교 후 갱신이 없으면 두 번 뺀다(12 → 0)
    pos(e, "105560", 12, 152_508)
    ev.filled_qty = 0
    _race(e, sl.apply_stop_fills, [ev], cas=False)
    assert e.repo.positions_[0].quantity == 0 and ev.filled_qty == 6
    pos(e, "105560", 12, 152_508)
    ev.filled_qty = 0
    _race(e, sl.apply_stop_fills, [ev])
    assert e.repo.positions_[0].quantity == 6                               # 12주 중 6주만 빠진다


# ── B. EGW00215 ──────────────────────────────────────────────────────────


def test_is_ledger_busy():
    assert ex.is_ledger_busy(EGW) and ex.is_ledger_busy(gw.GatewayError(EGW, "KIS_ERROR", 500))
    assert not ex.is_ledger_busy("KIS_40330000 정정/취소할 수량이 없습니다") and not ex.is_ledger_busy(None)


def test_rebalance_slice_retries_once_on_egw00215_with_new_client_id(env, monkeypatch):
    e = env([_row("005930", 4, 200_000)], {"005930": {"price": 200_000, "open": 200_000}})
    run(ex.start_rebalance(e.repo.db, UID, manual=True, now=at(9, 5), repo=e.repo))
    real, calls = e.fb.place, []

    async def place(*a, **kw):
        calls.append(kw["client_order_id"])
        if len(calls) == 1:
            return {"status": "error", "code": "KIS_ERROR", "error": EGW}
        return await real(*a, **kw)

    sleep = AsyncMock()
    monkeypatch.setattr(ex.asyncio, "sleep", sleep)
    with patch.object(auto_trade, "_place_live_order_via_gateway", place):
        out = run(ex.tick_plan(e.repo, e.plan, at(9, 10)))
    assert out["processed"][0]["status"] == "submitted" and len(calls) == 2
    assert calls[0].endswith(":ATR0:0") and calls[1].endswith(":ATR0R:0")        # 재시도는 새 clientOrderId
    assert e.repo.slices_[0].client_order_id == calls[1] and sleep.await_args.args == (ex.LEDGER_BUSY_RETRY_SEC,)


def test_rebalance_slice_fails_after_second_egw00215(env, monkeypatch):
    e = env([_row("005930", 4, 200_000)], {"005930": {"price": 200_000, "open": 200_000}})
    run(ex.start_rebalance(e.repo.db, UID, manual=True, now=at(9, 5), repo=e.repo))
    monkeypatch.setattr(ex.asyncio, "sleep", AsyncMock())
    place = AsyncMock(return_value={"status": "error", "code": "KIS_ERROR", "error": EGW})
    with patch.object(auto_trade, "_place_live_order_via_gateway", place):
        out = run(ex.tick_plan(e.repo, e.plan, at(9, 10)))
    assert place.await_count == 2 and out["processed"][0]["status"] == "failed" and "EGW00215" in out["processed"][0]["reason"]


def test_stop_loss_egw00215_goes_waiting_not_failed(env):
    e = env([], {}, balance=bal(s005930=(5, 10_000)))
    pos(e, "005930", 5, 100_000)
    with patch.object(auto_trade, "_place_live_order_via_gateway", AsyncMock(return_value={"status": "error", "code": "KIS_ERROR", "error": EGW})), \
         patch("app.services.notification.notify_order_error", AsyncMock()) as notify:
        for i in range(4):
            run(sl.watch_plan(e.repo, e.plan, at(10, i)))
    assert len(e.repo.stops) == 1 and e.repo.stops[0].status == "waiting" and "EGW00215" in e.repo.stops[0].reason
    notify.assert_not_awaited()


def test_account_snapshot_retries_once_on_egw00215(monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_BASE_URL", "https://sct.test")
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_API_KEY", "key")
    monkeypatch.setattr(svc.asyncio, "sleep", AsyncMock())
    get = AsyncMock(side_effect=[gw.GatewayError(EGW, "KIS_ERROR", 500), {"cashBalance": 10, "holdings": []}])
    with patch.object(svc.gateway, "get_balance", get):
        assert run(svc.account_snapshot()) == (10.0, {})        # 2026-10-23 첫 /rebalance/start 409 의 원인
    get = AsyncMock(side_effect=[gw.GatewayError(EGW, "KIS_ERROR", 500)] * 2)
    with patch.object(svc.gateway, "get_balance", get), pytest.raises(svc.StrategyError, match="EGW00215"):
        run(svc.account_snapshot())


# ── C. 잔고 공유 · 빈 tick · 감시 30초 지연 ───────────────────────────────


def test_balance_is_shared_within_ttl_and_retried_on_egw00215(monkeypatch):
    monkeypatch.setattr(ex.risk_guard, "_redis", lambda: None)
    ex._balance_mem.clear()
    monkeypatch.setattr(ex, "BALANCE_CACHE_SEC", 15)
    monkeypatch.setattr(ex.asyncio, "sleep", AsyncMock())
    get = AsyncMock(side_effect=[gw.GatewayError(EGW, "KIS_ERROR", 500), {"holdings": [], "n": 1}, {"holdings": [], "n": 2}])
    with patch.object(gw, "get_balance", get):
        a = run(ex.get_balance_shared("paper"))                 # 1번째 EGW00215 → 재시도
        b = run(ex.get_balance_shared("paper"))                 # TTL 안 → 캐시
        c = run(ex.get_balance_shared("paper", max_age=0))      # TTL 0 → 새로 조회
    assert (a["n"], b["n"], c["n"], get.await_count) == (1, 1, 2, 3)
    ex._balance_mem.clear()


def test_idle_tick_does_not_query_balance(env):
    e = env([_row("005930", 4, 200_000)], {"005930": {"price": 200_000, "open": 200_000}})
    run(ex.start_rebalance(e.repo.db, UID, manual=True, now=at(9, 5), repo=e.repo))
    run(ex.tick_plan(e.repo, e.plan, at(9, 10)))                  # 첫 주문 회차
    with patch.object(gw, "get_balance", AsyncMock(side_effect=AssertionError("잔고를 조회하면 안 된다"))):
        out = run(ex.tick_plan(e.repo, e.plan, at(9, 15)))        # 주문 회차 아님
    assert out["idle"] is True and out["processed"] == []


def test_stop_watch_runs_at_30_seconds_past_the_minute():
    from app.celery_app import celery_app
    entry = celery_app.conf.beat_schedule["strategy-stop-loss-watch"]
    assert entry["options"]["countdown"] == 30 and entry["options"]["expires"] > 30
    assert celery_app.conf.beat_schedule["strategy-rebalance-tick"].get("options", {}).get("countdown") is None


# ── D. 회차 분산 ─────────────────────────────────────────────────────────


def _hhmm(seq, last=planner.BUY_LAST_SLICE):
    return planner.slice_times(last)[seq].strftime("%H:%M")


def test_plan_slots_spread_over_remaining_time():
    slots = ex.plan_slots(0, 70, 4)
    assert slots == [0, 17, 35, 52] and [_hhmm(s) for s in slots] == ["09:10", "10:35", "12:05", "13:30"]
    assert ex.plan_slots(0, 70, 6) == [0, 11, 23, 35, 46, 58]
    assert ex.plan_slots(0, 70, 200) == list(range(70))           # 주식 수 ≥ 회차 → 매 회차
    assert ex.plan_slots(60, 10, 3) == [60, 63, 66]               # 늦게 시작해도 남은 시간에 고르게
    sells = ex.plan_slots(0, planner.SELL_ROUNDS, 3)
    assert max(sells) < planner.SELL_ROUNDS and _hhmm(max(sells), planner.SELL_LAST_SLICE) <= "14:30"
    assert ex.plan_slots(0, 70, 0) == []


def test_slot_due_and_catch_up():
    slots = [0, 17, 35, 52]
    assert ex.slot_due(slots, 0, 4) == (True, 4) and ex.slot_due(slots, 1, 3) == (False, 3)
    assert ex.slot_due(slots, 17, 3) == (True, 3) and ex.slot_due(slots, 52, 1) == (True, 1)
    assert ex.slot_due(slots, 60, 1) == (True, 1)                 # 마지막 주문 회차 뒤에도 남았으면 따라잡기
    assert ex.slot_due(slots, 60, 0) == (False, 0)


def test_orders_follow_slots_not_every_round(env):
    e = env([_row("005930", 4, 200_000), _row("105560", 6, 150_000)],
            {"005930": {"price": 200_000, "open": 200_000}, "105560": {"price": 150_000, "open": 150_000}})
    run(ex.start_rebalance(e.repo.db, UID, manual=True, now=at(9, 5), repo=e.repo))
    placed_at = {}
    for i in range(70):
        t = datetime.combine(DAY, planner.slice_times(planner.BUY_LAST_SLICE)[i], tzinfo=KST)
        n = len(e.fb.placed)
        run(ex.tick_plan(e.repo, e.plan, t))
        for o in e.repo.orders[n:]:
            e.fb.fill(o.order_no, o.quantity)                       # 바로 체결
            placed_at.setdefault(o.symbol, []).append(t.strftime("%H:%M"))
    assert placed_at["005930"] == ["09:10", "10:35", "12:05", "13:30"]
    assert placed_at["105560"] == ["09:10", "10:05", "11:05", "12:05", "13:00", "14:00"]
    assert {p.symbol: p.quantity for p in e.repo.positions_} == {"005930": 4, "105560": 6}


# ── F. 장부 평균가 · 계좌 혼합 ───────────────────────────────────────────


def test_avg_synced_to_kis_when_account_is_all_strategy(env):
    """KB금융: 장부 6주 = 계좌 6주, 전략 밖 보유분 없음 → KIS 매입평균가 152,508 로 맞춘다."""
    e = env([], {}, balance=bal(s105560=(6, 153_000, 152_508)))
    p = pos(e, "105560", 6, 152_728.888)
    with patch.object(sl, "audit", AsyncMock()) as aud:
        out = run(sl.watch_plan(e.repo, e.plan, at(10, 0)))
    assert p.avg_price == 152_508 and out["avg_sync"] == [{"symbol": "105560", "name": "105560", "avg_before": 152_728.89, "avg_after": 152_508.0}]
    assert any(c.args[2] == "strategy.atr_rebal.ledger_avg_sync" for c in aud.await_args_list)


def test_mixed_account_is_flagged_and_never_synced_even_after_manual_sell(env):
    """삼성전자: 계좌 14주(직접 산 10주 + 전략 4주) → 섞임 표시. HTS 로 10주를 팔아 계좌 4주가 돼도 평균가·수량 그대로."""
    e = env([], {}, balance=bal(s005930=(14, 210_000, 219_745)))
    p = pos(e, "005930", 4, 207_000)
    run(sl.watch_plan(e.repo, e.plan, at(10, 0)))
    assert p.account_mixed is True and p.avg_price == 207_000 and p.quantity == 4
    e.fb.balance = bal(s005930=(4, 210_000, 219_745))               # HTS 수동 매도 10주 — KIS 평균가는 평균법이라 그대로
    out = run(sl.watch_plan(e.repo, e.plan, at(10, 1)))
    assert (p.quantity, p.avg_price, p.account_mixed) == (4, 207_000, True) and out["adjustments"] == [] and out["avg_sync"] == []


def test_avg_sync_skipped_while_order_or_fill_pending_and_mixed_resets_at_zero(env):
    e = env([], {}, balance=bal(s105560=(7, 153_000, 152_600)))
    p = pos(e, "105560", 6, 152_508)
    s, lo = filled_slice(e, "105560", 1, 152_700.0)
    lo.status, lo.filled_quantity = "ACCEPTED", 0                     # 체결 확인 전: 계좌 7 > 장부 6 이지만 섞임으로 보지 않는다
    run(sl.watch_plan(e.repo, e.plan, at(10, 0)))
    assert p.account_mixed is False and p.avg_price == 152_508
    p.quantity, p.account_mixed = 0, True
    e.fb.balance = bal()
    run(sl.watch_plan(e.repo, e.plan, at(10, 1)))
    assert p.account_mixed is False                                   # 계좌·장부 모두 0 → KIS 평균가 초기화 → 표시 해제


def test_mixed_survives_manual_sell_and_next_rebalance_plan(env):
    """14주(직접 산 10주 + 전략 4주, 섞임) → HTS 10주 매도 → 11/2 리밸런싱 계획(계좌 4 = 장부 4) → 섞임·평균가 207,000 유지."""
    e = env([_row("005930", 4, 210_000, account_qty=4, ledger_qty=4)], {"005930": {"price": 210_000, "open": 210_000}},
            balance=bal(s005930=(14, 210_000, 219_745)))
    p = pos(e, "005930", 4, 207_000)
    run(sl.watch_plan(e.repo, e.plan, at(10, 0, day=date(2026, 10, 26))))         # 계좌 14 > 장부 4 → 섞임
    assert p.account_mixed is True
    e.fb.balance = bal(s005930=(4, 210_000, 219_745))                               # HTS 10주 매도(KIS 평균가는 평균법이라 그대로)
    run(sl.watch_plan(e.repo, e.plan, at(10, 1, day=date(2026, 10, 26))))
    assert (p.quantity, p.avg_price, p.account_mixed) == (4, 207_000, True)
    r = run(ex.start_rebalance(e.repo.db, UID, manual=False, now=at(9, 5), repo=e.repo))   # 11/2 첫 거래일 자동 시작
    assert r.trigger == "auto" and r.rows[0]["account_qty"] == r.rows[0]["ledger_qty"] == 4
    assert p.account_mixed is True                                                  # 계획 시점 계좌 = 장부여도 false 로 안 돌아간다
    out = run(sl.watch_plan(e.repo, e.plan, at(9, 6)))                              # 계획 뒤 첫 감시
    assert (p.quantity, p.avg_price, p.account_mixed) == (4, 207_000, True) and out["avg_sync"] == []


def test_mixed_clears_only_when_account_reaches_zero_then_fresh_buy_syncs(env):
    e = env([], {}, balance=bal(s005930=(4, 210_000, 219_745)))
    p = pos(e, "005930", 4, 207_000, mixed=True)
    run(sl.watch_plan(e.repo, e.plan, at(10, 0)))
    assert p.account_mixed is True and p.avg_price == 207_000
    p.quantity, p.avg_price = 0, 0.0                                               # 전략이 4주를 다 팔았지만
    e.fb.balance = bal(s005930=(3, 210_000, 219_745))                               # 계좌에 아직 3주가 있다(직접 산 몫)
    run(sl.watch_plan(e.repo, e.plan, at(10, 1)))
    assert p.account_mixed is True                                                  # 계좌 0 이 아니므로 유지
    e.fb.balance = bal()                                                            # 계좌 0 → KIS 평균가 초기화
    run(sl.watch_plan(e.repo, e.plan, at(10, 2)))
    assert p.account_mixed is False
    p.quantity, p.avg_price = 2, 200_100.0                                          # 이후 전략이 새로 2주(지정가 기준)
    e.fb.balance = bal(s005930=(2, 200_000, 200_000))
    run(sl.watch_plan(e.repo, e.plan, at(10, 3)))
    assert p.avg_price == 200_000                                                   # 전부 전략 몫 → KIS 평균가로 맞춘다


def test_freeze_atr_marks_mixed_when_plan_saw_foreign_shares(env):
    e = env([_row("005930", 4, 200_000, account_qty=10, ledger_qty=0), _row("105560", 6, 150_000, account_qty=0, ledger_qty=0)],
            {"005930": {"price": 200_000, "open": 200_000}, "105560": {"price": 150_000, "open": 150_000}})
    run(ex.start_rebalance(e.repo.db, UID, manual=True, now=at(9, 5), repo=e.repo))
    mixed = {p.symbol: bool(p.account_mixed) for p in e.repo.positions_}
    assert mixed == {"005930": True, "105560": False}


# ── E. 거부 사유 로그 ────────────────────────────────────────────────────


def test_rebalance_start_refusal_is_logged(caplog):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.database.postgres import get_pg_session
    from app.lib.jwt_auth import get_current_user_any
    from app.routes import atr_strategy as routes
    app = FastAPI()
    app.include_router(routes.router)
    app.dependency_overrides[get_current_user_any] = lambda: {"id": str(UID)}
    app.dependency_overrides[get_pg_session] = lambda: SimpleNamespace(rollback=AsyncMock())
    with patch.object(ex, "start_rebalance", AsyncMock(side_effect=svc.StrategyError("장부 동기화 실패: KIS 잔고 조회 실패 EGW00215"))), \
         caplog.at_level(logging.WARNING, logger="app.routes.atr_strategy"):
        r = TestClient(app).post("/api/strategy/atr/rebalance/start", json={"force": False})
    assert r.status_code == 409
    assert any("/rebalance/start 거부" in m and "EGW00215" in m and str(UID) in m for m in caplog.messages)
