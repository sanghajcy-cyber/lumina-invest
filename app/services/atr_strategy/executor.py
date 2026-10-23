"""리밸런싱 날 시분할 주문 실행 (4단계).

Beat `strategy.rebalance_tick` 이 거래일 09:00~15:20 매분 호출한다. 한 번의 tick 은
  1) 오늘 리밸런싱 실행(StrategyRun kind=rebalance)이 없고 오늘이 첫 거래일이면 새로 시작, 있으면 **이어받는다**
  2) 장부에 체결을 반영한다(live_orders.filled_quantity − 회차가 이미 반영한 수량)
  3) 종목·방향마다 처음 할 일이 생긴 회차에 주문 회차 k = min(남은 회차, 남은 수량)개를 남은 매매 시간
     (매수 ~14:55, 매도 ~14:30)에 고르게 배치하고(plan_slots, rows[].slots_BUY/SELL), 이번 회차가 그 주문 회차인 종목만
     매도 먼저 최대 MAX_SYMBOLS_PER_TICK 개 처리한다 → 종목이 많으면 같은 회차 5분 안에 여러 tick 으로 나뉜다.
     할 일이 없는 tick 은 잔고를 조회하지 않는다. 잔고는 손절 감시와 BALANCE_CACHE_SEC 동안 공유한다.
  4) 15:00 열린 주문 일괄 취소, 15:20 결과 확정
종목 1개 처리 = 이전 회차의 열린 주문 취소·확인 → 장부 기준 남은 수량 재계산 → (매수) 위험관리·시가±ATR 범위 →
회차 수량 = ceil(남은 수량 ÷ 남은 주문 회차 수) → LIMIT(매수 현재가+1틱 / 매도 현재가−1틱).
주문 회차가 아닌 동안에는 열린 주문을 그대로 두고, 다음 주문 회차에 취소·재주문한다. EGW00215 는 1.5초 뒤 1회 재시도.
체결 반영은 비교 후 갱신(claim_fill)과 장부 행 잠금으로 손절 감시와 동시에 돌아도 두 번 더하지 않는다.
회차 기록(StrategySlice)을 주문 **전에** 남기고 커밋하므로, 주문 직후 프로세스가 죽어도 재시작 시 같은 회차를 다시 내지 않는다.

DB 접근은 Repo 로 모았다(테스트는 메모리 구현으로 바꿔 끼운다).
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import time as _time
import uuid
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BrokerSettings, LiveOrder, LIVE_ORDER_OPEN_STATUSES, LIVE_ORDER_SOURCE_ATR_REBAL,
    StrategyPlan, StrategyPosition, StrategyRun, StrategySlice, StrategyStopEvent,
)
from app.services import risk_guard
from app.services.atr_strategy import planner
from app.services.atr_strategy import service as svc
from app.services.audit import audit
from app.services.brokers import stock_coin_trade_gateway as gateway

logger = logging.getLogger(__name__)

KST = planner.KST
REBAL_TAG = "ATR"                 # clientOrderId 태그 ATR{attempt}
STOP_TAG = "SL"                   # 손절 주문 태그 — 리밸런싱 취소·일괄 취소·중지 대상이 아니다
# tick 당 처리 종목 수. 게이트웨이 Open API 한도는 API 키당 분당 60회이고 이 키를 다른 태스크와 같이 쓴다.
#   종목 1개(최악): 이전 주문 상태 1 + 취소 1 + 재조회 1 + 승인(order-approval) 1 + 주문 1 = 5회  (시세는 /api/kis-chart 라 한도 밖)
#   리밸런싱 tick = 잔고 1 + 4종목 × 5 = 21회
#   같은 분의 다른 호출(최악): 손절 감시 잔고 1 + 손절 1건(취소 3 + 승인·주문 2) ≈ 6,
#                              confirm_fills(2분) 열린 주문 1건당 1 — 20종목이면 ≤ 20, reconcile(10분) ≈ 2
#   합계 ≈ 21 + 6 + 20 + 2 = 49 < 60.  6종목이면 31 + 28 = 59 로 여유가 없어 4로 둔다.
#   st 는 KIS 호출을 1.05초 간격으로 직렬화하므로 종목당 KIS 6~8회 ≈ 8초 → 4종목 ≈ 32초 < tick time_limit 55초.
#   20종목이면 한 회차(5분)를 5 tick 에 나눠 처리한다(매도 먼저).
MAX_SYMBOLS_PER_TICK = 4
FINALIZE_AT = time(15, 20)
LOCK_TTL_SEC = 55
BALANCE_CACHE_SEC = 15            # 리밸런싱 tick 과 손절 감시가 잔고(원장 조회)를 이 시간 동안 공유한다
LEDGER_BUSY_RETRY_SEC = 1.5       # EGW00215(원장 초당 거래건수 초과) 뒤 재시도 간격
SYMBOL_LOCK_TTL_SEC = 50          # 리밸런싱 tick 과 손절 감시가 같은 종목을 동시에 건드리지 않게


def now_kst() -> datetime:
    return datetime.now(KST)


# ── 시간표·수량·가격 (순수 함수) ───────────────────────────────────────────


def current_round(now: datetime, side: str) -> tuple[int, int] | None:
    """(회차 번호 0부터, 이번 회차 포함 남은 회차 수). 회차 창 밖이면 None.

    회차 창은 시작 시각부터 5분. 마지막 매도 회차(14:30)는 1분만 — 매도는 14:30 까지만 낸다.
    """
    last = planner.SELL_LAST_SLICE if side == "SELL" else planner.BUY_LAST_SLICE
    times = planner.slice_times(last)
    t = now.astimezone(KST).time()
    for i in range(len(times) - 1, -1, -1):
        if times[i] <= t:
            width = 1 if (side == "SELL" and i == len(times) - 1) else planner.SLICE_INTERVAL_MIN
            end = (datetime.combine(date(2000, 1, 1), times[i]) + timedelta(minutes=width)).time()
            return (i, len(times) - i) if t < end else None
    return None


def slice_quantity(remaining: int, rounds_left: int, price: float, max_order_amount: float) -> tuple[int, str]:
    """회차 수량 = ceil(남은 수량 ÷ min(남은 회차, 남은 수량)), 1건 금액 상한으로 자른다."""
    if remaining <= 0 or rounds_left <= 0:
        return 0, "남은 수량 없음"
    qty = math.ceil(remaining / min(rounds_left, remaining))
    cap = int(max_order_amount // price) if price > 0 else 0
    if cap < 1:
        return 0, f"1주 가격 {price:,.0f}원이 1건 상한 {max_order_amount:,.0f}원을 넘음"
    return min(qty, cap), ""


def limit_price(price: float, side: str) -> int:
    """LIMIT 대체가: 매수 현재가 + 1틱, 매도 현재가 − 1틱 (호가 단위 맞춤, 구간 경계는 아래 구간 틱)."""
    if side == "BUY":
        base = gateway.align_price_to_tick(price, "buy")
        return base + gateway.tick_size(base)
    base = gateway.align_price_to_tick(price, "sell")
    return max(1, base - gateway.tick_size(max(1, base - 1)))


def remaining_qty(row: dict, side: str, ledger_qty: int, account_qty: int) -> int:
    target = int(row.get("target_qty") or 0)
    if side == "BUY":
        return max(0, target - ledger_qty)
    return max(0, min(ledger_qty - target, min(ledger_qty, account_qty)))


def plan_slots(seq: int, rounds_left: int, remaining: int) -> list[int]:
    """남은 회차(seq 부터 rounds_left 개)에 주문 회차 k = min(남은 회차, 남은 수량)개를 고르게 배치한다.

    예: 09:10(seq 0, 남은 70회)에 4주 → seq 0, 17, 35, 52 = 09:10, 10:35, 12:05, 13:30 (약 1시간 25분 간격).
    """
    k = min(rounds_left, remaining)
    if k <= 0:
        return []
    return sorted({seq + (j * rounds_left) // k for j in range(k)})


def slot_due(slots: list[int], seq: int, remaining: int) -> tuple[bool, int]:
    """(이번 회차에 주문할지, 이번 회차를 포함한 남은 주문 회차 수).

    마지막 주문 회차가 지났는데 남은 수량이 있으면(미체결·건너뜀) 이후 매 회차 따라잡는다.
    """
    if remaining <= 0 or not slots:
        return False, 0
    left = [x for x in slots if x >= seq]
    if seq in slots:
        return True, len(left)
    if seq > max(slots):
        return True, 1
    return False, len(left)


# ── 저장소 ──────────────────────────────────────────────────────────────


class Repo:
    """executor 가 쓰는 DB 조회·기록. 커밋 시점은 호출자(executor)가 정한다."""

    def __init__(self, db: AsyncSession):
        self.db = db

    def add(self, obj) -> None:
        self.db.add(obj)

    async def commit(self) -> None:
        await self.db.commit()

    async def flush(self) -> None:
        await self.db.flush()

    async def live_plans(self) -> list[StrategyPlan]:
        return list((await self.db.execute(select(StrategyPlan).where(StrategyPlan.is_active.is_(True),
                                                                       StrategyPlan.dry_run.is_(False)))).scalars().all())

    async def plan_for(self, user_id: uuid.UUID) -> StrategyPlan | None:
        return await svc.get_plan(self.db, user_id, create=False)

    async def broker_row(self, user_id: uuid.UUID) -> BrokerSettings | None:
        return (await self.db.execute(select(BrokerSettings).where(BrokerSettings.user_id == user_id))).scalar_one_or_none()

    async def today_run(self, plan: StrategyPlan, day: date) -> StrategyRun | None:
        return (await self.db.execute(select(StrategyRun).where(StrategyRun.plan_id == plan.id, StrategyRun.run_date == day,
                                                                StrategyRun.kind == "rebalance"))).scalar_one_or_none()

    async def slices(self, run: StrategyRun) -> list[StrategySlice]:
        return list((await self.db.execute(select(StrategySlice).where(StrategySlice.run_id == run.id))).scalars().all())

    async def live_order(self, order_id: uuid.UUID) -> LiveOrder | None:
        # confirm_fills 등 다른 프로세스가 갱신한 체결 수량을 읽도록 세션 캐시를 덮어쓴다
        stmt = select(LiveOrder).where(LiveOrder.id == order_id).execution_options(populate_existing=True)
        return (await self.db.execute(stmt)).scalar_one_or_none()

    async def live_order_by_client_id(self, client_order_id: str) -> LiveOrder | None:
        return (await self.db.execute(select(LiveOrder).where(LiveOrder.client_order_id == client_order_id))).scalar_one_or_none()

    async def open_rebalance_orders(self, user_id: uuid.UUID, symbol: str | None = None) -> list[LiveOrder]:
        stmt = select(LiveOrder).where(LiveOrder.user_id == user_id, LiveOrder.source == LIVE_ORDER_SOURCE_ATR_REBAL,
                                       LiveOrder.status.in_(LIVE_ORDER_OPEN_STATUSES),
                                       LiveOrder.client_order_id.like(f"%:{REBAL_TAG}%"))
        if symbol:
            stmt = stmt.where(LiveOrder.symbol == symbol)
        return list((await self.db.execute(stmt)).scalars().all())

    async def position(self, plan: StrategyPlan, symbol: str, lock: bool = False) -> StrategyPosition | None:
        stmt = select(StrategyPosition).where(StrategyPosition.plan_id == plan.id, StrategyPosition.symbol == symbol)
        if lock:   # SELECT … FOR UPDATE — 다른 프로세스의 장부 갱신이 끝날 때까지 기다리고 최신 값을 읽는다
            stmt = stmt.with_for_update().execution_options(populate_existing=True)
        return (await self.db.execute(stmt)).scalar_one_or_none()

    async def claim_fill(self, row, old: int, new: int) -> bool:
        """회차·손절 기록의 반영 수량을 old → new 로 바꾼다(비교 후 갱신). 1행이 바뀐 쪽만 장부에 반영한다.

        두 프로세스가 같은 체결을 동시에 반영하려 하면 한쪽 UPDATE 는 행 잠금을 기다렸다가 WHERE 가 맞지 않아 0행이 된다.
        """
        model = type(row)
        res = await self.db.execute(update(model).where(model.id == row.id, model.filled_qty == old)
                                    .values(filled_qty=new).execution_options(synchronize_session=False))
        return res.rowcount == 1

    async def refresh(self, obj) -> None:
        await self.db.refresh(obj)

    async def ledger(self, plan: StrategyPlan) -> dict[str, dict]:
        return await svc.load_ledger(self.db, plan)

    async def open_stop_orders(self, user_id: uuid.UUID, symbol: str | None = None) -> list[LiveOrder]:
        stmt = select(LiveOrder).where(LiveOrder.user_id == user_id, LiveOrder.source == LIVE_ORDER_SOURCE_ATR_REBAL,
                                       LiveOrder.status.in_(LIVE_ORDER_OPEN_STATUSES),
                                       LiveOrder.client_order_id.like(f"%:{STOP_TAG}:%"))
        if symbol:
            stmt = stmt.where(LiveOrder.symbol == symbol)
        return list((await self.db.execute(stmt)).scalars().all())

    async def pending_slices(self, plan: StrategyPlan) -> list[StrategySlice]:
        """체결이 아직 다 반영되지 않았을 수 있는 회차(모든 날짜). confirm_fills 가 장 마감 뒤에 늦게 갱신한 체결도 놓치지 않는다."""
        return list((await self.db.execute(select(StrategySlice).where(StrategySlice.plan_id == plan.id,
                                                                       StrategySlice.status == "submitted"))).scalars().all())

    async def stop_events(self, plan: StrategyPlan, day: date | None = None) -> list[StrategyStopEvent]:
        stmt = select(StrategyStopEvent).where(StrategyStopEvent.plan_id == plan.id)
        if day is not None:
            stmt = stmt.where(StrategyStopEvent.trade_date == day)
        return list((await self.db.execute(stmt.order_by(StrategyStopEvent.created_at))).scalars().all())

    async def pending_stop_events(self, plan: StrategyPlan) -> list[StrategyStopEvent]:
        return list((await self.db.execute(select(StrategyStopEvent).where(
            StrategyStopEvent.plan_id == plan.id, StrategyStopEvent.status.in_(("submitted", "planned"))))).scalars().all())

    async def positions(self, plan: StrategyPlan, lock: bool = False) -> list[StrategyPosition]:
        stmt = select(StrategyPosition).where(StrategyPosition.plan_id == plan.id).order_by(StrategyPosition.symbol)
        if lock:   # 종목 순서대로 잠가 다른 프로세스와 잠금 순서를 맞춘다
            stmt = stmt.with_for_update().execution_options(populate_existing=True)
        return list((await self.db.execute(stmt)).scalars().all())

    async def orders_placed_today(self, plan: StrategyPlan, day: date) -> int:
        stmt = (select(func.count(StrategySlice.id)).join(StrategyRun, StrategyRun.id == StrategySlice.run_id)
                .where(StrategyRun.plan_id == plan.id, StrategyRun.run_date == day, StrategySlice.live_order_id.is_not(None)))
        return int((await self.db.execute(stmt)).scalar_one() or 0)


# ── 계좌 잠금 ───────────────────────────────────────────────────────────

_mem_lock: dict[str, float] = {}


async def _acquire_lock(key: str) -> bool:
    r = risk_guard._redis()
    if r is not None:
        try:
            return bool(await r.set(key, str(_time.time()), nx=True, ex=LOCK_TTL_SEC))
        except Exception as exc:
            logger.warning("전략 잠금 redis 실패, 메모리 폴백: %s", exc)
    now = _time.time()
    if _mem_lock.get(key, 0) > now:
        return False
    _mem_lock[key] = now + LOCK_TTL_SEC
    return True


async def _release_lock(key: str) -> None:
    r = risk_guard._redis()
    if r is not None:
        try:
            await r.delete(key)
            return
        except Exception:
            pass
    _mem_lock.pop(key, None)


def lock_key(environment: str) -> str:
    return f"strategy:acct-lock:kis:{environment}"


def is_ledger_busy(err) -> bool:
    """KIS EGW00215(원장에서 허용 가능한 초당 거래건수 초과). st 는 HTTP 500 KIS_ERROR 로 메시지에 코드를 담아 돌려준다."""
    return "EGW00215" in str(err or "")


_balance_mem: dict[str, tuple[float, dict]] = {}


async def get_balance_shared(environment: str, max_age: float | None = None) -> dict:
    """게이트웨이 잔고를 max_age 초 동안 Redis 로 공유한다(원장 조회가 매분 정각에 몰려 EGW00215 가 나는 것을 줄인다).

    EGW00215 면 LEDGER_BUSY_RETRY_SEC 뒤 1회 다시 조회한다. 실패하면 GatewayError 를 그대로 올린다.
    """
    max_age = BALANCE_CACHE_SEC if max_age is None else max_age
    key = f"strategy:balance:kis:{environment}"
    now = _time.time()
    r = risk_guard._redis()
    if r is not None:
        try:
            raw = await r.get(key)
            if raw:
                cached = json.loads(raw)
                if now - float(cached.get("ts", 0)) <= max_age:
                    return cached["balance"]
        except Exception as exc:
            logger.warning("잔고 공유 캐시 읽기 실패: %s", exc)
    elif key in _balance_mem and now - _balance_mem[key][0] <= max_age:
        return _balance_mem[key][1]
    try:
        bal = await gateway.get_balance()
    except gateway.GatewayError as exc:
        if not is_ledger_busy(exc):
            raise
        await asyncio.sleep(LEDGER_BUSY_RETRY_SEC)
        bal = await gateway.get_balance()
    if r is not None:
        try:
            await r.set(key, json.dumps({"ts": _time.time(), "balance": bal}, ensure_ascii=False), ex=int(max_age) + 5)
        except Exception as exc:
            logger.warning("잔고 공유 캐시 쓰기 실패: %s", exc)
    else:
        _balance_mem[key] = (_time.time(), bal)
    return bal


def symbol_lock_key(environment: str, symbol: str) -> str:
    return f"strategy:sym-lock:kis:{environment}:{symbol}"


async def acquire_symbol_lock(environment: str, symbol: str) -> bool:
    key = symbol_lock_key(environment, symbol)
    r = risk_guard._redis()
    if r is not None:
        try:
            return bool(await r.set(key, str(_time.time()), nx=True, ex=SYMBOL_LOCK_TTL_SEC))
        except Exception as exc:
            logger.warning("종목 잠금 redis 실패, 메모리 폴백: %s", exc)
    now = _time.time()
    if _mem_lock.get(key, 0) > now:
        return False
    _mem_lock[key] = now + SYMBOL_LOCK_TTL_SEC
    return True


async def release_symbol_lock(environment: str, symbol: str) -> None:
    await _release_lock(symbol_lock_key(environment, symbol))


# ── 장부 반영 ───────────────────────────────────────────────────────────


async def apply_fill(repo: Repo, plan: StrategyPlan, symbol: str, name: str, side: str, qty: int, price: float) -> None:
    if qty <= 0:
        return
    pos = await repo.position(plan, symbol, lock=True)
    if pos is None:
        pos = StrategyPosition(plan_id=plan.id, symbol=symbol, name=name or "", quantity=0, avg_price=0.0, atr_source="none")
        repo.add(pos)
    q = int(pos.quantity or 0)
    if side == "BUY":
        new_q = q + qty
        pos.avg_price = (float(pos.avg_price or 0) * q + float(price) * qty) / new_q
        pos.quantity = new_q
    else:
        pos.quantity = max(0, q - qty)
        if pos.quantity == 0:
            pos.avg_price = 0.0
    if name and not pos.name:
        pos.name = name


_TERMINAL_SLICE = {"FILLED": "filled", "CANCELLED": "cancelled", "REJECTED": "failed", "ERROR": "failed", "LOST": "failed"}


async def sync_fills(repo: Repo, plan: StrategyPlan, run: StrategyRun) -> int:
    """오늘 실행의 회차 체결 증가분을 장부에 반영. 반영한 수량 합을 돌려준다."""
    return await _sync_slices(repo, plan, await repo.slices(run))


async def sync_all_pending(repo: Repo, plan: StrategyPlan) -> int:
    """날짜와 무관하게 submitted 로 남은 모든 회차의 체결을 반영(장 마감 뒤 늦은 체결 확인 포함)."""
    return await _sync_slices(repo, plan, await repo.pending_slices(plan))


async def _sync_slices(repo: Repo, plan: StrategyPlan, slices: list[StrategySlice]) -> int:
    applied = 0
    for sl in slices:
        if not sl.live_order_id or sl.status not in ("submitted",):
            continue
        lo = await repo.live_order(sl.live_order_id)
        if lo is None:
            continue
        old, new = int(sl.filled_qty or 0), int(lo.filled_quantity or 0)
        if new > old:
            if not await repo.claim_fill(sl, old, new):
                await repo.refresh(sl)      # 다른 프로세스(손절 감시·리밸런싱 tick)가 이미 반영했다
                continue
            price = float(lo.avg_filled_price or 0) or float(lo.price or 0)
            sl.filled_qty = new
            sl.avg_fill_price = price
            await apply_fill(repo, plan, sl.symbol, lo.name, sl.side, new - old, price)
            await repo.commit()             # 체결 1건 = 짧은 트랜잭션(기록 행 → 장부 행 순서로 잠금) — 교착 방지
            applied += new - old
        if lo.status in _TERMINAL_SLICE:
            sl.status = _TERMINAL_SLICE[lo.status] if not (lo.status == "CANCELLED" and sl.filled_qty) else "filled"
    return applied


def _update_live_order(lo: LiveOrder, latest: dict) -> None:
    if not latest:
        return
    lo.status = str(latest.get("status") or lo.status)
    if latest.get("filledQuantity") is not None:
        lo.filled_quantity = int(latest.get("filledQuantity") or 0)
    if latest.get("avgFilledPrice"):
        lo.avg_filled_price = float(latest.get("avgFilledPrice") or 0)
    lo.raw = json.dumps(latest, ensure_ascii=False)[:4000]


async def cancel_and_confirm(lo: LiveOrder) -> bool:
    """열린 주문을 취소하고 다시 조회해 닫혔는지 확인. 닫혔으면(체결·취소·거부) True.

    이미 체결된 주문의 취소는 KIS 가 거부하므로(README 5) 오류여도 재조회 결과로 판단한다.
    """
    if lo.status not in LIVE_ORDER_OPEN_STATUSES:
        return True
    if not lo.order_no:
        return False          # 주문번호를 아직 모름(PENDING/UNKNOWN) — confirm_fills 가 정리할 때까지 기다린다
    env = lo.environment
    try:
        _update_live_order(lo, await gateway.get_order_status(lo.order_no, env=env))
        if lo.status not in LIVE_ORDER_OPEN_STATUSES:
            return True
        if lo.status != "CANCEL_REQUESTED":
            try:
                _update_live_order(lo, await gateway.cancel_order(lo.order_no, env=env))
                lo.status = lo.status if lo.status != "ACCEPTED" else "CANCEL_REQUESTED"
            except gateway.GatewayError as exc:
                logger.info("ATR 전략 취소 거부(체결됐을 수 있음) %s: [%s] %s", lo.order_no, exc.code, exc)
        _update_live_order(lo, await gateway.get_order_status(lo.order_no, env=env))
    except gateway.GatewayError as exc:
        logger.warning("ATR 전략 주문 상태 확인 실패 %s: [%s] %s", lo.order_no, exc.code, exc)
        return False
    return lo.status not in LIVE_ORDER_OPEN_STATUSES


# ── 실행 시작·재실행·중지 ───────────────────────────────────────────────


async def _freeze_atr(repo: Repo, plan: StrategyPlan, rows: list[dict]) -> None:
    """리밸런싱 날 ATR 을 장부 행에 고정(다음 리밸런싱까지 손절 기준)."""
    for r in rows:
        if not r.get("in_portfolio"):
            continue
        pos = await repo.position(plan, r["symbol"])
        if pos is None:
            pos = StrategyPosition(plan_id=plan.id, symbol=r["symbol"], name=r.get("name") or "", quantity=0, avg_price=0.0)
            repo.add(pos)
        pos.atr = r.get("atr")
        pos.atr_source = r.get("atr_source") or "none"
        if int(r.get("account_qty") or 0) > int(r.get("ledger_qty") or 0):
            pos.account_mixed = True        # 계획 시점에 계좌에 전략 밖 보유분이 있었다(예: 직접 산 삼성전자 10주)
        pos.atr_as_of = date.fromisoformat(r["atr_as_of"]) if r.get("atr_as_of") else None


async def start_rebalance(db: AsyncSession, user_id: uuid.UUID, *, manual: bool, force: bool = False,
                          now: datetime | None = None, repo: Repo | None = None) -> StrategyRun:
    """오늘 리밸런싱을 시작(또는 이어받기/재실행)한다.

    manual=True: 첫 거래일이 아니어도 시작(참고 구현 --rebalance).
    오늘 실행이 이미 있으면 force=False 는 그대로 돌려주고(이어받기), force=True 는 열린 주문을 취소한 뒤
    현재 장부 기준으로 계획을 다시 세우고 attempt 를 올려 남은 회차를 새로 진행한다(참고 구현 --force).
    """
    repo = repo or Repo(db)
    now = (now or now_kst()).astimezone(KST)
    today = now.date()
    plan = await repo.plan_for(user_id)
    if plan is None:
        raise svc.StrategyError("전략 플랜이 없습니다.")
    if not svc.LIVE_EXECUTION_AVAILABLE:
        raise svc.StrategyError("실제 모의주문 실행이 아직 승인되지 않았습니다(dry-run 확인 후 별도 승인).")
    if not plan.is_active or plan.dry_run:
        raise svc.StrategyError("전략이 실주문 모드(dry_run=false)로 켜져 있지 않습니다.")
    if not gateway.is_krx_trading_day(today):
        raise svc.StrategyError(f"오늘({today})은 거래일이 아닙니다.")
    if not manual and today != gateway.first_trading_day_of_month(today.year, today.month):
        raise svc.StrategyError("오늘은 이번 달 첫 거래일이 아닙니다. 수동 실행(manual)으로 시작하세요.")
    if now.time() >= planner.BUY_LAST_SLICE:
        raise svc.StrategyError("오늘 남은 매매 회차가 없습니다 (마지막 매수 회차 14:55).")
    broker_row = await repo.broker_row(user_id)
    if broker_row is not None and broker_row.risk_kill_switch:
        raise svc.StrategyError(f"비상 정지 상태입니다. (사유: {broker_row.risk_halt_reason or '수동 정지'})")

    run = await repo.today_run(plan, today)
    if run is not None and not force:
        if run.status != "running" and manual:
            raise svc.StrategyError(f"오늘 리밸런싱이 이미 {run.status} 상태입니다. 처음부터 다시 하려면 force 로 재실행하세요.")
        return run
    if run is not None:
        for lo in await repo.open_rebalance_orders(user_id):
            await cancel_and_confirm(lo)
        await sync_fills(repo, plan, run)
        await repo.flush()

    # 계획 전에 늦게 확인된 체결을 반영하고 장부를 계좌 수량에 맞춘다(수동 매도 반영)
    await sync_all_pending(repo, plan)
    from app.services.atr_strategy import stoploss
    try:
        _cash, acct = await svc.account_snapshot()
        await stoploss.sync_ledger_with_account(repo, plan, acct)
    except svc.StrategyError as exc:
        raise svc.StrategyError(f"장부 동기화 실패: {exc}") from exc
    await repo.flush()
    fields = await svc.compute_plan(db, plan, user_id, "rebalance", today)
    if run is None:
        run = StrategyRun(plan_id=plan.id, user_id=user_id, run_date=today, kind="rebalance", attempt=0, **fields)
        repo.add(run)
    else:
        for k, v in fields.items():
            setattr(run, k, v)
        run.attempt = int(run.attempt or 0) + 1
    run.status = "running"
    run.trigger = "manual" if manual else "auto"
    run.started_at = now.astimezone(timezone.utc)
    run.finished_at = None
    await _freeze_atr(repo, plan, run.rows)
    await repo.commit()
    await audit(str(user_id), "", "strategy.atr_rebal.rebalance_start",
                {"run_id": str(run.id), "manual": manual, "force": force, "attempt": run.attempt,
                 "effective_budget": run.effective_budget, "warnings": run.warnings[:5]})
    return run


async def cancel_all(repo: Repo, plan: StrategyPlan, run: StrategyRun, reason: str) -> dict:
    out = {"closed": 0, "still_open": 0}
    for lo in await repo.open_rebalance_orders(run.user_id):
        if await cancel_and_confirm(lo):
            out["closed"] += 1
        else:
            out["still_open"] += 1
        lo.message = (lo.message + " · " if lo.message else "") + reason
    await sync_fills(repo, plan, run)
    return out


async def finalize(repo: Repo, plan: StrategyPlan, run: StrategyRun, now: datetime, status: str = "done", note: str = "") -> None:
    await sync_fills(repo, plan, run)
    await repo.flush()
    ledger = await repo.ledger(plan)
    stopped = {e.symbol for e in await repo.stop_events(plan, run.run_date)}
    result = []
    for r in run.rows:
        if not (r.get("in_portfolio") or r.get("excluded")) or r.get("target_qty") is None:
            continue
        held = int((ledger.get(r["symbol"]) or {}).get("quantity") or 0)
        result.append({"symbol": r["symbol"], "name": r.get("name"), "target_qty": r["target_qty"], "ledger_qty": held,
                       "diff": int(r["target_qty"]) - held, "band_skips": int(r.get("band_skips") or 0),
                       "halted": bool(r.get("halted")) or r["symbol"] in stopped, "excluded": bool(r.get("excluded"))})
    summary = dict(run.summary or {})
    summary["result"] = result
    summary["result_mismatch"] = sum(1 for x in result if x["diff"])
    run.summary = summary
    run.status = status
    run.finished_at = now.astimezone(timezone.utc)
    if note:
        run.note = (run.note + " · " + note)[:300]


async def stop_rebalance(db: AsyncSession, user_id: uuid.UUID, now: datetime | None = None, repo: Repo | None = None) -> StrategyRun:
    """오늘 리밸런싱을 멈춘다: 열린 주문 취소 후 skipped 로 확정. 다시 하려면 force 재실행."""
    repo = repo or Repo(db)
    now = (now or now_kst()).astimezone(KST)
    plan = await repo.plan_for(user_id)
    run = await repo.today_run(plan, now.date()) if plan else None
    if run is None or run.status != "running":
        raise svc.StrategyError("진행 중인 오늘 리밸런싱이 없습니다.")
    cancelled = await cancel_all(repo, plan, run, "수동 중지")
    summary = dict(run.summary or {})
    summary["cancelled_at"] = now.isoformat()
    summary["stop_cancel"] = cancelled          # still_open > 0 이면 Beat tick 이 이어서 취소한다
    run.summary = summary
    await finalize(repo, plan, run, now, status="skipped", note="수동 중지")
    await repo.commit()
    await audit(str(user_id), "", "strategy.atr_rebal.rebalance_stop", {"run_id": str(run.id)})
    return run


# ── 회차 처리 ───────────────────────────────────────────────────────────


async def stop_blocked_symbols(repo: Repo, plan: StrategyPlan, user_id: uuid.UUID, day: date) -> set[str]:
    """오늘 손절 이벤트가 있거나 손절 주문이 열려 있는 종목 — 리밸런싱은 이 종목을 취소도 주문도 하지 않는다."""
    blocked = {e.symbol for e in await repo.stop_events(plan, day)}
    blocked |= {o.symbol for o in await repo.open_stop_orders(user_id)}
    return blocked


async def _buy_block_reason(plan: StrategyPlan, broker_row: BrokerSettings | None, equity: float) -> str | None:
    """매수 전용 위험관리: 일손실 한도(risk_guard). 손절 매도에는 적용하지 않는다."""
    limits = risk_guard.RiskLimits.from_row(broker_row)
    if equity <= 0 or limits.daily_loss_limit_pct <= 0:
        return None
    start = await risk_guard.day_start_equity(f"{plan.user_id}:live:{plan.environment}", equity)
    if risk_guard.daily_loss_breached(start, equity, limits.daily_loss_limit_pct):
        return f"일손실 한도 {limits.daily_loss_limit_pct:g}% 도달 (당일 {risk_guard.daily_pnl_pct(start, equity):+.2f}%) — 매수 중단"
    return None


def buy_cooldown_min(broker_row: BrokerSettings | None) -> int:
    """risk_guard 쿨다운을 이 전략 매수에 쓰되, 5분 회차를 막지 않게 회차 간격 − 1분으로 자른다."""
    limits = risk_guard.RiskLimits.from_row(broker_row)
    return max(0, min(int(limits.cooldown_min), planner.SLICE_INTERVAL_MIN - 1))


async def _process_symbol(repo: Repo, plan: StrategyPlan, run: StrategyRun, row: dict, side: str, seq: int, rounds_left: int,
                          ctx: dict, now: datetime) -> StrategySlice:
    sym, uid = row["symbol"], run.user_id
    if not await acquire_symbol_lock(plan.environment, sym):
        return None                       # 손절 감시가 이 종목을 처리 중 — 기록 없이 다음 tick 에 다시 본다
    try:
        if sym in await stop_blocked_symbols(repo, plan, uid, run.run_date):
            return None
        return await _process_symbol_locked(repo, plan, run, row, side, seq, rounds_left, ctx, now)
    finally:
        await release_symbol_lock(plan.environment, sym)


async def _process_symbol_locked(repo: Repo, plan: StrategyPlan, run: StrategyRun, row: dict, side: str, seq: int,
                                 rounds_left: int, ctx: dict, now: datetime) -> StrategySlice:
    from app.services import auto_trade, kis_market_data as kmd

    sym, uid = row["symbol"], run.user_id
    sl = StrategySlice(run_id=run.id, plan_id=plan.id, symbol=sym, side=side, seq=seq, attempt=int(run.attempt or 0), status="planned",
                       quantity=0, price=0.0, order_type="LIMIT", client_order_id="", reason="", filled_qty=0, avg_fill_price=0.0)
    repo.add(sl)

    def skip(reason: str, status: str = "skipped") -> StrategySlice:
        sl.status, sl.reason = status, reason[:300]
        return sl

    for lo in await repo.open_rebalance_orders(uid, sym):
        if not await cancel_and_confirm(lo):
            return skip("이전 회차 주문 취소 확인 대기")
    await sync_fills(repo, plan, run)
    await repo.flush()
    ledger_qty = int(((await repo.ledger(plan)).get(sym) or {}).get("quantity") or 0)
    remaining = remaining_qty(row, side, ledger_qty, int(ctx["account_qty"].get(sym, 0)))
    if remaining <= 0:
        return skip("남은 수량 없음")
    if ctx["orders_today"] >= int(plan.max_orders_per_day):
        return skip(f"전략 일 주문 수 한도 {plan.max_orders_per_day} 도달")

    try:
        q = await kmd.get_quote(sym)
    except Exception as exc:
        return skip(f"시세 조회 실패: {str(exc)[:150]}")
    price = float(q.get("price") or 0)
    if price <= 0:
        return skip("현재가 없음")

    if side == "BUY":
        if ctx["buy_block"]:
            return skip(ctx["buy_block"])
        if not row.get("open") and float(q.get("open") or 0) > 0:
            row["open"] = float(q["open"])
            band = planner.buy_band(row["open"], row.get("atr"), plan.buy_band_atr_mult)
            if band:
                row["band_low"], row["band_high"] = round(band[0], 2), round(band[1], 2)
        if not row.get("open"):
            return skip("시가 없음(장 시작 전)")
        if row.get("band_low") is not None and not (row["band_low"] <= price <= row["band_high"]):
            row["band_skips"] = int(row.get("band_skips") or 0) + 1
            return skip(f"매수 범위 밖 (현재가 {price:,.0f}, 범위 {row['band_low']:,.0f}~{row['band_high']:,.0f})")

    qty, why = slice_quantity(remaining, rounds_left, price, plan.max_order_amount)
    if qty <= 0:
        return skip(why)
    if side == "BUY" and not await risk_guard.acquire_order_slot(f"atr:{uid}", sym, "buy", ctx["cooldown_min"]):
        return skip(f"매수 쿨다운 {ctx['cooldown_min']}분")

    lp = limit_price(price, side)
    sl.quantity, sl.price, sl.order_type = qty, float(lp), "LIMIT"
    sl.client_order_id = gateway.make_client_order_id(uid, sym, side, now=now, tag=f"{REBAL_TAG}{sl.attempt}", seq=seq)
    await repo.commit()                     # 주문 전에 회차 기록을 확정 — 재시작 시 같은 회차를 다시 내지 않는다

    res = await auto_trade._place_live_order_via_gateway(
        repo.db, None, sym, row.get("name") or sym, side.lower(), qty, float(lp), str(uid),
        source=LIVE_ORDER_SOURCE_ATR_REBAL, order_type="LIMIT", client_order_id=sl.client_order_id,
    )
    if res.get("status") == "error" and is_ledger_busy(res.get("error")):
        # EGW00215: 바로 실패로 닫지 않고 잠시 뒤 같은 tick 안에서 1회 재시도(새 clientOrderId — 첫 시도는 ERROR 행으로 남는다)
        logger.info("ATR 전략 %s %s 회차 %d: EGW00215 — %.1f초 뒤 재시도", sym, side, seq, LEDGER_BUSY_RETRY_SEC)
        await asyncio.sleep(LEDGER_BUSY_RETRY_SEC)
        sl.client_order_id = gateway.make_client_order_id(uid, sym, side, now=now, tag=f"{REBAL_TAG}{sl.attempt}R", seq=seq)
        await repo.commit()
        res = await auto_trade._place_live_order_via_gateway(
            repo.db, None, sym, row.get("name") or sym, side.lower(), qty, float(lp), str(uid),
            source=LIVE_ORDER_SOURCE_ATR_REBAL, order_type="LIMIT", client_order_id=sl.client_order_id,
        )
    lo = await repo.live_order_by_client_id(sl.client_order_id)
    sl.live_order_id = lo.id if lo is not None else None
    if res.get("status") == "submitted":
        sl.status = "submitted"
        ctx["orders_today"] += 1
    else:
        sl.status = "skipped" if res.get("status") == "skipped" else "failed"
        sl.reason = str(res.get("reason") or res.get("error") or res.get("code") or res.get("status"))[:300]
        if side == "BUY":
            await risk_guard.release_order_slot(f"atr:{uid}", sym, "buy")
        if res.get("status") == "error" and lo is not None and lo.status == "UNKNOWN":
            sl.status = "submitted"          # 게이트웨이 응답 미수신 — confirm_fills 가 멱등 재전송·확인
    return sl


async def _resolve_interrupted(repo: Repo, run: StrategyRun) -> None:
    """주문 직전에 죽어 planned 로 남은 회차: live_orders 에 있으면 연결, 없으면 중단으로 닫는다."""
    for sl in await repo.slices(run):
        if sl.status != "planned":
            continue
        lo = await repo.live_order_by_client_id(sl.client_order_id) if sl.client_order_id else None
        if lo is not None:
            sl.live_order_id, sl.status = lo.id, "submitted"
        else:
            sl.status, sl.reason = "failed", "주문 전 중단됨(재시작)"


async def process_round(repo: Repo, plan: StrategyPlan, run: StrategyRun, now: datetime) -> dict:
    await _resolve_interrupted(repo, run)
    await sync_fills(repo, plan, run)

    done = {(s.side, s.seq, s.symbol) for s in await repo.slices(run) if s.attempt == int(run.attempt or 0)}
    blocked = await stop_blocked_symbols(repo, plan, run.user_id, run.run_date)
    ledger = await repo.ledger(plan)
    rows = copy.deepcopy(run.rows)
    todo = []
    for side in ("SELL", "BUY"):
        rnd = current_round(now, side)
        if rnd is None:
            continue
        seq, rounds_left = rnd
        for row in rows:
            # 목표 종목 + 목표에서 빠진 장부 종목(excluded, 목표수량 0 → 전량 매도). 장부에 없는 계좌 종목은 행 자체가 없다.
            tradable = row.get("in_portfolio") or row.get("excluded")
            if not tradable or row.get("target_qty") is None or row.get("halted") or row["symbol"] in blocked:
                continue
            if (side, seq, row["symbol"]) in done:
                continue
            held = int((ledger.get(row["symbol"]) or {}).get("quantity") or 0)
            # 계좌 수량은 아직 모른다 — 매도 상한을 장부 수량으로 두고 할 일이 있는지만 본다(없으면 잔고를 조회하지 않는다)
            rem = remaining_qty(row, side, held, held)
            key = f"slots_{side}"
            if rem > 0 and not row.get(key):
                row[key] = plan_slots(seq, rounds_left, rem)          # 처음 할 일이 생긴 회차에 남은 매매 시간으로 고르게 배치
            due, slots_left = slot_due(row.get(key) or [], seq, rem)
            if not due:
                continue                                            # 주문 회차가 아니면 열린 주문도 그대로 둔다
            todo.append((side, seq, slots_left, row))

    if not todo:
        run.rows = rows
        return {"processed": [], "deferred": 0, "idle": True}

    try:
        bal = await get_balance_shared(plan.environment)
    except gateway.GatewayError as exc:
        run.rows = rows
        return {"skipped": f"잔고 조회 실패 [{exc.code}]"}
    account_qty: dict[str, int] = {}
    for h in bal.get("holdings") or []:
        try:
            account_qty[gateway.normalize_symbol(str(h.get("symbol") or ""))] = int(h.get("quantity") or 0)
        except gateway.GatewayError:
            continue
    broker_row = await repo.broker_row(run.user_id)
    equity = float(bal.get("totalEvalAmount") or 0)
    ctx = {"account_qty": account_qty, "orders_today": await repo.orders_placed_today(plan, run.run_date),
           "buy_block": await _buy_block_reason(plan, broker_row, equity), "cooldown_min": buy_cooldown_min(broker_row)}

    processed = []
    for side, seq, slots_left, row in todo[:MAX_SYMBOLS_PER_TICK]:
        sl = await _process_symbol(repo, plan, run, row, side, seq, slots_left, ctx, now)
        if sl is None:
            processed.append({"symbol": row["symbol"], "side": side, "seq": seq, "status": "deferred", "qty": 0, "reason": "손절 처리 중"})
            continue
        processed.append({"symbol": row["symbol"], "side": side, "seq": seq, "status": sl.status, "qty": sl.quantity, "reason": sl.reason})
    # 손절 여부는 rows 가 아니라 strategy_stop_events 가 기준이다(손절 감시는 다른 프로세스라 rows 를 쓰지 않는다)
    run.rows = rows
    return {"processed": processed, "deferred": max(0, len(todo) - MAX_SYMBOLS_PER_TICK)}


async def tick_plan(repo: Repo, plan: StrategyPlan, now: datetime) -> dict:
    today = now.date()
    broker_row = await repo.broker_row(plan.user_id)
    if broker_row is not None and broker_row.risk_kill_switch:
        return {"skipped": "kill_switch"}
    run = await repo.today_run(plan, today)
    if run is None:
        first = gateway.first_trading_day_of_month(today.year, today.month)
        if today != first or not (planner.SLICE_START <= now.time() < planner.BUY_LAST_SLICE):
            return {"skipped": "no_run"}
        try:
            run = await start_rebalance(repo.db, plan.user_id, manual=False, now=now, repo=repo)
        except svc.StrategyError as exc:
            logger.warning("ATR 전략 자동 리밸런싱 시작 실패 user=%s: %s", plan.user_id, exc)
            return {"skipped": f"start_failed: {exc}"}
    if run.status != "running":
        # 중지·확정 뒤에도 취소가 확인되지 않은 리밸런싱 주문이 남아 있으면 계속 정리한다(손절 주문은 건드리지 않음)
        if await repo.open_rebalance_orders(run.user_id):
            cleanup = await cancel_all(repo, plan, run, "중지/마감 뒤 잔여 주문 정리")
            await repo.commit()
            return {"skipped": f"run_{run.status}", "cleanup": cleanup}
        return {"skipped": f"run_{run.status}"}
    t = now.time()
    if t >= planner.CANCEL_ALL_AT:
        out: dict = {"run_id": str(run.id)}
        if not (run.summary or {}).get("cancelled_at"):
            out["cancel_all"] = await cancel_all(repo, plan, run, "15:00 일괄 취소")
            run.summary = {**(run.summary or {}), "cancelled_at": now.isoformat()}
        if t >= FINALIZE_AT:
            await finalize(repo, plan, run, now)
            out["finalized"] = True
        await repo.commit()
        return out
    out = await process_round(repo, plan, run, now)
    await repo.commit()
    return {"run_id": str(run.id), **out}


async def rebalance_tick(now: datetime | None = None, session_factory=None) -> dict:
    """Beat 진입점. 거래일이 아니거나 실행 승인 전이면 아무것도 하지 않는다."""
    now = (now or now_kst()).astimezone(KST)
    if not svc.LIVE_EXECUTION_AVAILABLE:
        return {"skipped": "live_execution_not_approved"}
    if not gateway.is_krx_trading_day(now.date()):
        return {"skipped": "not_trading_day"}
    if session_factory is None:
        from app.database.postgres import get_session_factory
        session_factory = get_session_factory()
    results = {}
    async with session_factory() as db:
        repo = Repo(db)
        for plan in await repo.live_plans():
            key = lock_key(plan.environment)
            if not await _acquire_lock(key):
                results[str(plan.id)] = {"skipped": "locked"}
                continue
            try:
                results[str(plan.id)] = await tick_plan(repo, plan, now)
            except Exception as exc:
                logger.exception("ATR 전략 tick 실패 plan=%s", plan.id)
                await db.rollback()
                results[str(plan.id)] = {"error": str(exc)[:200]}
            finally:
                await _release_lock(key)
    return results
