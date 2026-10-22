"""리밸런싱 날 시분할 주문 실행 (4단계).

Beat `strategy.rebalance_tick` 이 거래일 09:00~15:20 매분 호출한다. 한 번의 tick 은
  1) 오늘 리밸런싱 실행(StrategyRun kind=rebalance)이 없고 오늘이 첫 거래일이면 새로 시작, 있으면 **이어받는다**
  2) 장부에 체결을 반영한다(live_orders.filled_quantity − 회차가 이미 반영한 수량)
  3) 현재 회차(5분 단위)에서 아직 처리하지 않은 종목을 매도 먼저, 최대 MAX_SYMBOLS_PER_TICK 개 처리한다
     → 종목이 많으면 같은 회차 5분 안에 여러 tick 으로 나뉜다(게이트웨이 분당 60회 호출 예산)
  4) 15:00 열린 주문 일괄 취소, 15:20 결과 확정
종목 1개 처리 = 이전 회차의 열린 주문 취소·확인 → 장부 기준 남은 수량 재계산 → (매수) 위험관리·시가±ATR 범위 →
회차 수량 = ceil(남은 수량 ÷ min(남은 회차, 남은 수량)) → LIMIT(매수 현재가+1틱 / 매도 현재가−1틱).
회차 기록(StrategySlice)을 주문 **전에** 남기고 커밋하므로, 주문 직후 프로세스가 죽어도 재시작 시 같은 회차를 다시 내지 않는다.

DB 접근은 Repo 로 모았다(테스트는 메모리 구현으로 바꿔 끼운다).
"""
from __future__ import annotations

import copy
import json
import logging
import math
import time as _time
import uuid
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BrokerSettings, LiveOrder, LIVE_ORDER_OPEN_STATUSES, LIVE_ORDER_SOURCE_ATR_REBAL,
    StrategyPlan, StrategyPosition, StrategyRun, StrategySlice,
)
from app.services import risk_guard
from app.services.atr_strategy import planner
from app.services.atr_strategy import service as svc
from app.services.audit import audit
from app.services.brokers import stock_coin_trade_gateway as gateway

logger = logging.getLogger(__name__)

KST = planner.KST
REBAL_TAG = "ATR"                 # clientOrderId 태그 ATR{attempt} — 손절(5단계)은 다른 태그를 쓴다
MAX_SYMBOLS_PER_TICK = 6          # 종목당 호출 ≈ 시세 1 + 상태·취소 0~3 + 주문 2 → tick 당 ≤ 36회
FINALIZE_AT = time(15, 20)
LOCK_TTL_SEC = 55


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
        return (await self.db.execute(select(LiveOrder).where(LiveOrder.id == order_id))).scalar_one_or_none()

    async def live_order_by_client_id(self, client_order_id: str) -> LiveOrder | None:
        return (await self.db.execute(select(LiveOrder).where(LiveOrder.client_order_id == client_order_id))).scalar_one_or_none()

    async def open_rebalance_orders(self, user_id: uuid.UUID, symbol: str | None = None) -> list[LiveOrder]:
        stmt = select(LiveOrder).where(LiveOrder.user_id == user_id, LiveOrder.source == LIVE_ORDER_SOURCE_ATR_REBAL,
                                       LiveOrder.status.in_(LIVE_ORDER_OPEN_STATUSES),
                                       LiveOrder.client_order_id.like(f"%:{REBAL_TAG}%"))
        if symbol:
            stmt = stmt.where(LiveOrder.symbol == symbol)
        return list((await self.db.execute(stmt)).scalars().all())

    async def position(self, plan: StrategyPlan, symbol: str) -> StrategyPosition | None:
        return (await self.db.execute(select(StrategyPosition).where(StrategyPosition.plan_id == plan.id,
                                                                     StrategyPosition.symbol == symbol))).scalar_one_or_none()

    async def ledger(self, plan: StrategyPlan) -> dict[str, dict]:
        return await svc.load_ledger(self.db, plan)

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


# ── 장부 반영 ───────────────────────────────────────────────────────────


async def apply_fill(repo: Repo, plan: StrategyPlan, symbol: str, name: str, side: str, qty: int, price: float) -> None:
    if qty <= 0:
        return
    pos = await repo.position(plan, symbol)
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
    """회차에 연결된 live_orders 의 체결 증가분을 장부에 반영. 반영한 수량 합을 돌려준다."""
    applied = 0
    for sl in await repo.slices(run):
        if not sl.live_order_id or sl.status not in ("submitted",):
            continue
        lo = await repo.live_order(sl.live_order_id)
        if lo is None:
            continue
        delta = int(lo.filled_quantity or 0) - int(sl.filled_qty or 0)
        if delta > 0:
            price = float(lo.avg_filled_price or 0) or float(lo.price or 0)
            await apply_fill(repo, plan, sl.symbol, lo.name, sl.side, delta, price)
            sl.filled_qty = int(lo.filled_quantity)
            sl.avg_fill_price = price
            applied += delta
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
    result = []
    for r in run.rows:
        if not r.get("in_portfolio") or r.get("target_qty") is None:
            continue
        held = int((ledger.get(r["symbol"]) or {}).get("quantity") or 0)
        result.append({"symbol": r["symbol"], "name": r.get("name"), "target_qty": r["target_qty"], "ledger_qty": held,
                       "diff": int(r["target_qty"]) - held, "band_skips": int(r.get("band_skips") or 0),
                       "halted": bool(r.get("halted"))})
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
    await cancel_all(repo, plan, run, "수동 중지")
    summary = dict(run.summary or {})
    summary["cancelled_at"] = now.isoformat()
    run.summary = summary
    await finalize(repo, plan, run, now, status="skipped", note="수동 중지")
    await repo.commit()
    await audit(str(user_id), "", "strategy.atr_rebal.rebalance_stop", {"run_id": str(run.id)})
    return run


# ── 회차 처리 ───────────────────────────────────────────────────────────


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
    try:
        bal = await gateway.get_balance()
    except gateway.GatewayError as exc:
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

    done = {(s.side, s.seq, s.symbol) for s in await repo.slices(run) if s.attempt == int(run.attempt or 0)}
    ledger = await repo.ledger(plan)
    rows = copy.deepcopy(run.rows)
    todo = []
    for side in ("SELL", "BUY"):
        rnd = current_round(now, side)
        if rnd is None:
            continue
        seq, rounds_left = rnd
        for row in rows:
            if not row.get("in_portfolio") or row.get("target_qty") is None or row.get("halted"):
                continue
            if (side, seq, row["symbol"]) in done:
                continue
            held = int((ledger.get(row["symbol"]) or {}).get("quantity") or 0)
            # 같은 방향의 열린 주문이 있으면(미체결 재주문) 남은 수량이 0이어도 처리 대상 — 취소는 방향과 무관하게 종목 전체
            has_open = any(o.side == side for o in await repo.open_rebalance_orders(run.user_id, row["symbol"]))
            if remaining_qty(row, side, held, account_qty.get(row["symbol"], 0)) <= 0 and not has_open:
                continue
            todo.append((side, seq, rounds_left, row))

    processed = []
    for side, seq, rounds_left, row in todo[:MAX_SYMBOLS_PER_TICK]:
        sl = await _process_symbol(repo, plan, run, row, side, seq, rounds_left, ctx, now)
        processed.append({"symbol": row["symbol"], "side": side, "seq": seq, "status": sl.status, "qty": sl.quantity, "reason": sl.reason})
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
