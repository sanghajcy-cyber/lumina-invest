"""손절 감시와 장부 동기화 (5단계).

Beat `strategy.stop_loss_watch` 가 거래일 09:05~15:15 매분 호출한다. 리밸런싱 실행 상태(중지·마감 포함)와 무관하게 돌고,
비상 정지(kill switch)일 때만 멈춘다. 한 번의 감시는
  1) 아직 반영 안 된 체결(리밸런싱 회차·손절 주문)을 장부에 반영
  2) 장부 동기화: 계좌 보유수량이 장부보다 적으면(사람이 HTS·앱에서 수동 매도) 장부를 계좌 수량으로 줄인다.
     그 종목에 매도 주문(리밸런싱·손절)이 열려 있으면 체결이 아직 확인되지 않았을 수 있어 이번에는 건너뛴다.
  3) 장부에 수량이 남은 종목마다(목표 종목 + 목표에서 빠져 매도 중인 종목) 현재가(잔고 1회 조회) ≤ 손절가 이면 손절:
     종목 잠금 → 그 종목 리밸런싱 주문(매수·매도) 취소 → 확인 → 체결 반영 → 장부 수량 전량 시장가 매도.
     취소가 확인되지 않으면 waiting 으로 남겨 다음 감시에서 이어서 한다(그동안 리밸런싱은 이 종목을 건드리지 않음).
손절가 = 장부 평균가 − ATR×배수(리밸런싱 날 고정한 ATR), ATR 이 없으면 평균가 × (1 + 손절%).
손절 매도는 risk_guard 일손실 한도·일 주문 수·쿨다운과 전략 일 주문 수 한도에 막히지 않는다.
같은 종목에 손절 주문이 열려 있거나 체결 확인 대기 중이면 새 손절을 내지 않는다. 같은 날 실패가 MAX_STOP_FAILURES 번이면 멈추고 알린다.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, time

from app.models import LIVE_ORDER_OPEN_STATUSES, LIVE_ORDER_SOURCE_ATR_REBAL, StrategyPlan, StrategyStopEvent
from app.services.atr_strategy import executor as ex
from app.services.atr_strategy import planner
from app.services.atr_strategy import service as svc
from app.services.audit import audit
from app.services.brokers import stock_coin_trade_gateway as gateway

logger = logging.getLogger(__name__)

KST = planner.KST
WATCH_START = time(9, 5)
WATCH_END = time(15, 15)          # 15:20 부터 종가 단일가라 시장가가 바로 체결되지 않는다
MAX_STOP_FAILURES = 3
_STOP_TERMINAL = {"FILLED": "filled", "CANCELLED": "failed", "REJECTED": "failed", "ERROR": "failed", "LOST": "failed"}


def evaluate_stop(price: float, avg_price: float, atr: float | None, stop_atr_mult: float, stop_loss_pct: float) -> tuple[bool, float | None, str]:
    """(손절 여부, 손절가, 근거)."""
    sp, basis = planner.stop_price(avg_price, atr, stop_atr_mult, stop_loss_pct)
    if sp is None or price <= 0:
        return False, sp, basis
    return price <= sp, sp, basis


async def apply_stop_fills(repo: ex.Repo, plan: StrategyPlan) -> int:
    applied = 0
    for ev in await repo.pending_stop_events(plan):
        if ev.status == "planned":
            lo = await repo.live_order_by_client_id(ev.client_order_id) if ev.client_order_id else None
            if lo is None:
                ev.status, ev.reason = "failed", "주문 전 중단됨(재시작)"
                continue
            ev.live_order_id, ev.status = lo.id, "submitted"
        lo = await repo.live_order(ev.live_order_id) if ev.live_order_id else None
        if lo is None:
            continue
        delta = int(lo.filled_quantity or 0) - int(ev.filled_qty or 0)
        if delta > 0:
            price = float(lo.avg_filled_price or 0) or float(lo.price or 0) or float(ev.trigger_price or 0)
            await ex.apply_fill(repo, plan, ev.symbol, ev.name, "SELL", delta, price)
            ev.filled_qty = int(lo.filled_quantity)
            ev.avg_fill_price = price
            applied += delta
        if lo.status in _STOP_TERMINAL:
            ev.status = "filled" if ev.filled_qty >= ev.quantity else _STOP_TERMINAL[lo.status]
            if ev.status == "failed" and not ev.reason:
                ev.reason = f"손절 주문 {lo.status} (체결 {ev.filled_qty}/{ev.quantity})"
            if ev.status == "failed":
                await _notify_if_gave_up(repo, plan, ev)
    return applied


async def _notify_if_gave_up(repo: ex.Repo, plan: StrategyPlan, ev: StrategyStopEvent) -> None:
    """그날 그 종목 손절 실패가 MAX_STOP_FAILURES 번째가 되면 한 번 알린다(이후 감시는 그 종목 손절을 멈춘다)."""
    failed = [e for e in await repo.stop_events(plan, ev.trade_date) if e.symbol == ev.symbol and e.status == "failed"]
    if len(failed) == MAX_STOP_FAILURES:
        from app.services import notification
        await notification.notify_order_error(symbol=ev.symbol, side="sell", quantity=ev.quantity, price=ev.trigger_price,
                                              error=f"ATR 전략 손절 주문 {MAX_STOP_FAILURES}회 실패 — 오늘은 더 시도하지 않음, 수동 확인 필요",
                                              user_id=str(plan.user_id))


async def sync_ledger_with_account(repo: ex.Repo, plan: StrategyPlan, account_qty: dict[str, int]) -> list[dict]:
    """장부 수량 = min(장부, 계좌 보유). 줄어든 만큼은 수동 매도로 보고 기록한다. 매도 주문이 열린 종목은 건너뛴다."""
    adjustments = []
    open_sells = {o.symbol for o in await repo.open_rebalance_orders(plan.user_id) if o.side == "SELL"}
    open_sells |= {o.symbol for o in await repo.open_stop_orders(plan.user_id)}
    for pos in await repo.positions(plan):
        q = int(pos.quantity or 0)
        if q <= 0 or pos.symbol in open_sells:
            continue
        acct = int(account_qty.get(pos.symbol, 0))
        if acct < q:
            pos.quantity = acct
            if acct == 0:
                pos.avg_price = 0.0
            adjustments.append({"symbol": pos.symbol, "name": pos.name, "ledger_before": q, "account": acct, "manual_sold": q - acct})
    for a in adjustments:
        logger.warning("ATR 전략 장부 동기화(수동 매도로 판단) plan=%s %s", plan.id, a)
        await audit(str(plan.user_id), "", "strategy.atr_rebal.ledger_sync", a)
    return adjustments


async def _cancel_rebalance_orders(repo: ex.Repo, user_id: uuid.UUID, symbol: str) -> bool:
    ok = True
    for lo in await repo.open_rebalance_orders(user_id, symbol):
        if not await ex.cancel_and_confirm(lo):
            ok = False
        lo.message = (lo.message + " · " if lo.message else "") + "손절로 취소"
    return ok


async def _execute_stop(repo: ex.Repo, plan: StrategyPlan, ev: StrategyStopEvent, account_qty: dict[str, int], now: datetime) -> None:
    from app.services import auto_trade

    if not await _cancel_rebalance_orders(repo, plan.user_id, ev.symbol):
        ev.status, ev.reason = "waiting", "같은 종목 리밸런싱 주문 취소 확인 대기"
        return
    run = await repo.today_run(plan, ev.trade_date)
    if run is not None:
        await ex.sync_fills(repo, plan, run)
    await ex.sync_all_pending(repo, plan)
    await repo.flush()
    held = int(((await repo.ledger(plan)).get(ev.symbol) or {}).get("quantity") or 0)
    qty = min(held, int(account_qty.get(ev.symbol, 0)))
    if qty <= 0:
        ev.status, ev.reason = "skipped", f"매도 가능 수량 없음 (장부 {held}, 계좌 {account_qty.get(ev.symbol, 0)})"
        return
    ev.quantity = qty
    ev.status = "planned"
    ev.client_order_id = gateway.make_client_order_id(plan.user_id, ev.symbol, "sell", now=now, tag=ex.STOP_TAG, seq=ev.seq)
    await repo.commit()                     # 주문 전에 기록 — 재시작 시 같은 손절을 두 번 내지 않는다
    res = await auto_trade._place_live_order_via_gateway(
        repo.db, None, ev.symbol, ev.name or ev.symbol, "sell", qty, float(ev.trigger_price), str(plan.user_id),
        source=LIVE_ORDER_SOURCE_ATR_REBAL, order_type="MARKET", client_order_id=ev.client_order_id,
    )
    lo = await repo.live_order_by_client_id(ev.client_order_id)
    ev.live_order_id = lo.id if lo is not None else None
    if res.get("status") == "submitted" or (lo is not None and lo.status == "UNKNOWN"):
        ev.status = "submitted"             # UNKNOWN: confirm_fills 가 멱등 재전송·확인
    else:
        ev.status = "failed"
        ev.reason = str(res.get("reason") or res.get("error") or res.get("code") or res.get("status"))[:300]


async def watch_plan(repo: ex.Repo, plan: StrategyPlan, now: datetime) -> dict:
    broker_row = await repo.broker_row(plan.user_id)
    if broker_row is not None and broker_row.risk_kill_switch:
        return {"skipped": "kill_switch"}
    today = now.date()
    await apply_stop_fills(repo, plan)
    await ex.sync_all_pending(repo, plan)
    await repo.flush()
    try:
        bal = await gateway.get_balance()
    except gateway.GatewayError as exc:
        await repo.commit()
        return {"skipped": f"잔고 조회 실패 [{exc.code}]"}
    account_qty: dict[str, int] = {}
    prices: dict[str, float] = {}
    for h in bal.get("holdings") or []:
        try:
            code = gateway.normalize_symbol(str(h.get("symbol") or ""))
        except gateway.GatewayError:
            continue
        account_qty[code] = int(h.get("quantity") or 0)
        prices[code] = float(h.get("currentPrice") or 0)
    adjustments = await sync_ledger_with_account(repo, plan, account_qty)
    await repo.flush()

    # 대상 = 장부 수량이 남은 모든 종목. 목표에서 빠진 종목도 매도가 끝날 때까지 감시한다. 장부에 없는 계좌 종목은 대상이 아니다.
    events = await repo.stop_events(plan, today)
    out = {"adjustments": adjustments, "stops": []}
    for pos in await repo.positions(plan):
        sym = pos.symbol
        if int(pos.quantity or 0) <= 0:
            continue
        mine = [e for e in events if e.symbol == sym]
        if await repo.open_stop_orders(plan.user_id, sym) or any(e.status in ("submitted", "planned") for e in mine):
            continue                        # 손절 주문이 열려 있거나 체결 확인 대기 중 — 새 손절을 내지 않는다
        failures = sum(1 for e in mine if e.status == "failed")
        if failures >= MAX_STOP_FAILURES:
            continue
        waiting = next((e for e in mine if e.status == "waiting"), None)
        price = prices.get(sym, 0.0)
        hit, sp, basis = evaluate_stop(price, float(pos.avg_price or 0), pos.atr, plan.stop_atr_mult, plan.stop_loss_pct)
        if not hit and waiting is None:
            continue
        if not await ex.acquire_symbol_lock(plan.environment, sym):
            out["stops"].append({"symbol": sym, "status": "deferred", "reason": "리밸런싱이 이 종목을 처리 중"})
            continue
        try:
            ev = waiting
            if ev is None:
                ev = StrategyStopEvent(plan_id=plan.id, user_id=plan.user_id, trade_date=today, symbol=sym, name=pos.name or "",
                                       seq=len(mine), status="waiting", trigger_price=price, avg_price=float(pos.avg_price or 0),
                                       atr=pos.atr, stop_price=round(sp or 0, 2), basis=basis, quantity=0, client_order_id="",
                                       filled_qty=0, avg_fill_price=0.0, reason="")
                repo.add(ev)
                events.append(ev)
                await repo.commit()         # 손절 사실을 먼저 남긴다 → 리밸런싱 tick 이 즉시 이 종목을 건너뛴다
                logger.warning("ATR 전략 손절 조건 충족 plan=%s %s 현재가 %.0f ≤ 손절가 %.0f (%s)", plan.id, sym, price, sp or 0, basis)
            else:
                ev.trigger_price = price or ev.trigger_price
            await _execute_stop(repo, plan, ev, account_qty, now)
            if ev.status == "failed":
                await _notify_if_gave_up(repo, plan, ev)
            await repo.commit()
            out["stops"].append({"symbol": sym, "status": ev.status, "qty": ev.quantity, "stop_price": ev.stop_price,
                                 "price": price, "reason": ev.reason})
            await audit(str(plan.user_id), "", "strategy.atr_rebal.stop_loss",
                        {"symbol": sym, "status": ev.status, "qty": ev.quantity, "price": price, "stop_price": ev.stop_price, "basis": basis})
        finally:
            await ex.release_symbol_lock(plan.environment, sym)
    await repo.commit()
    return out


def in_watch_window(now: datetime) -> bool:
    return WATCH_START <= now.astimezone(KST).time() <= WATCH_END


async def stop_loss_watch(now: datetime | None = None, session_factory=None) -> dict:
    """Beat 진입점. 실행 승인 전·거래일 아님·감시 시간 밖이면 아무것도 하지 않는다. 리밸런싱 중지와 무관하게 돈다."""
    now = (now or ex.now_kst()).astimezone(KST)
    if not svc.LIVE_EXECUTION_AVAILABLE:
        return {"skipped": "live_execution_not_approved"}
    if not gateway.is_krx_trading_day(now.date()):
        return {"skipped": "not_trading_day"}
    if not in_watch_window(now):
        return {"skipped": "outside_window"}
    if session_factory is None:
        from app.database.postgres import get_session_factory
        session_factory = get_session_factory()
    results = {}
    async with session_factory() as db:
        repo = ex.Repo(db)
        for plan in await repo.live_plans():
            key = f"strategy:stop-watch-lock:kis:{plan.environment}"
            if not await ex._acquire_lock(key):
                results[str(plan.id)] = {"skipped": "locked"}
                continue
            try:
                results[str(plan.id)] = await watch_plan(repo, plan, now)
            except Exception as exc:
                logger.exception("ATR 전략 손절 감시 실패 plan=%s", plan.id)
                await db.rollback()
                results[str(plan.id)] = {"error": str(exc)[:200]}
            finally:
                await ex._release_lock(key)
    return results
