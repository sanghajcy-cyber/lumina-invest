"""월초 ATR 리밸런싱 전략 — DB·게이트웨이·KIS 시세를 묶는 진입점.

3단계 범위: 플랜 설정, 목표 파일 업로드, ATR 계산, 계획 수립(dry-run), 활성화 규칙, 기존 auto_trade 차단.
주문 실행(시분할·손절)은 4·5단계. 그 전까지 dry_run=False 로는 켤 수 없다(LIVE_EXECUTION_AVAILABLE).

auto_trade 차단 규칙: KIS 모의계좌는 게이트웨이 계정 하나를 모든 사용자가 같이 쓰므로, 어떤 사용자의 플랜이든
is_active ∧ ¬dry_run 이면 broker=kis ∧ quant_mode=live 인 auto_trade(시스템 배치 포함)를 모두 끄고 다시 켜지 못하게 한다.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BrokerSettings, StrategyPlan, StrategyPosition, StrategyRun, StrategyTarget
from app.services.atr_strategy import planner
from app.services.atr_strategy.targets import TargetFileError, parse_target_file
from app.services.audit import audit
from app.services.brokers import stock_coin_trade_gateway as gateway

logger = logging.getLogger(__name__)

LIVE_EXECUTION_AVAILABLE = False      # 4단계(시분할 주문 실행)가 들어오면 True
MAX_TICKERS_LIMIT = 20                # st 잔고 연속조회가 생기기 전까지의 상한
GATEWAY_MAX_ORDER_AMOUNT_PAPER = 10_000_000   # st KIS_PAPER_MAX_ORDER_AMOUNT 기본값
DAILY_CANDLES = 60                    # Wilder ATR(14) 수렴용으로 넉넉히


class StrategyError(Exception):
    """사용자에게 그대로 보여 줄 오류."""


def kst_today() -> date:
    return datetime.now(planner.KST).date()


# ── 플랜 ────────────────────────────────────────────────────────────────


async def get_plan(db: AsyncSession, user_id: uuid.UUID, create: bool = True) -> StrategyPlan | None:
    row = (await db.execute(select(StrategyPlan).where(StrategyPlan.user_id == user_id))).scalar_one_or_none()
    if row is None and create:
        row = StrategyPlan(user_id=user_id)
        db.add(row)
        await db.flush()
    return row


_RANGES = {
    "invest_amount": (1_000, 100_000_000_000, float),
    "max_tickers": (1, MAX_TICKERS_LIMIT, int),
    "atr_period": (2, 100, int),
    "buy_band_atr_mult": (0.1, 10, float),
    "stop_atr_mult": (0.1, 10, float),
    "stop_loss_pct": (-50, -0.1, float),
    "max_order_amount": (10_000, GATEWAY_MAX_ORDER_AMOUNT_PAPER, float),
    "max_orders_per_day": (1, 10_000, int),
}


def apply_plan_update(plan: StrategyPlan, data: dict) -> StrategyPlan:
    if data.get("name"):
        plan.name = str(data["name"])[:60]
    for key, (lo, hi, cast) in _RANGES.items():
        if key not in data or data[key] is None:
            continue
        try:
            v = cast(data[key])
        except (TypeError, ValueError):
            raise StrategyError(f"{key} 값이 숫자가 아닙니다.")
        if not lo <= v <= hi:
            raise StrategyError(f"{key} 는 {lo:,}~{hi:,} 사이여야 합니다.")
        setattr(plan, key, v)
    return plan


def plan_to_dict(plan: StrategyPlan) -> dict:
    keys = ["name", "strategy", "environment", "is_active", "dry_run", "target_source", "target_file_name", *_RANGES]
    out = {k: getattr(plan, k) for k in keys}
    out["id"] = str(plan.id)
    out["targets_uploaded_at"] = plan.targets_uploaded_at.isoformat() if plan.targets_uploaded_at else None
    out["schedule"] = {"interval_min": planner.SLICE_INTERVAL_MIN, "start": planner.SLICE_START.isoformat("minutes"),
                       "sell_last": planner.SELL_LAST_SLICE.isoformat("minutes"), "buy_last": planner.BUY_LAST_SLICE.isoformat("minutes"),
                       "cancel_all": planner.CANCEL_ALL_AT.isoformat("minutes"),
                       "sell_rounds": planner.SELL_ROUNDS, "buy_rounds": planner.BUY_ROUNDS}
    return out


# ── 목표 ────────────────────────────────────────────────────────────────


async def list_targets(db: AsyncSession, plan: StrategyPlan) -> list[StrategyTarget]:
    return list((await db.execute(select(StrategyTarget).where(StrategyTarget.plan_id == plan.id)
                                  .order_by(StrategyTarget.position))).scalars().all())


def target_to_dict(t: StrategyTarget) -> dict:
    return {"symbol": t.symbol, "name": t.name, "weight": t.weight, "weight_input": t.weight_input, "atr_input": t.atr_input}


async def replace_targets(db: AsyncSession, plan: StrategyPlan, filename: str, content: bytes) -> list[dict]:
    try:
        parsed = parse_target_file(content, filename, max_tickers=min(int(plan.max_tickers), MAX_TICKERS_LIMIT))
    except TargetFileError as exc:
        raise StrategyError(str(exc)) from exc
    await db.execute(delete(StrategyTarget).where(StrategyTarget.plan_id == plan.id))
    for t in parsed:
        db.add(StrategyTarget(plan_id=plan.id, **t))
    plan.target_file_name = (filename or "")[:200]
    plan.targets_uploaded_at = datetime.now(timezone.utc)
    await db.flush()
    return parsed


# ── 장부·계좌·시세 ──────────────────────────────────────────────────────


async def load_ledger(db: AsyncSession, plan: StrategyPlan) -> dict[str, dict]:
    rows = (await db.execute(select(StrategyPosition).where(StrategyPosition.plan_id == plan.id))).scalars().all()
    return {p.symbol: {"quantity": int(p.quantity), "avg_price": float(p.avg_price), "name": p.name, "atr": p.atr,
                       "atr_source": p.atr_source, "atr_as_of": p.atr_as_of.isoformat() if p.atr_as_of else None}
            for p in rows if p.quantity > 0}


async def account_snapshot() -> tuple[float, dict[str, int]]:
    """게이트웨이 잔고 → (예수금, {6자리 코드: 보유수량}). cashBalance 는 st 기준 예수금총액(dnca_tot_amt)."""
    if not gateway.is_configured():
        raise StrategyError("stock-coin-trade 게이트웨이(STOCK_COIN_TRADE_BASE_URL·API_KEY)가 설정되지 않아 계좌를 읽을 수 없습니다.")
    try:
        bal = await gateway.get_balance()
    except gateway.GatewayError as exc:
        raise StrategyError(f"KIS 잔고 조회 실패: [{exc.code}] {exc}") from exc
    holdings: dict[str, int] = {}
    for h in bal.get("holdings") or []:
        try:
            code = gateway.normalize_symbol(str(h.get("symbol") or ""))
        except gateway.GatewayError:
            continue
        qty = int(h.get("quantity") or 0)
        if qty > 0:
            holdings[code] = holdings.get(code, 0) + qty
    return float(bal.get("cashBalance") or 0), holdings


async def fetch_market(targets: list[dict], today: date, period: int) -> tuple[dict[str, dict], dict[str, dict]]:
    """종목당 KIS 일봉 1회 호출로 (시세, ATR) 를 함께 얻는다. 파일 ATR 이 있으면 그 값이 우선."""
    from app.services import kis_market_data as kmd
    quotes: dict[str, dict] = {}
    atrs: dict[str, dict] = {}
    for t in targets:
        sym = t["symbol"]
        file_atr = t.get("atr_input")
        try:
            raw = await kmd.fetch_daily(sym, DAILY_CANDLES)
        except Exception as exc:
            logger.warning("ATR 전략 시세 조회 실패 %s: %s", sym, exc)
            quotes[sym] = {"error": str(exc)[:200]}
            atrs[sym] = ({"atr": float(file_atr), "source": "file", "as_of": None} if file_atr
                         else {"atr": None, "source": "none", "as_of": None, "error": f"일봉 조회 실패: {str(exc)[:150]}"})
            continue
        s = raw.get("summary") or {}
        quotes[sym] = {"price": s.get("price"), "open": s.get("open"), "name": s.get("name") or ""}
        if file_atr:
            atrs[sym] = {"atr": float(file_atr), "source": "file", "as_of": None}
            continue
        try:
            value, as_of = planner.atr_from_daily(raw.get("candles") or [], today, period)
            atrs[sym] = {"atr": round(value, 2), "source": "kis", "as_of": as_of.isoformat()}
        except planner.AtrUnavailable as exc:
            atrs[sym] = {"atr": None, "source": "none", "as_of": None, "error": str(exc)}
    return quotes, atrs


# ── 계획 수립 ───────────────────────────────────────────────────────────


async def compute_plan(db: AsyncSession, plan: StrategyPlan, user_id: uuid.UUID, kind: str, today: date) -> dict:
    """잔고·장부·시세·ATR 로 종목별 계획을 계산한다(DB 기록 없음). StrategyRun 필드 dict 를 돌려준다."""
    targets = [target_to_dict(t) for t in await list_targets(db, plan)]
    if not targets:
        raise StrategyError("목표 포트폴리오가 비어 있습니다. CSV/엑셀 파일을 먼저 업로드하세요.")

    warnings: list[str] = []
    if not gateway.krx_holidays_known(today.year):
        msg = f"{today.year}년 KRX 휴장일 목록이 없습니다 (KRX_EXTRA_HOLIDAYS 에 추가 필요). 첫 거래일 판단이 틀릴 수 있습니다."
        if kind == "rebalance":
            raise StrategyError(msg)
        warnings.append(msg)
    first_day = gateway.first_trading_day_of_month(today.year, today.month)
    if kind == "dry_run" and today != first_day:
        warnings.append(f"오늘({today})은 리밸런싱 날이 아닙니다. 이번 달 첫 거래일은 {first_day} — 실제 실행은 그날만 합니다.")

    cash, account_qty = await account_snapshot()
    ledger = await load_ledger(db, plan)
    quotes, atr_info = await fetch_market(targets, today, int(plan.atr_period))
    ledger_eval = planner.ledger_eval_for(targets, ledger, quotes)
    budget, budget_warn = planner.effective_budget(plan.invest_amount, ledger_eval, cash)
    if budget_warn:
        logger.warning("ATR 전략 투자금액 축소 user=%s: %s", user_id, budget_warn[0])
    warnings += budget_warn
    rows, row_warn = planner.build_rows(
        targets, ledger, account_qty, quotes, atr_info, budget,
        stop_atr_mult=plan.stop_atr_mult, stop_loss_pct=plan.stop_loss_pct,
        buy_band_atr_mult=plan.buy_band_atr_mult, max_order_amount=plan.max_order_amount,
    )
    warnings += row_warn
    no_atr = [r["symbol"] for r in rows if r["in_portfolio"] and r["atr_source"] == "none"]
    if no_atr:
        warnings.append(f"ATR 없음(% 손절 {plan.stop_loss_pct:g}%로 대체, 매수 범위 제한 없음): {', '.join(no_atr)}")
    summary = {**planner.summarize(rows), "first_trading_day": first_day.isoformat(), "is_rebalance_day": today == first_day}
    return dict(invest_amount=float(plan.invest_amount), effective_budget=round(budget, 2), cash_balance=round(cash, 2),
                ledger_eval=round(ledger_eval, 2), warnings=warnings, rows=rows, summary=summary,
                note=f"{plan.target_file_name} · 목표 {len(targets)}종목"[:300])


async def build_plan(db: AsyncSession, user_id: uuid.UUID, kind: str = "dry_run", today: date | None = None) -> StrategyRun:
    """계획을 계산해 StrategyRun(status=planned)으로 남긴다(주문 없음). 커밋은 호출자."""
    if kind not in ("dry_run", "rebalance"):
        raise StrategyError(f"알 수 없는 실행 종류: {kind}")
    today = today or kst_today()
    plan = await get_plan(db, user_id, create=False)
    if plan is None:
        raise StrategyError("전략 플랜이 없습니다. 먼저 설정을 저장하세요.")
    fields = await compute_plan(db, plan, user_id, kind, today)
    run = StrategyRun(plan_id=plan.id, user_id=user_id, run_date=today, kind=kind, status="planned", **fields)
    db.add(run)
    await db.flush()
    return run


def run_to_dict(run: StrategyRun) -> dict:
    return {"id": str(run.id), "run_date": run.run_date.isoformat(), "kind": run.kind, "status": run.status,
            "invest_amount": run.invest_amount, "effective_budget": run.effective_budget, "cash_balance": run.cash_balance,
            "ledger_eval": run.ledger_eval, "warnings": run.warnings, "rows": run.rows, "summary": run.summary, "note": run.note,
            "trigger": getattr(run, "trigger", None), "attempt": getattr(run, "attempt", 0),
            "started_at": run.started_at.isoformat() if getattr(run, "started_at", None) else None,
            "finished_at": run.finished_at.isoformat() if getattr(run, "finished_at", None) else None,
            "created_at": run.created_at.isoformat() if run.created_at else None}


async def list_runs(db: AsyncSession, user_id: uuid.UUID, limit: int = 20) -> list[dict]:
    rows = (await db.execute(select(StrategyRun).where(StrategyRun.user_id == user_id)
                             .order_by(StrategyRun.created_at.desc()).limit(limit))).scalars().all()
    return [{k: v for k, v in run_to_dict(r).items() if k != "rows"} for r in rows]


async def get_run(db: AsyncSession, user_id: uuid.UUID, run_id: uuid.UUID) -> StrategyRun:
    row = (await db.execute(select(StrategyRun).where(StrategyRun.id == run_id, StrategyRun.user_id == user_id))).scalar_one_or_none()
    if row is None:
        raise StrategyError("실행 기록을 찾을 수 없습니다.")
    return row


# ── 활성화 · auto_trade 차단 ────────────────────────────────────────────


async def auto_trade_block_reason(db: AsyncSession) -> str | None:
    """실주문 모드로 켜진 ATR 전략이 있으면 그 사유, 없으면 None."""
    row = (await db.execute(select(StrategyPlan).where(StrategyPlan.is_active.is_(True), StrategyPlan.dry_run.is_(False))
                            .limit(1))).scalar_one_or_none()
    if row is None:
        return None
    return f"월초 ATR 리밸런싱 전략이 같은 KIS 모의계좌에서 실행 중입니다 (plan={row.id})."


def is_live_kis_row(row: BrokerSettings | None) -> bool:
    return bool(row and (row.broker or "").strip().lower() == "kis" and row.quant_mode == "live")


async def force_off_live_kis_auto_trade(db: AsyncSession, reason: str) -> list[str]:
    """broker=kis ∧ quant_mode=live 로 켜진 auto_trade 를 모두 끈다. 꺼진 user_id 목록 반환."""
    rows = (await db.execute(select(BrokerSettings).where(BrokerSettings.quant_auto_enabled.is_(True)))).scalars().all()
    stopped = []
    for row in rows:
        if is_live_kis_row(row):
            row.quant_auto_enabled = False
            stopped.append(str(row.user_id))
    if stopped:
        await db.commit()
        for uid in stopped:
            await audit(uid, "", "auto_trade.blocked_by_strategy", {"reason": reason})
        logger.warning("ATR 전략 실행 중 — 같은 KIS 계좌의 auto_trade %d건 해제: %s", len(stopped), stopped)
    return stopped


async def set_active(db: AsyncSession, user_id: uuid.UUID, active: bool, dry_run: bool = True) -> dict:
    plan = await get_plan(db, user_id, create=False)
    if plan is None:
        raise StrategyError("전략 플랜이 없습니다.")
    stopped: list[str] = []
    if active:
        if not await list_targets(db, plan):
            raise StrategyError("목표 포트폴리오가 비어 있어 켤 수 없습니다.")
        if not dry_run:
            if not LIVE_EXECUTION_AVAILABLE:
                raise StrategyError("주문 실행(4단계)이 아직 없어 dry-run 으로만 켤 수 있습니다.")
            other = (await db.execute(select(StrategyPlan).where(StrategyPlan.is_active.is_(True), StrategyPlan.dry_run.is_(False),
                                                                StrategyPlan.user_id != user_id).limit(1))).scalar_one_or_none()
            if other is not None:
                raise StrategyError("다른 사용자의 ATR 전략이 같은 KIS 모의계좌에서 이미 실행 중입니다.")
    warnings: list[str] = []
    if not active and plan.is_active and not plan.dry_run:
        warnings = await deactivate_warnings(db, plan)
    plan.is_active = bool(active)
    plan.dry_run = bool(dry_run) if active else plan.dry_run
    await db.commit()
    if active and not dry_run:
        stopped = await force_off_live_kis_auto_trade(db, "ATR 전략 활성화")
    await audit(str(user_id), "", "strategy.atr_rebal." + ("activate" if active else "deactivate"),
                {"dry_run": plan.dry_run, "stopped_auto_trade": stopped, "warnings": warnings})
    return {"plan": plan_to_dict(plan), "stopped_auto_trade": stopped, "warnings": warnings}


async def deactivate_warnings(db: AsyncSession, plan: StrategyPlan) -> list[str]:
    """실주문 모드 전략을 끌 때 남는 위험: 손절 감시 없이 남는 장부 종목, 정리되지 않는 열린 리밸런싱 주문."""
    from app.models import LIVE_ORDER_OPEN_STATUSES, LIVE_ORDER_SOURCE_ATR_REBAL, LiveOrder
    out = []
    ledger = await load_ledger(db, plan)
    if ledger:
        names = ", ".join(f"{s} {v.get('name') or ''} {v['quantity']}주".strip() for s, v in sorted(ledger.items()))
        out.append(f"장부에 보유 종목 {len(ledger)}개가 손절 감시 없이 남습니다: {names}")
    open_orders = (await db.execute(select(LiveOrder).where(
        LiveOrder.user_id == plan.user_id, LiveOrder.source == LIVE_ORDER_SOURCE_ATR_REBAL,
        LiveOrder.status.in_(LIVE_ORDER_OPEN_STATUSES)))).scalars().all()
    if open_orders:
        out.append(f"열린 전략 주문 {len(open_orders)}건은 자동으로 취소·정리되지 않습니다. "
                   "끄기 전에 /rebalance/stop 으로 취소하거나 KIS 에서 직접 확인하세요.")
    return out
