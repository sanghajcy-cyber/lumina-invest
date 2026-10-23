"""월초 ATR 리밸런싱 전략 API — /api/strategy/atr

플랜 설정 · 목표 파일(CSV/xlsx) 업로드 · dry-run 계획(plan_*.csv 형식 내려받기) · 활성화/해제 ·
리밸런싱 수동 시작(--rebalance)·재실행(--force)·중지·오늘 진행 상황.
실제 모의주문은 service.LIVE_EXECUTION_AVAILABLE 승인 전까지 막혀 있다.
"""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.postgres import get_pg_session
from app.lib.jwt_auth import get_current_user_any
from app.services.atr_strategy import executor, planner
from app.services.atr_strategy import service as svc

router = APIRouter(prefix="/api/strategy/atr", tags=["atr-strategy"])
logger = logging.getLogger(__name__)

MAX_UPLOAD_BYTES = 1_000_000


def _uid(user: dict) -> uuid.UUID:
    try:
        return uuid.UUID(str(user["id"]))
    except Exception:
        raise HTTPException(400, "유효하지 않은 사용자 ID입니다.")


class PlanBody(BaseModel):
    name: str | None = None
    invest_amount: float | None = Field(None, description="투자금액(원). 계좌 예수금+장부 평가액보다 크면 실행 시 줄어든다")
    max_tickers: int | None = Field(None, description=f"종목 수 상한(1~{svc.MAX_TICKERS_LIMIT})")
    atr_period: int | None = None
    buy_band_atr_mult: float | None = None
    stop_atr_mult: float | None = None
    stop_loss_pct: float | None = Field(None, description="ATR 없을 때 손절 기준(%). 예: -10")
    max_order_amount: float | None = None
    max_orders_per_day: int | None = Field(None, description="이 전략 전용 일 주문 수 한도(risk_guard 와 별도)")


class ActivateBody(BaseModel):
    dry_run: bool = True


class RebalanceStartBody(BaseModel):
    force: bool = Field(False, description="오늘 실행이 있어도 열린 주문을 취소하고 현재 장부 기준으로 처음부터 다시(참고 구현 --force)")


def _slice_dict(sl) -> dict:
    return {"symbol": sl.symbol, "side": sl.side, "seq": sl.seq, "attempt": sl.attempt, "quantity": sl.quantity,
            "price": sl.price, "status": sl.status, "reason": sl.reason, "filled_qty": sl.filled_qty,
            "client_order_id": sl.client_order_id, "created_at": sl.created_at.isoformat() if sl.created_at else None}


async def _plan_payload(db: AsyncSession, uid: uuid.UUID) -> dict:
    plan = await svc.get_plan(db, uid)
    await db.commit()
    targets = [svc.target_to_dict(t) for t in await svc.list_targets(db, plan)]
    ledger = await svc.load_ledger(db, plan)
    return {"plan": svc.plan_to_dict(plan), "targets": targets,
            "positions": [{"symbol": k, **v} for k, v in sorted(ledger.items())],
            "auto_trade_block_reason": await svc.auto_trade_block_reason(db)}


@router.get("/plan")
async def get_plan(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    return await _plan_payload(db, _uid(user))


@router.put("/plan")
async def put_plan(body: PlanBody, user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    uid = _uid(user)
    plan = await svc.get_plan(db, uid)
    try:
        svc.apply_plan_update(plan, body.model_dump(exclude_none=True))
    except svc.StrategyError as exc:
        raise HTTPException(400, str(exc))
    await db.commit()
    return await _plan_payload(db, uid)


@router.post("/targets")
async def upload_targets(file: UploadFile = File(...), user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    """목표 포트폴리오 파일로 목표를 통째로 바꾼다. 컬럼: 종목코드, 종목명, 비중, ATR."""
    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"파일이 너무 큽니다 (최대 {MAX_UPLOAD_BYTES // 1000}KB).")
    uid = _uid(user)
    plan = await svc.get_plan(db, uid)
    try:
        parsed = await svc.replace_targets(db, plan, file.filename or "", content)
    except svc.StrategyError as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))
    await db.commit()
    return {"ok": True, "count": len(parsed), "targets": parsed}


@router.get("/targets")
async def get_targets(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    plan = await svc.get_plan(db, _uid(user))
    await db.commit()
    return {"targets": [svc.target_to_dict(t) for t in await svc.list_targets(db, plan)]}


@router.post("/dry-run")
async def dry_run(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    """주문 없이 오늘 기준 계획을 만든다(잔고·장부·KIS 일봉 ATR·실효 투자금액)."""
    try:
        run = await svc.build_plan(db, _uid(user), "dry_run")
    except svc.StrategyError as exc:
        await db.rollback()
        logger.warning("ATR 전략 dry-run 거부 user=%s: %s", user.get("id"), exc)
        raise HTTPException(409, str(exc))
    await db.commit()
    await db.refresh(run)
    return svc.run_to_dict(run)


@router.get("/runs")
async def list_runs(limit: int = Query(20, ge=1, le=100), user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    return {"runs": await svc.list_runs(db, _uid(user), limit)}


def _run_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise HTTPException(400, "run_id 형식이 올바르지 않습니다.")


@router.get("/runs/{run_id}")
async def get_run(run_id: str, user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    try:
        return svc.run_to_dict(await svc.get_run(db, _uid(user), _run_id(run_id)))
    except svc.StrategyError as exc:
        raise HTTPException(404, str(exc))


@router.get("/runs/{run_id}/plan.csv")
async def get_run_csv(run_id: str, user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    """참고 구현 state/plan_YYYYMMDD.csv 와 같은 앞 10개 컬럼 + 추가 컬럼 (UTF-8 BOM)."""
    try:
        run = await svc.get_run(db, _uid(user), _run_id(run_id))
    except svc.StrategyError as exc:
        raise HTTPException(404, str(exc))
    filename = f"plan_{run.run_date:%Y%m%d}_{run.kind}.csv"
    return Response(planner.to_plan_csv(run.rows), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.post("/activate")
async def activate(body: ActivateBody = ActivateBody(), user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    try:
        return await svc.set_active(db, _uid(user), True, dry_run=body.dry_run)
    except svc.StrategyError as exc:
        await db.rollback()
        logger.warning("ATR 전략 활성화 거부 user=%s dry_run=%s: %s", user.get("id"), body.dry_run, exc)
        raise HTTPException(409, str(exc))


@router.post("/deactivate")
async def deactivate(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    try:
        return await svc.set_active(db, _uid(user), False)
    except svc.StrategyError as exc:
        raise HTTPException(409, str(exc))


@router.post("/rebalance/start")
async def rebalance_start(body: RebalanceStartBody = RebalanceStartBody(), user=Depends(get_current_user_any),
                          db: AsyncSession = Depends(get_pg_session)):
    """첫 거래일이 아니어도 오늘 리밸런싱을 시작한다(처음 매수·월중 포트폴리오 변경). 이후 회차는 Beat tick 이 진행.

    오늘 실행이 이미 있으면 그대로 이어받는다. force=true 면 열린 주문을 취소하고 장부 기준으로 다시 계획한다.
    """
    try:
        run = await executor.start_rebalance(db, _uid(user), manual=True, force=body.force)
    except svc.StrategyError as exc:
        await db.rollback()
        logger.warning("ATR 전략 /rebalance/start 거부 user=%s force=%s: %s", user.get("id"), body.force, exc)
        raise HTTPException(409, str(exc))
    return svc.run_to_dict(run)


@router.get("/rebalance/today")
async def rebalance_today(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    repo = executor.Repo(db)
    plan = await repo.plan_for(_uid(user))
    run = await repo.today_run(plan, executor.now_kst().date()) if plan else None
    if run is None:
        return {"run": None, "slices": []}
    slices = sorted(await repo.slices(run), key=lambda s: (s.attempt, s.seq, s.side, s.symbol))
    return {"run": svc.run_to_dict(run), "slices": [_slice_dict(s) for s in slices]}


@router.post("/rebalance/stop")
async def rebalance_stop(user=Depends(get_current_user_any), db: AsyncSession = Depends(get_pg_session)):
    """오늘 리밸런싱을 멈춘다(열린 주문 취소 → skipped). 다시 하려면 /rebalance/start force=true."""
    try:
        run = await executor.stop_rebalance(db, _uid(user))
    except svc.StrategyError as exc:
        await db.rollback()
        logger.warning("ATR 전략 /rebalance/stop 거부 user=%s: %s", user.get("id"), exc)
        raise HTTPException(409, str(exc))
    return svc.run_to_dict(run)
