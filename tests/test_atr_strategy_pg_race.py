"""실제 PostgreSQL 에서 체결 이중 반영 경합 재현 (2026-10-23 첫 모의 운영 KB금융 장부 9주 사고).

두 DB 세션(손절 감시·리밸런싱 tick 프로세스 역할)이 같은 미반영 체결을 동시에 읽은 뒤 반영한다.
claim_fill(UPDATE … WHERE filled_qty = old)과 장부 행 SELECT … FOR UPDATE 로 한 번만 반영돼야 한다.

빈 테스트용 DB 가 필요해 기본은 건너뛴다:
  ATR_PG_TEST_URL=postgresql+asyncpg://user:pass@host:5432/<빈 테스트 DB> pytest tests/test_atr_strategy_pg_race.py
테이블을 만들고 끝나면 지운다. 운영 DB 를 가리키지 말 것.
"""
import asyncio
import os
import uuid
from datetime import date

import pytest

URL = os.environ.get("ATR_PG_TEST_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="ATR_PG_TEST_URL 이 없으면 실제 PostgreSQL 경합 테스트를 건너뛴다")


def test_two_sessions_apply_same_fill_once():
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.models import Base, LiveOrder, StrategyPlan, StrategyPosition, StrategyRun, StrategySlice, User
    from app.services.atr_strategy import executor as ex

    async def main():
        engine = create_async_engine(URL)
        assert "fin_ai" not in URL.rsplit("/", 1)[-1], "운영 DB 이름처럼 보인다 — 빈 테스트 DB 를 지정할 것"
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        uid = uuid.uuid4()
        try:
            async with factory() as db:
                db.add(User(id=uid, email=f"race-{uid.hex[:8]}@test.local", name="race", password_hash="x", client_id=uid.hex[:16]))
                await db.flush()
                plan = StrategyPlan(user_id=uid)
                db.add(plan)
                await db.flush()
                run = StrategyRun(plan_id=plan.id, user_id=uid, run_date=date(2026, 11, 2), kind="rebalance", status="running",
                                  rows=[], warnings=[], summary={})
                db.add(run)
                db.add(StrategyPosition(plan_id=plan.id, symbol="105560", name="KB금융", quantity=0, avg_price=0.0))
                lo = LiveOrder(user_id=uid, client_order_id=f"race:{uid.hex[:8]}", symbol="105560", name="KB금융", side="BUY",
                               order_type="LIMIT", quantity=1, price=152_600.0, status="FILLED", filled_quantity=1,
                               avg_filled_price=152_600.0, source="ATR_REBAL")
                db.add(lo)
                await db.flush()
                db.add(StrategySlice(run_id=run.id, plan_id=plan.id, symbol="105560", side="BUY", seq=0, attempt=0,
                                     status="submitted", quantity=1, price=152_600.0, live_order_id=lo.id, filled_qty=0))
                await db.commit()
                plan_id = plan.id

            arrived, go = [0], asyncio.Event()

            async def one():
                async with factory() as db:
                    repo = ex.Repo(db)
                    orig = repo.live_order

                    async def live_order(oid):           # 두 세션 모두 '반영 전' 을 읽은 뒤에야 진행
                        row = await orig(oid)
                        arrived[0] += 1
                        if arrived[0] >= 2:
                            go.set()
                        await go.wait()
                        return row

                    repo.live_order = live_order
                    p = (await db.execute(select(StrategyPlan).where(StrategyPlan.id == plan_id))).scalar_one()
                    return await ex.sync_all_pending(repo, p)

            applied = await asyncio.wait_for(asyncio.gather(one(), one()), timeout=30)
            async with factory() as db:
                pos = (await db.execute(select(StrategyPosition).where(StrategyPosition.plan_id == plan_id))).scalar_one()
                sl = (await db.execute(select(StrategySlice).where(StrategySlice.plan_id == plan_id))).scalar_one()
            assert sorted(applied) == [0, 1]                    # 한 세션만 반영
            assert (pos.quantity, pos.avg_price, sl.filled_qty) == (1, 152_600.0, 1)
        finally:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
            await engine.dispose()

    asyncio.run(main())
