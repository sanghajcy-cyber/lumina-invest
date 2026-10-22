"""월초 ATR 리밸런싱 전략 Celery 태스크 (trading 큐 — celery_app.task_routes 의 strategy.*)."""
import asyncio
import logging

from app.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="strategy.rebalance_tick", time_limit=55, ignore_result=True)
def strategy_rebalance_tick() -> dict:
    """거래일 09:00~15:20 매분: 첫 거래일 자동 시작·이어받기, 현재 회차 처리, 15:00 일괄 취소, 15:20 결과 확정."""

    async def _async() -> dict:
        from app.database.postgres import connect_postgres, close_postgres
        from app.lib.redis_cache import connect_redis, close_redis
        from app.services.atr_strategy.executor import rebalance_tick

        await connect_redis()
        await connect_postgres()
        try:
            return await rebalance_tick()
        finally:
            await close_redis()
            await close_postgres()

    result = asyncio.run(_async())
    if result and result.get("skipped") not in ("live_execution_not_approved", "not_trading_day"):
        logger.info("[beat] strategy.rebalance_tick: %s", result)
    return result
