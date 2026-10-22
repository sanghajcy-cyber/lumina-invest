"""월초 ATR 리밸런싱 전략의 기반 작업: Wilder ATR, 거래일 달력, clientOrderId 태그, live_orders.source, trading 큐.

전략 본체(시분할·손절)는 아직 없다. 여기서는 그 전략이 기대는 하부 부품만 검증한다.
"""
import asyncio
import math
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import numpy as np
import pandas as pd
import pytest

from app.celery_app import TRADING_QUEUE, celery_app
from app.config import settings
from app.models import LIVE_ORDER_SOURCE_MANUAL, LIVE_ORDER_SOURCE_QUANT_AUTO, LIVE_ORDER_SOURCE_REBALANCE, LiveOrder
from app.services import auto_trade
from app.services import ta_utils as ta
from app.services.brokers import stock_coin_trade_gateway as gw
from app.tasks import beat_health as bh
from tests.conftest import make_candles

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")


# ── Wilder ATR ────────────────────────────────────────────────────────────


def _hlc():
    # TR: [92(전일 종가 없음 — 제외돼야 함), 2, 2, 2, 3, 2]
    high = pd.Series([100.0, 11, 12, 11, 13, 14])
    low = pd.Series([8.0, 9, 10, 9, 11, 12])
    close = pd.Series([9.0, 10, 11, 10, 12, 13])
    return high, low, close


def test_wilder_atr_matches_hand_computed_values():
    out = ta.atr(*_hlc(), period=3, method="wilder")
    assert out.iloc[:3].isna().all()
    assert out.iloc[3] == pytest.approx(2.0)            # TR[1..3] 평균, 첫 봉 TR(92)은 쓰지 않는다
    assert out.iloc[4] == pytest.approx(7 / 3)          # (2×2 + 3) / 3
    assert out.iloc[5] == pytest.approx(20 / 9)         # (7/3×2 + 2) / 3


def test_default_atr_is_unchanged_sma():
    out = ta.atr(*_hlc(), period=3)
    assert out.iloc[2] == pytest.approx((92 + 2 + 2) / 3)   # 기존 동작: 첫 봉 TR 포함 단순이동평균
    assert out.iloc[5] == pytest.approx((2 + 3 + 2) / 3)


def test_wilder_atr_is_causal_and_short_input_is_all_nan():
    df = pd.DataFrame(make_candles(120))
    h, l, c = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
    full = ta.atr(h, l, c, 14, method="wilder")
    cut = ta.atr(h.iloc[:90], l.iloc[:90], c.iloc[:90], 14, method="wilder")
    pd.testing.assert_series_equal(full.iloc[:90], cut, check_names=False)
    assert ta.atr(h.iloc[:14], l.iloc[:14], c.iloc[:14], 14, method="wilder").isna().all()   # period+1 봉 미만


# ── 거래일 달력 ─────────────────────────────────────────────────────────────


def test_first_trading_day_skips_weekend_and_holidays():
    assert gw.first_trading_day_of_month(2026, 11) == date(2026, 11, 2)    # 11/1 일요일
    assert gw.first_trading_day_of_month(2026, 10) == date(2026, 10, 1)    # 목요일
    assert gw.first_trading_day_of_month(2026, 3) == date(2026, 3, 3)      # 3/1 일요일, 3/2 대체공휴일
    assert gw.first_trading_day_of_month(2026, 1) == date(2026, 1, 2)      # 1/1 신정
    assert gw.is_first_trading_day_of_month(date(2026, 3, 3))
    assert not gw.is_first_trading_day_of_month(date(2026, 3, 4))
    assert not gw.is_krx_trading_day(date(2026, 10, 9))                    # 한글날


def test_holiday_data_coverage_and_extra_holidays(monkeypatch):
    monkeypatch.setattr(settings, "KRX_EXTRA_HOLIDAYS", "", raising=False)
    assert gw.krx_holidays_known(2026)
    assert not gw.krx_holidays_known(2027)
    monkeypatch.setattr(settings, "KRX_EXTRA_HOLIDAYS", "2027-01-01", raising=False)
    assert gw.krx_holidays_known(2027)
    assert gw.first_trading_day_of_month(2027, 1) == date(2027, 1, 4)      # 1/1 금 휴장 → 1/4 월


def test_market_open_still_respects_holidays():
    assert not gw.is_krx_market_open(datetime(2026, 10, 9, 10, 0, tzinfo=KST))
    assert gw.is_krx_market_open(datetime(2026, 10, 8, 10, 0, tzinfo=KST))


# ── clientOrderId 태그 ────────────────────────────────────────────────────


def test_client_order_id_tag_and_seq_distinguish_same_minute_orders():
    at = datetime(2026, 11, 2, 9, 10, 5, tzinfo=KST)
    base = gw.make_client_order_id(UID, "005930.KS", "buy", now=at)
    assert base == "0f1e2d3c4b5a:005930:B:202611020910"                     # 태그 없으면 기존 형식 그대로
    s1 = gw.make_client_order_id(UID, "005930.KS", "buy", now=at, tag="atr", seq=1)
    s2 = gw.make_client_order_id(UID, "005930.KS", "buy", now=at, tag="ATR", seq=2)
    sl = gw.make_client_order_id(UID, "005930.KS", "sell", now=at, tag="ATR_SL", seq=0)
    assert s1 == "0f1e2d3c4b5a:005930:B:202611020910:ATR:1"
    assert len({base, s1, s2, sl}) == 4 and all(len(x) <= 64 for x in (s1, s2, sl))


@pytest.mark.parametrize("tag,seq", [("bad tag", None), ("X" * 17, None), ("한글", None), ("OK", -1)])
def test_client_order_id_rejects_bad_tag_or_seq(tag, seq):
    with pytest.raises(gw.GatewayError):
        gw.make_client_order_id(UID, "005930", "buy", tag=tag, seq=seq)


# ── live_orders.source ────────────────────────────────────────────────────


class FakeDb:
    def __init__(self):
        self.added: list = []
    def add(self, obj): self.added.append(obj)
    async def commit(self): pass


@pytest.fixture
def gateway_env(monkeypatch):
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_BASE_URL", "https://sct.test")
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_API_KEY", "key")
    monkeypatch.setattr(settings, "STOCK_COIN_TRADE_KIS_ENVIRONMENT", "paper")
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json
        if request.url.path.endswith("/order-approval"):
            bodies.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True, "approvalToken": "t", "expiresIn": 60})
        return httpx.Response(200, json={"ok": True, "order": {"orderNo": "0000000001", "status": "ACCEPTED"}, "duplicate": False})

    gw.set_transport(httpx.MockTransport(handler))
    with patch.object(auto_trade.notification, "notify_order_placed", AsyncMock()), \
         patch.object(auto_trade.notification, "notify_order_error", AsyncMock()), \
         patch.object(gw, "is_krx_market_open", return_value=True):
        yield bodies
    gw.set_transport(None)


def _place(db, **kw):
    return asyncio.run(auto_trade._place_live_order_via_gateway(db, None, "005930.KS", "삼성전자", "buy", 1, 70_000.0, str(UID), **kw))


def test_live_order_defaults_to_quant_auto_source(gateway_env):
    db = FakeDb()
    res = _place(db)
    row = next(o for o in db.added if isinstance(o, LiveOrder))
    assert res["status"] == "submitted" and row.source == LIVE_ORDER_SOURCE_QUANT_AUTO
    assert row.client_order_id.count(":") == 3                              # 태그 없음 → 기존 형식


def test_live_order_records_source_tag_and_explicit_order_type(gateway_env):
    db = FakeDb()
    _place(db, source="ATR_REBAL", order_type="market", order_tag="ATR", order_seq=7)
    row = next(o for o in db.added if isinstance(o, LiveOrder))
    assert row.source == "ATR_REBAL" and row.order_type == "MARKET"
    assert row.client_order_id.endswith(":ATR:7")
    assert gateway_env[0]["orderType"] == "MARKET" and gateway_env[0]["clientOrderId"] == row.client_order_id


def test_bad_order_tag_returns_error_without_placing(gateway_env):
    db = FakeDb()
    res = _place(db, order_tag="no spaces allowed")
    assert res["status"] == "error" and db.added == [] and gateway_env == []


def test_manual_and_rebalance_callers_pass_their_source():
    import inspect
    from app.services import rebalance
    assert "source=LIVE_ORDER_SOURCE_MANUAL" in inspect.getsource(auto_trade.place_manual_kis_order)
    assert "source=LIVE_ORDER_SOURCE_REBALANCE" in inspect.getsource(rebalance.execute)
    assert LIVE_ORDER_SOURCE_MANUAL == "MANUAL" and LIVE_ORDER_SOURCE_REBALANCE == "REBALANCE"


# ── trading 큐 ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("task", ["quant.auto_trade_cycle", "quant.confirm_fills", "quant.reconcile",
                                  "rebalance.check_triggers", "beat.heartbeat_trading", "strategy.stop_loss_watch"])
def test_trading_tasks_route_to_trading_queue(task):
    import app.tasks.sync_tasks  # noqa: F401
    route = celery_app.amqp.router.route({}, task)
    assert route["queue"].name == TRADING_QUEUE


@pytest.mark.parametrize("task", ["beat.heartbeat", "sync.market_data", "agent.run"])
def test_other_tasks_stay_on_default_queue(task):
    route = celery_app.amqp.router.route({}, task)
    assert route["queue"].name == "celery"


def test_trading_heartbeat_registered_scheduled_and_keyed_per_queue():
    import app.tasks.sync_tasks  # noqa: F401
    assert "beat.heartbeat_trading" in celery_app.tasks
    entry = celery_app.conf.beat_schedule["beat-heartbeat-trading-1min"]
    assert entry["task"] == "beat.heartbeat_trading" and entry["schedule"] == 60.0 and entry["options"]["expires"] < 60
    store = {}
    fake = MagicMock()
    fake.set.side_effect = lambda k, v, ex=None: store.__setitem__(k, v.encode())
    fake.get.side_effect = lambda k: store.get(k)
    with patch("redis.Redis.from_url", return_value=fake):
        t0 = datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc)
        bh.write_heartbeat(t0, queue="trading")
        assert set(store) == {"celery:beat:heartbeat:trading"}
        assert bh.heartbeat_age_sec(t0 + timedelta(seconds=30), queue="trading") == 30
        assert bh.heartbeat_age_sec(t0, queue="") is None                   # 공용 키와 섞이지 않는다


def test_healthcheck_cli_reads_queue_argument():
    seen = {}
    def fake_age(now=None, queue=""):
        seen["queue"] = queue
        return 10
    with patch.object(bh, "heartbeat_age_sec", side_effect=fake_age), patch.object(bh, "_uptime_sec", return_value=999):
        assert bh.main(["--queue", "trading"]) == 0 and seen["queue"] == "trading"
        assert bh.main([]) == 0 and seen["queue"] == ""
