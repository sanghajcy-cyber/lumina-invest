"""ATR 전략 executor·stoploss 테스트 공용 가짜: 메모리 저장소(executor.Repo 와 같은 메서드), 게이트웨이·시세·주문 경로."""
import uuid
from datetime import timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.models import LiveOrder, StrategyPosition, StrategyRun, StrategySlice, StrategyStopEvent
from app.services import auto_trade, kis_market_data as kmd, risk_guard
from app.services.atr_strategy import executor as ex
from app.services.atr_strategy import service as svc
from app.services.brokers import stock_coin_trade_gateway as gw

KST = timezone(timedelta(hours=9))
UID = uuid.UUID("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")


class MemRepo:
    def __init__(self, plan, broker_row=None):
        self.db = SimpleNamespace(name="memdb")
        self.plan, self.broker = plan, broker_row
        self.runs, self.slices_, self.positions_, self.orders, self.stops = [], [], [], [], []
        self.commits = 0
    def add(self, obj):
        if getattr(obj, "id", None) is None:
            obj.id = uuid.uuid4()
        bucket = {StrategyRun: self.runs, StrategySlice: self.slices_, StrategyPosition: self.positions_, LiveOrder: self.orders,
                  StrategyStopEvent: self.stops}[type(obj)]
        if obj not in bucket:
            bucket.append(obj)
    async def commit(self): self.commits += 1
    async def flush(self): pass
    async def live_plans(self): return [self.plan] if self.plan.is_active and not self.plan.dry_run else []
    async def plan_for(self, uid): return self.plan
    async def broker_row(self, uid): return self.broker
    async def today_run(self, plan, day): return next((r for r in self.runs if r.run_date == day and r.kind == "rebalance"), None)
    async def slices(self, run): return [s for s in self.slices_ if s.run_id == run.id]
    async def live_order(self, oid): return next((o for o in self.orders if o.id == oid), None)
    async def live_order_by_client_id(self, cid): return next((o for o in self.orders if o.client_order_id == cid), None)
    async def open_stop_orders(self, uid, symbol=None):
        return [o for o in self.orders if o.status in ex.LIVE_ORDER_OPEN_STATUSES and ":SL:" in o.client_order_id
                and (symbol is None or o.symbol == symbol)]
    async def pending_slices(self, plan): return [s for s in self.slices_ if s.status == "submitted"]
    async def stop_events(self, plan, day=None): return [e for e in self.stops if day is None or e.trade_date == day]
    async def pending_stop_events(self, plan): return [e for e in self.stops if e.status in ("submitted", "planned")]
    async def positions(self, plan): return list(self.positions_)
    async def open_rebalance_orders(self, uid, symbol=None):
        return [o for o in self.orders if o.status in ex.LIVE_ORDER_OPEN_STATUSES and ":ATR" in o.client_order_id
                and (symbol is None or o.symbol == symbol)]
    async def position(self, plan, symbol): return next((p for p in self.positions_ if p.symbol == symbol), None)
    async def ledger(self, plan):
        return {p.symbol: {"quantity": p.quantity, "avg_price": p.avg_price, "name": p.name} for p in self.positions_ if (p.quantity or 0) > 0}
    async def orders_placed_today(self, plan, day):
        ids = {r.id for r in self.runs if r.run_date == day}
        return sum(1 for s in self.slices_ if s.run_id in ids and s.live_order_id)


class FakeBroker:
    """게이트웨이·시세·주문 경로. 주문은 ACCEPTED 로 들어오고 fill()/cancel 로 상태가 바뀐다."""

    def __init__(self, repo, quotes, balance=None):
        self.repo, self.quotes = repo, quotes
        self.balance = balance or {"cashBalance": 100_000_000, "totalEvalAmount": 100_000_000, "holdings": []}
        self.placed, self.cancelled, self.n = [], [], 0
        self.cancel_sticks = set()        # 취소 요청이 남아 있는(확인 안 된) 주문번호

    async def place(self, db, broker_row, symbol, name, side, qty, price, user_id, **kw):
        self.n += 1
        lo = LiveOrder(user_id=UID, client_order_id=kw["client_order_id"], environment="paper", broker="kis", symbol=symbol, name=name,
                       side=side.upper(), order_type=kw["order_type"], quantity=qty, price=price, order_no=f"{self.n:010d}",
                       status="ACCEPTED", filled_quantity=0, avg_filled_price=0.0, message="", raw="", source=kw["source"])
        self.repo.add(lo)
        assert any(s.client_order_id == lo.client_order_id for s in [*self.repo.slices_, *self.repo.stops]), "회차·손절 기록이 주문보다 먼저 있어야 한다"
        self.placed.append((symbol, side.upper(), qty, price, kw["client_order_id"]))
        return {"status": "submitted", "order_no": lo.order_no, "client_order_id": lo.client_order_id}

    def order(self, order_no): return next(o for o in self.repo.orders if o.order_no == order_no)

    def fill(self, order_no, qty):
        o = self.order(order_no)
        o.filled_quantity = qty
        o.avg_filled_price = o.price
        o.status = "FILLED" if qty >= o.quantity else "PARTIALLY_FILLED"

    async def status(self, order_no, env=None):
        o = self.order(order_no)
        st = o.status
        if st == "CANCEL_REQUESTED" and order_no not in self.cancel_sticks:
            st = "CANCELLED"
        return {"orderNo": order_no, "status": st, "filledQuantity": o.filled_quantity, "avgFilledPrice": o.avg_filled_price}

    async def cancel(self, order_no, env=None):
        o = self.order(order_no)
        if o.status == "FILLED":
            raise gw.GatewayError("정정/취소할 수량이 없습니다", "KIS_40330000", 502)
        self.cancelled.append(order_no)
        return {"orderNo": order_no, "status": "CANCEL_REQUESTED", "filledQuantity": o.filled_quantity}

    async def get_balance(self, env=None): return self.balance
    async def account_snapshot(self):
        return float(self.balance.get("cashBalance") or 0), {h["symbol"]: int(h["quantity"]) for h in self.balance.get("holdings") or []}
    async def get_quote(self, symbol): return dict(self.quotes[symbol])


def _plan(**over):
    base = dict(id=uuid.uuid4(), user_id=UID, environment="paper", is_active=True, dry_run=False, invest_amount=10_000_000.0,
                max_tickers=20, atr_period=14, buy_band_atr_mult=1.0, stop_atr_mult=2.0, stop_loss_pct=-10.0,
                max_order_amount=5_000_000.0, max_orders_per_day=1000, target_file_name="p.csv")
    base.update(over)
    return SimpleNamespace(**base)


def _row(symbol, target, price, atr=None, **over):
    r = {"symbol": symbol, "name": symbol, "in_portfolio": True, "weight": 0.5, "atr": atr, "atr_source": "kis" if atr else "none",
         "atr_as_of": "2026-10-30" if atr else None, "price": price, "open": None, "target_qty": target, "trade_qty": target,
         "band_low": None, "band_high": None}
    r.update(over)
    return r


def _fields(rows):
    return dict(invest_amount=10_000_000.0, effective_budget=10_000_000.0, cash_balance=1.0, ledger_eval=0.0, warnings=[],
                rows=rows, summary={}, note="")


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(svc, "LIVE_EXECUTION_AVAILABLE", True)
    monkeypatch.setattr(risk_guard, "_redis", lambda: None)
    risk_guard._mem_keys.clear(); risk_guard._mem_values.clear(); ex._mem_lock.clear()

    def make(rows, quotes, plan=None, broker_row=None, balance=None):
        plan = plan or _plan()
        # 쿨다운은 실제 시계(time.time)로 판정하므로 가상 시각으로 회차를 넘기는 테스트에서는 0 으로 둔다(clamp 는 별도 테스트)
        broker_row = broker_row or SimpleNamespace(risk_kill_switch=False, risk_halt_reason="", risk_daily_loss_limit_pct=3.0,
                                                   risk_max_position_pct=30.0, risk_max_orders_per_day=20, risk_cooldown_min=0)
        repo = MemRepo(plan, broker_row)
        fb = FakeBroker(repo, quotes, balance)
        compute = AsyncMock(side_effect=lambda *a, **k: _fields([dict(r) for r in rows]))
        patches = [patch.object(svc, "compute_plan", compute),
                   patch.object(svc, "account_snapshot", AsyncMock(side_effect=fb.account_snapshot)), patch.object(auto_trade, "_place_live_order_via_gateway", fb.place),
                   patch.object(gw, "get_order_status", fb.status), patch.object(gw, "cancel_order", fb.cancel),
                   patch.object(gw, "get_balance", fb.get_balance), patch.object(kmd, "get_quote", fb.get_quote),
                   patch.object(ex, "audit", AsyncMock())]
        for p in patches:
            p.start()
        make.patches.extend(patches)
        return SimpleNamespace(repo=repo, fb=fb, plan=plan, compute=compute)
    make.patches = []
    yield make
    for p in reversed(make.patches):        # 같은 대상을 두 번 패치했으면 역순으로 풀어야 원래 값이 돌아온다
        p.stop()


