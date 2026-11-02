"""ATR 리밸런싱 전략 운영 도우미 (lumina 컨테이너 안에서 실행 — 보통은 같은 폴더의 atr.sh 로 부른다).

you1@example.com 계정용 15분짜리 액세스 토큰을 내부에서 발급해 /api/strategy/atr API 를 호출한다. 토큰은 출력하지 않는다.

  plan                        플랜 설정·목표·장부 요약
  invest <원>                 투자금액만 바꾼다 (다른 설정은 그대로)
  upload <컨테이너 경로> [이름] 목표 파일(CSV/xlsx)로 목표를 통째로 바꾼다 — 주문은 나가지 않는다
  dryrun                      설정을 바꾸지 않고 오늘 기준 계획만 만든다 → /tmp/plan.csv, 종목별 예상 주문 시각 출력
  start                       실주문 모드로 켜고(dry_run=false) 오늘 리밸런싱을 수동 시작
  today                       오늘 리밸런싱 진행 상황
  stop                        오늘 리밸런싱 중지(열린 리밸런싱 주문 취소, 손절 감시는 계속)
  kill | unkill               비상 정지 켜기/끄기 (켜면 리밸런싱·손절 모두 멈춤, 열린 주문은 취소하지 않으니 stop 먼저)
  deactivate                  전략 끄기 (손절 감시도 멈춤)
"""
import asyncio
import json
import sys

import httpx
from sqlalchemy import select

from app.database.postgres import connect_postgres, get_session_factory
from app.lib.jwt_auth import create_access_token
from app.models import User

EMAIL = "you1@example.com"
BASE = "http://localhost:8000/api/strategy/atr"


async def _user():
    await connect_postgres()
    async with get_session_factory()() as db:
        return (await db.execute(select(User).where(User.email == EMAIL))).scalar_one()


def client():
    u = asyncio.run(_user())
    token = create_access_token({"id": str(u.id), "name": u.name, "email": u.email, "client_id": "", "roles": ["user"]})
    return httpx.Client(timeout=180, headers={"Authorization": f"Bearer {token}"}, base_url=BASE)


def _show(r, keys=None):
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    if keys and isinstance(body, dict):
        body = {k: body.get(k) for k in keys}
    print(r.status_code, json.dumps(body, ensure_ascii=False, indent=1)[:4000])
    return r.status_code < 400


PLAN_KEYS = ("is_active", "dry_run", "invest_amount", "buy_band_atr_mult", "stop_atr_mult", "stop_loss_pct", "max_tickers",
             "max_order_amount", "max_orders_per_day", "target_file_name", "targets_uploaded_at")


def _slot_times(rounds: int, side: str) -> list[str]:
    """09:10(첫 회차)에 시작한다고 가정한 예상 주문 시각 — executor.plan_slots 와 같은 규칙."""
    from app.services.atr_strategy import executor as ex, planner
    last = planner.SELL_LAST_SLICE if side == "SELL" else planner.BUY_LAST_SLICE
    times = planner.slice_times(last)
    return [times[s].strftime("%H:%M") for s in ex.plan_slots(0, len(times), rounds)] if rounds else []


def cmd_plan(c):
    b = c.get("/plan").json()
    print("plan:", json.dumps({k: b["plan"].get(k) for k in PLAN_KEYS}, ensure_ascii=False))
    print("targets:", [(t["symbol"], t["name"], round(t["weight"], 4), t["atr_input"]) for t in b["targets"]])
    print("positions:", [(p["symbol"], p["quantity"], round(p["avg_price"], 2), p.get("atr")) for p in b["positions"]])
    print("auto_trade_block_reason:", b.get("auto_trade_block_reason"))


def cmd_invest(c, amount):
    r = c.put("/plan", json={"invest_amount": float(amount)})
    if r.status_code != 200:
        return _show(r)
    print(200, "invest_amount =", f'{r.json()["plan"]["invest_amount"]:,.0f}')


def cmd_upload(c, path, name=None):
    with open(path, "rb") as f:
        r = c.post("/targets", files={"file": (name or path.rsplit("/", 1)[-1], f.read(), "text/csv")})
    if r.status_code != 200:
        return _show(r)
    print(200, "목표", r.json()["count"], "종목:", [(t["symbol"], t["name"], round(t["weight"], 4), t["atr_input"]) for t in r.json()["targets"]])


def cmd_dryrun(c):
    r = c.post("/dry-run")
    if r.status_code != 200:
        return _show(r)
    run = r.json()
    open("/tmp/plan.csv", "w", encoding="utf-8").write(c.get(f"/runs/{run['id']}/plan.csv").text)
    print("run", run["id"], "| 실효 투자금액", f"{run['effective_budget']:,.0f}", "| 예수금", f"{run['cash_balance']:,.0f}")
    print("summary:", {k: run["summary"].get(k) for k in ("buy_symbols", "buy_qty", "buy_amount", "sell_symbols", "sell_qty", "sell_amount", "first_trading_day")})
    for w in run["warnings"]:
        print("경고:", w)
    for r_ in run["rows"]:
        side = "SELL" if r_["trade_qty"] < 0 else "BUY"
        print(f'{r_["symbol"]} {r_["name"]} | {side} {abs(r_["trade_qty"])}주 {abs(r_["amount"]):,.0f}원 | 회차 {r_["rounds"]} '
              f'| 예상 주문 시각 {", ".join(_slot_times(r_["rounds"], side)) or "-"} | ATR {r_["atr"]} ({r_["atr_source"]}) | {r_.get("note") or ""}')
    print("plan.csv → /tmp/plan.csv")


def cmd_start(c):
    if _show(c.post("/activate", json={"dry_run": False}), ["plan", "stopped_auto_trade", "warnings"]):
        _show(c.post("/rebalance/start", json={"force": False}), ["id", "status", "trigger", "attempt", "effective_budget", "warnings"])


async def _kill(enabled: bool):
    from app.models import BrokerSettings
    from app.services.audit import audit
    u = await _user()
    async with get_session_factory()() as db:
        row = (await db.execute(select(BrokerSettings).where(BrokerSettings.user_id == u.id))).scalar_one_or_none()
        if row is None:
            row = BrokerSettings(user_id=u.id)
            db.add(row)
        row.risk_kill_switch = enabled
        row.risk_halt_reason = "ATR 전략 수동 비상 정지" if enabled else ""
        await db.commit()
        await audit(str(u.id), "", "quant.kill_switch", {"enabled": enabled, "via": "atr_api.py"})
        print("kill_switch =", row.risk_kill_switch)


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    cmd, args = argv[0], argv[1:]
    if cmd in ("kill", "unkill"):
        asyncio.run(_kill(cmd == "kill"))
        return 0
    with client() as c:
        if cmd == "plan":
            cmd_plan(c)
        elif cmd == "invest":
            cmd_invest(c, args[0])
        elif cmd == "upload":
            cmd_upload(c, *args)
        elif cmd == "dryrun":
            cmd_dryrun(c)
        elif cmd == "start":
            cmd_start(c)
        elif cmd == "today":
            _show(c.get("/rebalance/today"))
        elif cmd == "stop":
            _show(c.post("/rebalance/stop"))
        elif cmd == "deactivate":
            _show(c.post("/deactivate"))
        else:
            print(__doc__)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
