"""계획 계산 — 순수 함수(입출력 없음). service 가 시세·잔고·장부를 모아 넘긴다.

용어
  장부(ledger)  : 이 전략이 산 수량·평균가(StrategyPosition). 계좌 전체 보유와 다를 수 있다.
  실효 투자금액 : min(설정 투자금액, 장부 전체 평가액 + 예수금). 설정값이 더 크면 경고.
                  (목표에서 빠진 장부 종목도 그날 팔아 현금이 되므로 포함한다)
  목표수량      : floor(실효 투자금액 × 비중 ÷ 현재가)  (내림 — 주문가능금액을 넘기지 않게)
  매매수량      : 목표수량 − 장부 수량. 매도는 min(장부 수량, 계좌 보유수량)을 넘지 않는다.
목표 파일에서 빠진 장부 종목은 "목표 제외 → 전량 매도"(목표수량 0, 매도 시분할). 장부에 없는 계좌 보유 종목(직접 산 종목)은 다루지 않는다.
"""
from __future__ import annotations

import csv
import io
import math
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

from app.services import ta_utils as ta

KST = timezone(timedelta(hours=9))

# 리밸런싱 날 시분할 시간표 (5분 간격). 15:00 에 미체결 일괄 취소.
SLICE_INTERVAL_MIN = 5
SLICE_START = time(9, 10)
SELL_LAST_SLICE = time(14, 30)
BUY_LAST_SLICE = time(14, 55)
CANCEL_ALL_AT = time(15, 0)


def slice_times(last: time, start: time = SLICE_START, interval_min: int = SLICE_INTERVAL_MIN) -> list[time]:
    out, cur = [], datetime.combine(date(2000, 1, 1), start)
    end = datetime.combine(date(2000, 1, 1), last)
    while cur <= end:
        out.append(cur.time())
        cur += timedelta(minutes=interval_min)
    return out


SELL_ROUNDS = len(slice_times(SELL_LAST_SLICE))   # 65
BUY_ROUNDS = len(slice_times(BUY_LAST_SLICE))     # 70


class AtrUnavailable(Exception):
    pass


def _kst_date(ts) -> date:
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, KST).date()
    return datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()


def atr_from_daily(candles: list[dict], today: date, period: int = 14) -> tuple[float, date]:
    """완성된 일봉(today 이전)만으로 Wilder ATR. 반환 (ATR, 마지막으로 쓴 봉 날짜).

    st 기간별 시세는 장중에 오늘 미완성 봉을 함께 돌려주므로 반드시 오늘 봉을 뺀다.
    """
    rows = []
    for c in candles or []:
        try:
            d = _kst_date(c["time"])
            h, l, cl = float(c["high"]), float(c["low"]), float(c["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if d < today and h > 0 and l > 0 and cl > 0:
            rows.append((d, h, l, cl))
    rows.sort()
    if len(rows) < period + 1:
        raise AtrUnavailable(f"완성 일봉 {len(rows)}개 — ATR({period}) 계산에는 {period + 1}개 이상 필요")
    df = pd.DataFrame(rows, columns=["d", "h", "l", "c"])
    series = ta.atr(df["h"], df["l"], df["c"], period, method="wilder")
    value = float(series.iloc[-1])
    if not math.isfinite(value) or value <= 0:
        raise AtrUnavailable("ATR 계산 결과가 유효하지 않음")
    return value, rows[-1][0]


def effective_budget(invest_amount: float, ledger_eval: float, cash: float) -> tuple[float, list[str]]:
    cap = max(0.0, float(ledger_eval) + float(cash))
    amount = max(0.0, float(invest_amount))
    if amount > cap:
        return cap, [f"설정한 투자금액 {amount:,.0f}원이 (장부 평가액 {ledger_eval:,.0f}원 + 예수금 {cash:,.0f}원) = "
                     f"{cap:,.0f}원보다 커서 {cap:,.0f}원으로 줄였습니다."]
    return amount, []


def stop_price(avg_price: float, atr: float | None, stop_atr_mult: float, stop_loss_pct: float) -> tuple[float | None, str]:
    """손절가와 근거. ATR 이 있으면 평균가 − ATR×배수, 없으면 평균가 × (1 + 손절%)."""
    if not avg_price or avg_price <= 0:
        return None, ""
    if atr and atr > 0:
        return avg_price - atr * stop_atr_mult, f"평균가 − ATR×{stop_atr_mult:g}"
    return avg_price * (1 + stop_loss_pct / 100), f"평균가 {stop_loss_pct:+g}%"


def buy_band(open_price: float | None, atr: float | None, mult: float) -> tuple[float, float] | None:
    """리밸런싱 날 시가 ± ATR×배수. ATR 이 없으면 제한 없음(None), 시가가 0(장 시작 전)이면 아직 모름(None)."""
    if not atr or atr <= 0 or not open_price or open_price <= 0:
        return None
    return open_price - atr * mult, open_price + atr * mult


def rounds_for(quantity: int, side: str) -> int:
    """회차 수 = min(전체 회차, 주식 수), 회당 최소 1주."""
    total = SELL_ROUNDS if side == "SELL" else BUY_ROUNDS
    return min(total, abs(int(quantity)))


def build_rows(targets: list[dict], ledger: dict[str, dict], account_qty: dict[str, int], quotes: dict[str, dict],
               atr_info: dict[str, dict], budget: float, *, stop_atr_mult: float, stop_loss_pct: float,
               buy_band_atr_mult: float, max_order_amount: float) -> tuple[list[dict], list[str]]:
    """종목별 계획 행과 경고. 행 키는 영문(저장용), to_plan_csv 가 plan_*.csv 한글 컬럼으로 바꾼다."""
    rows, warnings = [], []
    target_syms = set()
    for t in targets:
        sym = t["symbol"]
        target_syms.add(sym)
        q = quotes.get(sym) or {}
        a = atr_info.get(sym) or {}
        led = ledger.get(sym) or {}
        held = int(led.get("quantity") or 0)
        acct = int(account_qty.get(sym, 0))
        price = float(q.get("price") or 0)
        row = {
            "symbol": sym, "name": t.get("name") or q.get("name") or led.get("name") or "", "in_portfolio": True, "excluded": False,
            "weight": float(t["weight"]), "atr": a.get("atr"), "atr_source": a.get("source", "none"),
            "atr_as_of": a.get("as_of"), "atr_error": a.get("error", ""), "price": price or None,
            "open": float(q.get("open") or 0) or None,
            "ledger_qty": held, "ledger_avg_price": float(led.get("avg_price") or 0) or None, "account_qty": acct,
            "sellable_qty": min(held, acct), "target_qty": None, "trade_qty": 0, "amount": 0.0,
            "rounds": 0, "stop_price": None, "stop_basis": "", "band_low": None, "band_high": None, "note": "",
        }
        if price <= 0:
            row["note"] = "현재가 조회 실패 — 이번 계획에서 제외"
            warnings.append(f"{sym} {row['name']}: 현재가를 가져오지 못해 계획에서 뺐습니다. {q.get('error', '')}".strip())
            rows.append(row)
            continue
        target_qty = math.floor(budget * row["weight"] / price)
        trade = target_qty - held
        if trade < 0 and -trade > row["sellable_qty"]:
            row["note"] = f"매도 가능 수량({row['sellable_qty']}주)까지만 매도"
            trade = -row["sellable_qty"]
        side = "SELL" if trade < 0 else "BUY"
        row.update(target_qty=target_qty, trade_qty=trade, amount=round(trade * price, 2), rounds=rounds_for(trade, side) if trade else 0)
        if trade:
            per_slice = math.ceil(abs(trade) / row["rounds"])
            if per_slice * price > max_order_amount:
                warnings.append(f"{sym} {row['name']}: 회당 {per_slice}주 × {price:,.0f}원이 1건 상한 {max_order_amount:,.0f}원을 넘습니다 "
                                "(4단계에서 회차 안 분할 필요).")
        sp, basis = stop_price(row["ledger_avg_price"] or 0, row["atr"], stop_atr_mult, stop_loss_pct)
        row.update(stop_price=round(sp, 2) if sp else None, stop_basis=basis)
        band = buy_band(row["open"], row["atr"], buy_band_atr_mult)
        if band:
            row.update(band_low=round(band[0], 2), band_high=round(band[1], 2))
        elif side == "BUY" and trade > 0 and not row["atr"]:
            row["note"] = (row["note"] + " · " if row["note"] else "") + "ATR 없음 — 매수 범위 제한 없음, 손절은 % 기준"
        rows.append(row)

    for sym, led in sorted(ledger.items()):
        if sym in target_syms or int(led.get("quantity") or 0) <= 0:
            continue
        q = quotes.get(sym) or {}
        rows.append({
            "symbol": sym, "name": led.get("name") or q.get("name") or "", "in_portfolio": False, "weight": 0.0,
            "atr": led.get("atr"), "atr_source": led.get("atr_source", "none"), "atr_as_of": led.get("atr_as_of"), "atr_error": "",
            "price": float(q.get("price") or 0) or None, "open": None,
            "ledger_qty": int(led["quantity"]), "ledger_avg_price": float(led.get("avg_price") or 0) or None,
            "account_qty": int(account_qty.get(sym, 0)), "sellable_qty": min(int(led["quantity"]), int(account_qty.get(sym, 0))),
            "target_qty": 0, "trade_qty": 0, "amount": 0.0, "rounds": 0, "excluded": True,
            "stop_price": None, "stop_basis": "", "band_low": None, "band_high": None,
            "note": "목표 제외 → 전량 매도",
        })
        row = rows[-1]
        price = row["price"] or 0
        trade = -row["sellable_qty"]
        if row["sellable_qty"] < row["ledger_qty"]:
            row["note"] += f" (매도 가능 {row['sellable_qty']}주 — 계좌 보유 {row['account_qty']}주)"
        if price <= 0:
            row["note"] += " · 현재가 조회 실패 — 회차마다 다시 확인"
        row.update(trade_qty=trade, amount=round(trade * price, 2), rounds=rounds_for(trade, "SELL") if trade else 0)
        sp, basis = stop_price(row["ledger_avg_price"] or 0, row["atr"], stop_atr_mult, stop_loss_pct)
        row.update(stop_price=round(sp, 2) if sp else None, stop_basis=basis)
    rows.sort(key=lambda r: (r["trade_qty"], r["symbol"]))   # 참고 구현처럼 매도(음수) 먼저
    return rows, warnings


def ledger_eval_for(targets: list[dict], ledger: dict[str, dict], quotes: dict[str, dict]) -> float:
    """장부 전체 평가액(현재가 없으면 평균가로 평가). 목표에서 빠진 장부 종목도 그날 매도해 현금이 되므로 포함한다."""
    total = 0.0
    for sym, led in ledger.items():
        qty = int(led.get("quantity") or 0)
        if qty:
            price = float((quotes.get(sym) or {}).get("price") or 0) or float(led.get("avg_price") or 0)
            total += qty * price
    return total


def summarize(rows: list[dict]) -> dict:
    buys = [r for r in rows if r["trade_qty"] > 0]
    sells = [r for r in rows if r["trade_qty"] < 0]
    return {
        "buy_symbols": len(buys), "buy_qty": sum(r["trade_qty"] for r in buys), "buy_amount": round(sum(r["amount"] for r in buys), 2),
        "sell_symbols": len(sells), "sell_qty": -sum(r["trade_qty"] for r in sells), "sell_amount": round(-sum(r["amount"] for r in sells), 2),
        "buy_rounds": BUY_ROUNDS, "sell_rounds": SELL_ROUNDS, "slices": sum(r["rounds"] for r in rows),
    }


# plan_*.csv(참고 구현) 컬럼 → 행 키. 앞 10개가 참고 구현과 같은 순서, 뒤는 이 구현에서 추가한 컬럼.
PLAN_CSV_COLUMNS = [
    ("종목코드", "symbol"), ("종목명", "name"), ("비중", "weight"), ("ATR", "atr"), ("현재가", "price"),
    ("보유수량", "ledger_qty"), ("주문가능수량", "sellable_qty"), ("목표수량", "target_qty"), ("매매수량", "trade_qty"),
    ("예상금액", "amount"),
    ("ATR출처", "atr_source"), ("ATR기준일", "atr_as_of"), ("계좌보유수량", "account_qty"), ("장부평균가", "ledger_avg_price"),
    ("손절가", "stop_price"), ("손절기준", "stop_basis"), ("시가", "open"), ("매수범위하단", "band_low"), ("매수범위상단", "band_high"),
    ("회차수", "rounds"), ("포트폴리오포함", "in_portfolio"), ("목표제외", "excluded"), ("비고", "note"),
]


def to_plan_csv(rows: list[dict]) -> str:
    """UTF-8 BOM CSV (엑셀에서 한글이 깨지지 않게, 참고 구현과 같은 utf-8-sig)."""
    def fmt(key, v):
        if v is None:
            return ""
        if key == "weight":
            return f"{v:.6g}"
        if isinstance(v, bool):
            return "Y" if v else "N"
        if isinstance(v, float):
            return str(int(v)) if v == int(v) else f"{v:.2f}"
        return str(v)
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([c for c, _ in PLAN_CSV_COLUMNS])
    for r in rows:
        writer.writerow([fmt(key, r.get(key)) for _, key in PLAN_CSV_COLUMNS])
    return "﻿" + buf.getvalue()
