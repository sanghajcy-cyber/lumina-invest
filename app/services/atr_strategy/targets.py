"""목표 포트폴리오 파일(CSV/xlsx) → 검증된 목표 행.

컬럼: 종목코드(필수) · 종목명 · 비중 · ATR. 별칭(code/ticker/weight/비중(%)…)을 받는다.
  - 종목코드: 6자리. 엑셀에서 앞자리 0이 빠지거나(5930) 5930.0·005930.KS 로 와도 6자리로 맞춘다.
  - 비중: 합계가 100이 아니어도 비율로 환산한다. 컬럼이 없거나 전부 비면 동일비중.
  - ATR: 원 단위, 0보다 커야 한다. 빈칸은 None(→ KIS 일봉으로 계산).
  - 종목 수 상한(max_tickers): 게이트웨이 잔고가 모의투자에서 20종목을 넘으면 잘리므로 기본 20.
"""
from __future__ import annotations

import io
import re

import pandas as pd

COLUMN_ALIASES = {
    "종목코드": "종목코드", "코드": "종목코드", "ticker": "종목코드", "code": "종목코드", "pdno": "종목코드", "symbol": "종목코드",
    "종목명": "종목명", "name": "종목명",
    "비중": "비중", "weight": "비중", "비중(%)": "비중", "weight_pct": "비중",
    "atr": "ATR",
}
_CODE_RE = re.compile(r"^\d{1,6}$")


class TargetFileError(ValueError):
    """사용자에게 그대로 보여 줄 파일 오류."""


def _read_frame(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(io.BytesIO(content), dtype=str)
        except Exception as exc:
            raise TargetFileError(f"엑셀 파일을 읽지 못했습니다: {exc}") from exc
    for enc in ("utf-8-sig", "cp949"):        # 엑셀에서 저장한 한글 CSV 는 cp949
        try:
            return pd.read_csv(io.BytesIO(content), dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            raise TargetFileError(f"CSV 파일을 읽지 못했습니다: {exc}") from exc
    raise TargetFileError("CSV 인코딩을 알 수 없습니다 (UTF-8 또는 CP949 로 저장하세요).")


def _norm_code(raw: str) -> str:
    code = str(raw).strip().upper()
    for suffix in (".KS", ".KQ"):
        if code.endswith(suffix):
            code = code[: -len(suffix)]
    code = code.split(".")[0]                   # 엑셀 숫자 셀 5930.0
    if not _CODE_RE.match(code):
        raise TargetFileError(f"종목코드 형식 오류: {raw!r} (6자리 숫자)")
    return code.zfill(6)


def _num(raw) -> float | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).replace(",", "").replace("%", "").strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_target_file(content: bytes, filename: str, max_tickers: int = 20) -> list[dict]:
    """반환: [{symbol, name, weight(0~1, 합 1), weight_input, atr_input, position}] (파일 행 순서)."""
    if not content:
        raise TargetFileError("빈 파일입니다.")
    df = _read_frame(content, filename)
    df.columns = [COLUMN_ALIASES.get(str(c).strip().lower(), COLUMN_ALIASES.get(str(c).strip(), str(c).strip())) for c in df.columns]
    if "종목코드" not in df.columns:
        raise TargetFileError(f"'종목코드' 컬럼이 필요합니다. 현재 컬럼: {list(df.columns)}")
    df = df[df["종목코드"].notna() & (df["종목코드"].astype(str).str.strip() != "")].reset_index(drop=True)
    if df.empty:
        raise TargetFileError("종목이 한 줄도 없습니다.")
    if max_tickers and len(df) > max_tickers:
        raise TargetFileError(f"종목 수({len(df)})가 상한({max_tickers})을 넘습니다. "
                              "게이트웨이 잔고 조회가 모의투자에서 20종목을 넘으면 잘리기 때문에 제한합니다.")

    codes = [_norm_code(c) for c in df["종목코드"]]
    dup = sorted({c for c in codes if codes.count(c) > 1})
    if dup:
        raise TargetFileError(f"중복된 종목코드: {dup}")

    names = [str(n).strip() if "종목명" in df.columns and pd.notna(n) else "" for n in (df["종목명"] if "종목명" in df.columns else [None] * len(df))]

    raw_w = [_num(v) for v in df["비중"]] if "비중" in df.columns else [None] * len(df)
    if any(w is not None for w in raw_w):
        bad = [codes[i] for i, w in enumerate(raw_w) if w is None or pd.isna(w) or w < 0]
        if bad:
            raise TargetFileError(f"비중이 비었거나 숫자가 아니거나 음수인 종목: {bad} (비중을 쓰려면 모든 행에 적으세요)")
        total = sum(raw_w)
        if total <= 0:
            raise TargetFileError("비중 합계가 0입니다.")
        weights = [w / total for w in raw_w]
    else:
        weights = [1.0 / len(codes)] * len(codes)

    atrs = [_num(v) for v in df["ATR"]] if "ATR" in df.columns else [None] * len(df)
    bad_atr = [codes[i] for i, a in enumerate(atrs) if a is not None and (pd.isna(a) or a <= 0)]
    if bad_atr:
        raise TargetFileError(f"ATR 값은 0보다 큰 숫자여야 합니다: {bad_atr}")

    return [{"symbol": codes[i], "name": names[i], "weight": weights[i], "weight_input": raw_w[i],
             "atr_input": atrs[i], "position": i} for i in range(len(codes))]
