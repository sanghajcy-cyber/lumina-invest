"""DART 공시 RAG 용 종목 식별 (graph_service 의 15종목 매칭과 독립).

사전: app/data/dart_companies.json  (Qdrant 에 공시가 있는 종목, api-test1-project 의
      scripts/build_dart_company_dictionary.py 로 생성)
      app/data/dart_aliases.json    (수동 관리 별칭)

규칙(엄격):
  1. 종목코드(005930, 005930.KS, 0001A0)는 사전에 있으면 확정한다.
  2. 별칭과 3자 이상 정식 종목명은 가장 긴 이름 우선, 겹침 금지로 확정한다.
     영문/숫자로 시작하거나 끝나는 이름은 앞뒤가 영문/숫자가 아닐 때만 인정한다.
  3. 2자 이하 이름은 이름 바로 뒤에 금융 단서(주가·실적·공시 ...)가 있을 때만 인정한다.
     이름과 단서 사이에는 조사·공백·기간어만 허용한다. 앞쪽 단서는 인정하지 않는다("투자 대상" 오탐 방지).
  4. LG, SK, CJ, GS, DB, LS 처럼 영문/숫자 2자 이하 이름은 그룹명으로도 쓰이므로 더 좁은 단서만 인정한다.
  5. 영문/숫자만으로 된 이름은 대소문자를 구분한다.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
COMPANIES_PATH = DATA_DIR / "dart_companies.json"
ALIASES_PATH = DATA_DIR / "dart_aliases.json"

FINANCE_CUES = (
    "주가", "주식", "종목", "실적", "공시", "재무", "투자", "매출", "영업이익", "순이익", "이익", "배당",
    "시가총액", "목표주가", "목표가", "재무제표", "사업보고서", "분기보고서", "반기보고서",
    "PER", "PBR", "ROE", "EPS",
)
STRICT_CUES = ("주가", "주식", "종목", "공시", "시가총액", "목표주가", "목표가", "PER", "PBR", "배당")

_PARTICLES = "은는이가의을를과와도만에"
_MODIFIERS = ("올해", "작년", "지난해", "최근", "이번", "금년", "당기", "전기", "내년", "상반기", "하반기", "연간", "연결", "별도")
_PUNCT = r"""[,.·:;'"()\[\]]"""
_GAP = "(?:" + "|".join(
    [f"[{_PARTICLES}]", r"\s", _PUNCT, *map(re.escape, _MODIFIERS), r"\d{4}년", r"\d분기", r"[1-4]Q"]
) + "){0,6}"
_CUE_TAIL = re.compile(
    _GAP + "(?:" + "|".join(map(re.escape, sorted(FINANCE_CUES, key=len, reverse=True))) + ")", re.IGNORECASE
)
_STRICT_TAIL = re.compile(
    r"(?:의)?\s{0,2}(?:" + "|".join(map(re.escape, sorted(STRICT_CUES, key=len, reverse=True))) + ")", re.IGNORECASE
)

_CODE_RE = re.compile(r"(?<![0-9A-Za-z])([0-9][0-9A-Z]{5})(?:\.(?:KS|KQ))?(?![0-9A-Za-z])")
_ASCII_NAME_RE = re.compile(r"[A-Za-z0-9&.]+")
_HANGUL_RE = re.compile(r"[가-힣]")
_ALNUM_RE = re.compile(r"[A-Za-z0-9]")


@dataclass(frozen=True)
class StockMatch:
    stock_code: str
    name: str
    rule: str   # code | alias | name
    start: int
    end: int


@dataclass(frozen=True)
class _Entry:
    text: str
    code: str
    rule: str
    needle: str
    fold: bool      # True 면 소문자로 비교 (한글이 섞인 이름)
    short: bool     # 공백 제외 2자 이하
    strict: bool    # 영문/숫자만으로 된 2자 이하


@dataclass(frozen=True)
class _Dictionary:
    code_to_name: dict
    entries: tuple


@lru_cache(maxsize=1)
def _dictionary() -> _Dictionary:
    companies = json.loads(COMPANIES_PATH.read_text(encoding="utf-8"))["companies"]
    if not companies:
        raise ValueError(f"종목 사전이 비어 있습니다: {COMPANIES_PATH}")
    code_to_name = {c["stock_code"]: c["name"] for c in companies}

    names: dict = {}
    ambiguous: set = set()

    def add(text: str, code: str) -> None:
        text = unicodedata.normalize("NFKC", text).strip()
        if not text:
            return
        prev = names.get(text)
        if prev is not None and prev != code:
            ambiguous.add(text)       # 서로 다른 종목이 같은 문자열이면 모호하므로 이름 매칭에서 제외
            return
        names[text] = code

    for c in companies:
        add(c["name"], c["stock_code"])
        compact = re.sub(r"\s+", "", c["name"])
        if compact != c["name"]:
            add(compact, c["stock_code"])
    for text in ambiguous:
        names.pop(text, None)

    rules = {t: "name" for t in names}
    aliases = json.loads(ALIASES_PATH.read_text(encoding="utf-8"))["aliases"]
    for alias, code in aliases.items():
        if code not in code_to_name:
            raise ValueError(f"별칭 '{alias}' 의 종목코드 {code} 가 종목 사전에 없습니다.")
        text = unicodedata.normalize("NFKC", alias).strip()
        names[text] = code            # 별칭이 정식 이름보다 우선한다
        rules[text] = "alias"

    entries = []
    for text, code in names.items():
        compact_len = len(re.sub(r"\s+", "", text))
        ascii_only = bool(_ASCII_NAME_RE.fullmatch(text))
        short = compact_len <= 2
        entries.append(_Entry(
            text=text, code=code, rule=rules[text],
            needle=text.lower() if _HANGUL_RE.search(text) else text,
            fold=bool(_HANGUL_RE.search(text)),
            short=short, strict=short and ascii_only,
        ))
    return _Dictionary(code_to_name=code_to_name, entries=tuple(entries))


def reload() -> None:
    """사전 캐시를 비운다 (사전 파일을 바꾼 뒤나 테스트에서 사용)."""
    _dictionary.cache_clear()


def _boundary_ok(q: str, start: int, end: int, text: str) -> bool:
    if _ALNUM_RE.fullmatch(text[0]) and start > 0 and _ALNUM_RE.fullmatch(q[start - 1]):
        return False
    if _ALNUM_RE.fullmatch(text[-1]) and end < len(q) and _ALNUM_RE.fullmatch(q[end]):
        return False
    return True


def _context_ok(q: str, end: int, e: _Entry) -> bool:
    if not e.short:
        return True
    tail = q[end:end + 40]
    return bool((_STRICT_TAIL if e.strict else _CUE_TAIL).match(tail))


def _select(cands: list) -> list:
    order = sorted(cands, key=lambda m: (m.rule != "code", -(m.end - m.start), m.rule != "alias", m.start))
    taken: list = []
    for m in order:
        if any(m.start < t.end and t.start < m.end for t in taken):
            continue
        taken.append(m)
    taken.sort(key=lambda m: m.start)
    seen: set = set()
    out = []
    for m in taken:
        if m.stock_code in seen:
            continue
        seen.add(m.stock_code)
        out.append(m)
    return out


def match_stocks(question: str) -> list:
    """질문에서 식별된 종목을 등장 순서대로 돌려준다 (규칙·위치 포함)."""
    d = _dictionary()
    q = unicodedata.normalize("NFKC", question or "")
    ql = q.lower()
    cands: list = []
    for m in _CODE_RE.finditer(q):
        code = m.group(1)
        if code in d.code_to_name:
            cands.append(StockMatch(code, d.code_to_name[code], "code", m.start(), m.end()))
    for e in d.entries:
        hay = ql if e.fold else q
        i = hay.find(e.needle)
        while i != -1:
            end = i + len(e.text)
            if _boundary_ok(q, i, end, e.text) and _context_ok(q, end, e):
                cands.append(StockMatch(e.code, e.text, e.rule, i, end))
            i = hay.find(e.needle, i + 1)
    return _select(cands)


def resolve_stock_codes(question: str) -> list:
    return [m.stock_code for m in match_stocks(question)]
