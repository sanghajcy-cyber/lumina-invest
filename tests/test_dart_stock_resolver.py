"""DART 종목 식별(엄격 정책) 검증: 일반 문장에서 평범한 단어가 종목으로 잡히지 않고, 금융 문맥에서는 잡힌다."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.services import dart_stock_resolver as r

_DATA = Path(r.__file__).resolve().parent.parent / "data"
_COMPANIES = json.loads((_DATA / "dart_companies.json").read_text(encoding="utf-8"))["companies"]
CODE = {c["name"]: c["stock_code"] for c in _COMPANIES}
ORIGINAL_15 = ["삼성전자", "SK하이닉스", "NAVER", "카카오", "현대자동차", "기아", "현대모비스", "LG화학",
               "삼성SDI", "LG에너지솔루션", "POSCO홀딩스", "셀트리온", "삼성바이오로직스", "SK", "SK이노베이션"]


def codes(question: str) -> list[str]:
    return r.resolve_stock_codes(question)


@pytest.mark.parametrize("question, names", [
    ("두산 실적", ["두산"]),
    ("기아 주가 전망 알려줘", ["기아"]),
    ("대상 공시 내용 요약", ["대상"]),
    ("기아의 올해 실적이 궁금해", ["기아"]),
    ("두산 2분기 실적", ["두산"]),
    ("동서 재무 상태", ["동서"]),
    ("노을 영업이익", ["노을"]),
    ("삼성전자 사업 내용", ["삼성전자"]),
    ("삼성전자와 SK하이닉스 비교", ["삼성전자", "SK하이닉스"]),
    ("SK하이닉스 사업", ["SK하이닉스"]),            # SK 가 겹쳐서 잡히지 않는다
    ("SK 주가", ["SK"]),
    ("SK의 주가 전망", ["SK"]),
    ("LG 주가", ["LG"]),
    ("CJ ENM 실적", ["CJ ENM"]),                      # CJ 가 따로 잡히지 않는다
    ("CJENM 실적", ["CJ ENM"]),                       # 공백 없는 표기
])
def test_positive_name_cases(question, names):
    assert codes(question) == [CODE[n] for n in names]


@pytest.mark.parametrize("question", [
    "이번 조사 대상은 누구인가",
    "동서 간의 갈등이 깊다",
    "노을이 아름다운 저녁이다",
    "투자 대상 기업을 알려줘",
    "대상 기업의 매출을 알려줘",                       # 대상 뒤에 다른 단어가 끼면 인정하지 않는다
    "노을이 지는 날 주가가 올랐다",                    # 단서가 이름 바로 뒤가 아니다
    "요즘 금리 전망 알려줘",
    "SK 실적",                                        # 그룹명일 수 있어 좁은 단서만 인정
    "SK 매출 전망",
    "LG 실적",
    "sk 주가",                                        # 영문 이름은 대소문자 구분
    "NAVERS 주가",                                    # 영문 경계
    "삼바 춤을 배웠다",
    "000000 공시",                                    # 사전에 없는 코드
])
def test_negative_cases(question):
    assert codes(question) == []


def test_codes():
    assert codes("005930 사업 내용") == ["005930"]
    assert codes("005930.KS 주가") == ["005930"]
    alnum = next(c["stock_code"] for c in _COMPANIES if not c["stock_code"].isdigit())
    assert codes(f"{alnum} 공시") == [alnum]


def test_aliases():
    assert codes("네이버 실적") == [CODE["NAVER"]]
    assert codes("현대차 사업") == [CODE["현대자동차"]]
    assert codes("삼전 주가") == [CODE["삼성전자"]]
    assert codes("삼바 주가") == [CODE["삼성바이오로직스"]]
    assert codes("하이닉스 사업") == [CODE["SK하이닉스"]]


def test_order_follows_question_and_no_duplicates():
    assert codes("현대자동차와 삼성전자, 다시 삼성전자") == [CODE["현대자동차"], CODE["삼성전자"]]


@pytest.mark.parametrize("name", ORIGINAL_15)
def test_original_15_companies_still_resolve(name):
    assert codes(f"{name} 주가") == [CODE[name]]


def test_dictionary_is_consistent():
    data = json.loads((_DATA / "dart_companies.json").read_text(encoding="utf-8"))
    assert data["count"] == len(data["companies"]) == len({c["stock_code"] for c in data["companies"]})
    aliases = json.loads((_DATA / "dart_aliases.json").read_text(encoding="utf-8"))["aliases"]
    assert all(code in {c["stock_code"] for c in data["companies"]} for code in aliases.values())


def test_resolve_dart_stocks_caps_at_two():
    from app.services import dart_rag
    assert dart_rag.resolve_dart_stocks("삼성전자 SK하이닉스 현대자동차 사업 비교") == [
        CODE["삼성전자"], CODE["SK하이닉스"]]


def test_kepco_alias_and_longest_match():
    from app.services.dart_stock_resolver import resolve_stock_codes

    assert resolve_stock_codes("한국전력 재무 상태") == ["015760"]
    assert resolve_stock_codes("한전 실적 알려줘") == ["015760"]
    assert resolve_stock_codes("한국전력공사 공시") == ["015760"]
    # 정식명이 더 긴 계열사는 별칭이 가로채지 않는다
    assert resolve_stock_codes("한전KPS 실적") == ["051600"]
    assert resolve_stock_codes("한전기술 매출") == ["052690"]
