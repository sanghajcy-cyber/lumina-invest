"""DART 공시 RAG 검색 (Qdrant 컬렉션 settings.DART_COLLECTION, 기본 dart_disclosures_3small_v3).

기존 rag_pipeline.rag_search(Ollama nomic-embed-text 768차원, fin_chunks/law_chunks)와 완전히 분리된 경로다.
- 질의 임베딩은 컬렉션을 만든 모델(text-embedding-3-small, 1536차원)로 서버 OPENAI_API_KEY를 써서 강제한다.
  사용자(브라우저) 키는 쓰지 않는다. 채팅/최종 LLM 호출의 키 정책과 무관하다.
- 컬렉션을 자동 생성하지 않는다. 없거나 차원/거리가 다르면 DartCollectionError로 실패한다.
- 예외를 삼켜 빈 목록으로 바꾸지 않는다. 빈 목록은 "기준일에 유효한 공시가 없음"일 때만 반환된다.
- PIT 범위(종목 + as_of)는 Qdrant payload만으로 정한다: rcept_dt <= as_of 인 공시 중 문서 가족
  (document_family_id)별 최신 버전(정정 포함) 하나를 고르고, period_end가 최근인 가족 filings개를 쓴다.
"""
from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import httpx
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qm

from app.config import settings

DART_EMBED_MODEL = "text-embedding-3-small"
DART_EMBED_DIM = 1536
TOP_SECTIONS = ("business_content", "management_discussion_analysis")
SCOPE_FIELDS = ["rcept_no", "rcept_dt", "document_family_id", "version_seq", "report_type",
                "fiscal_year", "report_period", "is_correction", "period_end", "stock_code"]

_AS_OF_RE = re.compile(r"^\d{8}$")
_SCOPE_TTL = 600.0        # 종목별 공시 목록 캐시(초)
_COLLECTION_TTL = 60.0    # 컬렉션 검증 결과 캐시(초)
_scope_cache: dict[tuple[str, str], tuple[float, dict[str, dict]]] = {}
_collection_ok: dict[str, float] = {}


class DartRagError(RuntimeError):
    """DART RAG 검색 실패의 공통 부모."""


class DartCollectionError(DartRagError):
    """컬렉션이 없거나 벡터 구성이 질의 임베딩과 맞지 않음."""


class DartEmbeddingError(DartRagError):
    """서버 키 없음 또는 질의 임베딩 호출 실패."""


def clear_cache() -> None:
    _scope_cache.clear()
    _collection_ok.clear()


def select_scope(rows: Iterable[dict], as_of: str, filings: int = 1,
                 report_type: str | None = None, fiscal_year: int | None = None) -> list[dict]:
    """공시 메타(rcept_no 단위, 한 종목)에서 as_of 시점에 유효한 검색 범위를 고른다."""
    valid = [r for r in rows
             if str(r.get("rcept_dt") or "") <= as_of
             and (not report_type or r.get("report_type") == report_type)
             and (fiscal_year is None or r.get("fiscal_year") == fiscal_year)]
    latest: dict[str, dict] = {}
    for r in sorted(valid, key=lambda r: (str(r["rcept_dt"]), str(r["rcept_no"]))):
        latest[r.get("document_family_id") or r["rcept_no"]] = r   # 가족별 as_of 시점 최신 버전
    ordered = sorted(latest.values(), key=lambda r: (str(r.get("period_end") or ""), str(r["rcept_dt"])), reverse=True)
    return ordered[:max(filings, 0)]


def _error_type(resp: httpx.Response) -> str:
    try:
        err = resp.json().get("error") or {}
        return str(err.get("code") or err.get("type") or "unknown")[:60]
    except Exception:
        return "unknown"


def _server_key() -> str:
    key = (settings.OPENAI_API_KEY or "").strip()
    if not key:
        raise DartEmbeddingError("서버 OPENAI_API_KEY가 설정되지 않았습니다 (DART 질의 임베딩은 서버 키를 사용합니다).")
    return key


async def _embed_query(query: str) -> list[float]:
    key = _server_key()
    url = settings.DART_OPENAI_BASE_URL.rstrip("/") + "/v1/embeddings"
    last = "unknown"
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=20.0) as http:
                resp = await http.post(url, headers={"Authorization": f"Bearer {key}"},
                                       json={"model": DART_EMBED_MODEL, "input": query})
        except httpx.HTTPError as exc:
            last = type(exc).__name__           # 메시지에는 URL 등이 들어갈 수 있어 유형만 남긴다
        else:
            if resp.status_code == 200:
                vector = resp.json()["data"][0]["embedding"]
                if len(vector) != DART_EMBED_DIM:
                    raise DartEmbeddingError(f"질의 임베딩 차원 {len(vector)} != {DART_EMBED_DIM}")
                return vector
            last = f"HTTP {resp.status_code} ({_error_type(resp)})"   # 응답 본문은 키 일부를 되풀이할 수 있어 싣지 않는다
            if resp.status_code not in (429, 500, 502, 503, 504):
                break
        await asyncio.sleep(0.5 * (attempt + 1))
    raise DartEmbeddingError(f"질의 임베딩 실패: {last}")


async def _ensure_collection(client: AsyncQdrantClient, name: str) -> None:
    if time.monotonic() < _collection_ok.get(name, 0.0):
        return
    if not await client.collection_exists(name):
        raise DartCollectionError(f"Qdrant 컬렉션 '{name}'이 없습니다. 자동 생성하지 않습니다.")
    vectors = (await client.get_collection(name)).config.params.vectors
    if isinstance(vectors, dict):
        raise DartCollectionError(f"컬렉션 '{name}'이 이름 있는 벡터 구성입니다 (단일 {DART_EMBED_DIM}차원 벡터가 필요).")
    if vectors.size != DART_EMBED_DIM:
        raise DartCollectionError(f"컬렉션 '{name}'의 벡터 차원 {vectors.size} != {DART_EMBED_DIM} ({DART_EMBED_MODEL}).")
    if vectors.distance != qm.Distance.COSINE:
        raise DartCollectionError(f"컬렉션 '{name}'의 거리 {vectors.distance} != Cosine.")
    _collection_ok[name] = time.monotonic() + _COLLECTION_TTL


async def _load_stock_filings(client: AsyncQdrantClient, name: str, stock_code: str) -> dict[str, dict]:
    cached = _scope_cache.get((name, stock_code))
    if cached and time.monotonic() - cached[0] < _SCOPE_TTL:
        return cached[1]
    flt = qm.Filter(must=[qm.FieldCondition(key="stock_code", match=qm.MatchValue(value=stock_code))])
    filings: dict[str, dict] = {}
    offset = None
    while True:
        points, offset = await client.scroll(name, scroll_filter=flt, limit=1000, offset=offset,
                                             with_payload=SCOPE_FIELDS, with_vectors=False)
        for p in points:
            payload = p.payload or {}
            rcept_no = payload.get("rcept_no")
            if rcept_no and rcept_no not in filings:
                filings[rcept_no] = {k: payload.get(k) for k in SCOPE_FIELDS}
        if offset is None:
            break
    if len(_scope_cache) > 2000:
        _scope_cache.clear()
    _scope_cache[(name, stock_code)] = (time.monotonic(), filings)
    return filings


def _to_result(point: Any) -> dict:
    p = point.payload or {}
    period = f" {p['report_period']}" if p.get("report_period") else ""
    meta = {k: p.get(k) for k in ("stock_code", "company_name", "rcept_no", "rcept_dt", "report_type", "fiscal_year",
                                   "report_period", "document_family_id", "version_seq", "is_correction",
                                   "section_code", "section_path", "chunk_id", "chunk_index", "window_index", "has_table")}
    return {
        "text": p.get("content") or "",
        "title": f"{p.get('company_name', '')} {p.get('report_type', '')} {p.get('fiscal_year', '')}{period} | {p.get('section_path', '')}".strip(),
        "source": f"dart:{p.get('rcept_no', '')}",
        "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={p.get('rcept_no', '')}",
        "score": float(point.score),
        **meta,
    }


async def dart_search(query: str, stock_code: str, *, as_of: str | None = None, top_k: int = 5,
                      filings: int = 1, report_type: str | None = None, fiscal_year: int | None = None,
                      top_section: str | None = None) -> list[dict]:
    """종목 + 기준일(PIT)로 범위를 정한 뒤 그 공시들 안에서 질의와 가까운 청크를 반환한다.

    실패는 예외로 드러낸다(DartCollectionError / DartEmbeddingError / Qdrant 연결 오류 등).
    반환이 빈 목록이면 그 기준일에 유효한 공시(또는 일치하는 청크)가 없다는 뜻이다.
    """
    if not (query or "").strip():
        raise ValueError("query가 비어 있습니다.")
    if not (stock_code or "").strip():
        raise ValueError("stock_code는 필수입니다 (공시 범위를 종목으로 정합니다).")
    as_of = as_of or datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
    if not _AS_OF_RE.match(as_of):
        raise ValueError(f"as_of는 YYYYMMDD 형식이어야 합니다: {as_of!r}")
    if top_section is not None and top_section not in TOP_SECTIONS:
        raise ValueError(f"top_section은 {TOP_SECTIONS} 중 하나여야 합니다: {top_section!r}")
    if not 1 <= top_k <= 50:
        raise ValueError("top_k는 1~50 이어야 합니다.")
    _server_key()  # 설정 오류는 공시가 없는 종목이어도 숨기지 않고 즉시 드러낸다

    name = settings.DART_COLLECTION
    client = AsyncQdrantClient(url=settings.QDRANT_URL)
    try:
        await _ensure_collection(client, name)
        all_filings = await _load_stock_filings(client, name, stock_code)
        scope = select_scope(all_filings.values(), as_of, filings, report_type, fiscal_year)
        if not scope:
            return []
        vector = await _embed_query(query)
        must = [qm.FieldCondition(key="rcept_no", match=qm.MatchAny(any=[r["rcept_no"] for r in scope])),
                qm.FieldCondition(key="stock_code", match=qm.MatchValue(value=stock_code))]
        if top_section:
            must.append(qm.FieldCondition(key="top_section", match=qm.MatchValue(value=top_section)))
        resp = await client.query_points(collection_name=name, query=vector, query_filter=qm.Filter(must=must),
                                         limit=max(top_k * 3, 20), with_payload=True)
    finally:
        await client.close()

    seen: set[str] = set()
    results: list[dict] = []
    for point in resp.points:          # 긴 청크는 윈도우별 point라 같은 chunk_id가 여러 번 나올 수 있다
        cid = (point.payload or {}).get("chunk_id")
        if cid in seen:
            continue
        seen.add(cid)
        results.append(_to_result(point))
        if len(results) == top_k:
            break
    return results



# ── 채팅 연동 헬퍼 ───────────────────────────────────────────────────
import logging as _logging
import re as _re
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone

_log = _logging.getLogger(__name__)
_KST = _timezone(_timedelta(hours=9))
MAX_CHAT_STOCKS = 2          # 질문 하나에서 DART 를 검색할 최대 종목 수
CHAT_TOP_K_PER_STOCK = 3     # 종목당 반환 청크 수


def kst_today() -> str:
    return _datetime.now(_KST).strftime("%Y%m%d")


def resolve_dart_stocks(question: str) -> list[str]:
    """질문에서 DART 검색 대상 6자리 종목코드를 뽑는다 (질문 속 등장 순서, 최대 MAX_CHAT_STOCKS).

    extract_mentioned_symbols 는 부분 문자열 매칭이라 'SK하이닉스' 에서 'SK' 도 잡힌다.
    다른 매칭 이름에 포함되는 더 짧은 이름은 버린다. 심볼의 '.KS' 같은 접미사는 제거한다.
    """
    from app.services.graph_service import _COMPANIES, extract_mentioned_symbols

    names = {c["symbol"]: c["name"] for c in _COMPANIES}
    syms = list(dict.fromkeys(extract_mentioned_symbols(question)))
    keep = [s for s in syms
            if not any(names[s] != names[o] and names[s] in names[o] for o in syms)]
    keep.sort(key=lambda s: question.find(names[s]))
    codes: list[str] = []
    for s in keep:
        code = s.split(".")[0]
        if len(code) == 6 and code.isdigit() and code not in codes:
            codes.append(code)
    return codes[:MAX_CHAT_STOCKS]


async def dart_search_for_question(question: str, *, as_of: str | None = None,
                                   top_k_per_stock: int = CHAT_TOP_K_PER_STOCK):
    """채팅용: (docs, error) 를 돌려준다. 이 함수는 예외를 던지지 않는다.

    - 종목이 식별되지 않으면 ([], None): DART 검색을 건너뛴다.
    - 실패하면 빈 결과로 위장하지 않고 error 문자열을 돌려준다 (호출부가 응답에 싣는다).
    - as_of 가 없으면 KST 오늘.
    """
    if as_of is not None and not _re.fullmatch(r"\d{8}", as_of):
        return [], "as_of 는 YYYYMMDD 형식이어야 합니다."
    try:
        codes = resolve_dart_stocks(question)
    except Exception:
        _log.exception("DART 종목 식별 실패")
        return [], "DART 종목 식별 중 오류가 발생했습니다."
    if not codes:
        return [], None

    as_of = as_of or kst_today()
    docs: list[dict] = []
    for code in codes:
        try:
            docs.extend(await dart_search(question, code, as_of=as_of, top_k=top_k_per_stock))
        except DartRagError as exc:
            _log.error("DART 검색 실패 stock=%s as_of=%s: %s", code, as_of, exc)
            return docs, f"DART 검색 실패: {exc}"
        except Exception:
            _log.exception("DART 검색 중 예기치 않은 오류 stock=%s", code)
            return docs, "DART 검색 중 예기치 않은 오류가 발생했습니다."
    return docs, None
