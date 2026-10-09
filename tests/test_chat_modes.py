"""로보 어드바이저 채팅의 답변 엔진 선택(llm_mode: ollama | openai | rag) 검증.

- OpenAIClient 가 사용자 키로 OpenAI Chat Completions 를 호출하는지 (httpx MockTransport)
- /api/chat 이 모드별로 올바른 경로를 타는지 (DB·RAG·에이전트는 스텁)
"""
from __future__ import annotations

import json

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.lib import llm_client as llm_client_mod
from app.lib.llm_client import OpenAIClient

pytest.importorskip("langgraph")  # /api/chat 라우터는 LangGraph 에이전트를 임포트한다
from app.routes import chat as chat_mod  # noqa: E402
from app.lib.jwt_auth import get_current_user_any  # noqa: E402
from app.database.postgres import get_pg_session  # noqa: E402


# ── OpenAIClient ──────────────────────────────────────────────────────────────

def _patch_httpx(monkeypatch, handler):
    """app.lib.llm_client 안의 httpx.AsyncClient 가 MockTransport 를 쓰도록 바꾼다."""
    real = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real(*args, **kwargs)

    monkeypatch.setattr(llm_client_mod.httpx, "AsyncClient", factory)


def test_openai_client_sends_key_and_maps_options(monkeypatch):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "안녕하세요"}}]})

    _patch_httpx(monkeypatch, handler)
    client = OpenAIClient("sk-test-123", model="gpt-4o-mini")

    import asyncio
    out = asyncio.run(client.chat("ignored-model", [{"role": "user", "content": "hi"}],
                                  {"temperature": 0.1, "num_predict": 2048}))

    assert out == "안녕하세요"
    assert seen["url"] == f"{settings.OPENAI_BASE_URL.rstrip('/')}/v1/chat/completions"
    assert seen["auth"] == "Bearer sk-test-123"
    assert seen["body"]["model"] == "gpt-4o-mini"
    assert seen["body"]["max_tokens"] == 2048
    assert seen["body"]["temperature"] == 0.1
    assert seen["body"]["messages"] == [{"role": "user", "content": "hi"}]


def test_openai_client_defaults_and_rejects_empty_key():
    with pytest.raises(ValueError):
        OpenAIClient("   ")
    c = OpenAIClient("sk-x")
    assert c.model == settings.OPENAI_MODEL


# ── /api/chat 모드 분기 ───────────────────────────────────────────────────────

USER = {"id": "00000000-0000-0000-0000-000000000001", "name": "홍길동", "email": "h@x.com", "client_id": "C1", "roles": ["user"]}
DOCS = [
    {"text": "삼성전자 2025년 2분기 영업이익은 ...", "url": "https://ex.com/a", "title": "삼성전자 실적", "source": "news", "score": 0.91},
    {"text": "반도체 업황 전망 ...", "url": "", "title": "", "source": "upload", "score": 0.77},
]


class _FakeDb:
    """chat 라우터가 호출하는 DB 메서드만 흉내 (저장은 무시)."""
    def add(self, *_): pass
    async def commit(self): pass
    async def rollback(self): pass
    async def execute(self, *_):
        class _R:
            def scalar_one_or_none(self): return None
            def scalars(self): return self
            def all(self): return []
        return _R()


@pytest.fixture
def chat_app(monkeypatch):
    calls = {"agent": [], "rag": []}

    async def fake_conv(db, user_id, cid):
        return cid or "11111111-1111-1111-1111-111111111111"

    async def fake_rag(question, top_k=5, **kw):
        calls["rag"].append(question)
        return DOCS

    async def fake_agent(db, llm, llm_model, question, history, rag_context=""):
        calls["agent"].append({"llm": llm, "model": llm_model, "rag_context": rag_context})
        return {"answer": f"[{llm_model}] 답변", "steps": [], "citations": []}

    async def noop(*a, **k): pass

    monkeypatch.setattr(chat_mod, "_get_or_create_conversation", fake_conv)
    monkeypatch.setattr(chat_mod, "rag_search", fake_rag)
    monkeypatch.setattr(chat_mod, "run_agent", fake_agent)
    monkeypatch.setattr(chat_mod, "set_active_conversation", noop)
    monkeypatch.setattr(chat_mod, "get_llm_client", lambda: "OLLAMA-CLIENT")

    app = FastAPI()
    app.include_router(chat_mod.router)

    async def fake_db():
        yield _FakeDb()

    app.dependency_overrides[get_current_user_any] = lambda: USER
    app.dependency_overrides[get_pg_session] = fake_db
    return TestClient(app), calls


def test_rag_mode_falls_back_to_rule_answer_when_llm_unavailable(chat_app):
    """LLM 클라이언트가 chat 을 제공하지 않거나 실패하면 규칙 기반 답변("LLM 미사용")을 그대로 돌려준다."""
    client, calls = chat_app                      # get_llm_client 스텁이 문자열 → llm.chat 실패 → 폴백
    res = client.post("/api/chat", json={"question": "삼성전자 실적", "llm_mode": "rag"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["mode"] == "rag" and body["llm_used"] is False
    assert len(body["chunks"]) == 2
    assert calls["agent"] == []                    # LangGraph 에이전트 미호출
    assert calls["rag"] == ["삼성전자 실적"]
    assert "LLM 미사용" in body["answer"]
    assert "삼성전자 실적" in body["answer"] and "https://ex.com/a" in body["answer"]
    assert body["citations"][0]["title"] == "삼성전자 실적"
    assert body["citations"][1]["title"] == "upload"  # 제목 없으면 source 로 대체


def test_rag_mode_uses_llm_summary_when_available(chat_app, monkeypatch):
    """검색 근거가 있고 LLM 이 응답하면 그 요약이 답변이 되고 model·llm_used 가 채워진다. 근거 목록(chunks·citations)은 그대로."""
    client, calls = chat_app
    seen = {}

    class FakeLLM:
        async def chat(self, model, messages, options=None):
            seen["model"], seen["messages"], seen["options"] = model, messages, options
            return "  삼성전자 실적 근거에 따르면 2분기 영업이익이 개선되었습니다. (출처: 삼성전자 실적)  "

    monkeypatch.setattr(chat_mod, "get_llm_client", lambda: FakeLLM())
    res = client.post("/api/chat", json={"question": "삼성전자 실적", "llm_mode": "rag"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["llm_used"] is True and body["model"] == settings.LLM_MODEL
    assert body["answer"].startswith("삼성전자 실적 근거에 따르면")
    assert len(body["chunks"]) == 2 and calls["agent"] == []
    assert seen["model"] == settings.LLM_MODEL and "삼성전자 실적" in seen["messages"][1]["content"]
    assert seen["options"]["num_predict"] == 160


def test_rag_mode_with_no_docs(chat_app, monkeypatch):
    client, _ = chat_app

    async def empty(*a, **k): return []
    monkeypatch.setattr(chat_mod, "rag_search", empty)
    res = client.post("/api/chat", json={"question": "없는 내용", "llm_mode": "rag", "use_rag": False})
    assert res.status_code == 200
    assert res.json()["chunks"] == []
    assert "검색된 참고 문서" in res.json()["answer"]


def test_ollama_mode_uses_server_llm(chat_app):
    client, calls = chat_app
    res = client.post("/api/chat", json={"question": "안녕"})  # llm_mode 기본값 ollama
    assert res.status_code == 200, res.text
    assert res.json()["mode"] == "ollama"
    assert calls["agent"][0]["llm"] == "OLLAMA-CLIENT"
    assert calls["agent"][0]["model"] == settings.LLM_MODEL
    assert "삼성전자 실적" in calls["agent"][0]["rag_context"]


def test_openai_mode_requires_key_and_uses_openai_client(chat_app, monkeypatch):
    client, calls = chat_app
    monkeypatch.setattr(settings, "OPENAI_API_KEY", "")

    res = client.post("/api/chat", json={"question": "안녕", "llm_mode": "openai"})
    assert res.status_code == 400
    assert "OpenAI API 키" in res.json()["detail"]
    assert calls["agent"] == []

    res = client.post("/api/chat", json={"question": "안녕", "llm_mode": "openai",
                                         "openai_api_key": "sk-abc", "openai_model": "gpt-4.1-mini"})
    assert res.status_code == 200, res.text
    assert res.json()["mode"] == "openai"
    llm = calls["agent"][0]["llm"]
    assert isinstance(llm, OpenAIClient)
    assert llm.model == "gpt-4.1-mini"
    assert calls["agent"][0]["model"] == "gpt-4.1-mini"
    assert "sk-abc" not in res.text  # 키는 응답에 노출되지 않는다


def test_openai_http_errors_are_mapped(chat_app, monkeypatch):
    client, _ = chat_app

    def raiser(status):
        async def fake_agent(*a, **k):
            req = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
            raise httpx.HTTPStatusError("err", request=req, response=httpx.Response(status, request=req))
        return fake_agent

    monkeypatch.setattr(chat_mod, "run_agent", raiser(401))
    res = client.post("/api/chat", json={"question": "q", "llm_mode": "openai", "openai_api_key": "sk-bad"})
    assert res.status_code == 401 and "유효하지 않" in res.json()["detail"]

    monkeypatch.setattr(chat_mod, "run_agent", raiser(429))
    res = client.post("/api/chat", json={"question": "q", "llm_mode": "openai", "openai_api_key": "sk-x"})
    assert res.status_code == 429


def test_unknown_mode_rejected(chat_app):
    client, _ = chat_app
    res = client.post("/api/chat", json={"question": "q", "llm_mode": "claude"})
    assert res.status_code == 422



# ── DART 공시 RAG 연동 ───────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _no_real_dart(monkeypatch):
    """기본은 DART 검색을 끈다 (테스트가 실제 Qdrant/OpenAI 를 호출하지 않도록). DART 테스트는 개별로 덮어쓴다."""
    async def none(*a, **k):
        return [], None
    monkeypatch.setattr(chat_mod, "dart_search_for_question", none)


DART_DOC = {
    "text": "삼성전자 주요 제품은 DX 부문과 DS 부문으로 구분됩니다.",
    "title": "삼성전자 반기보고서 2026 2Q | II. 사업의 내용 > 주요 제품 및 서비스",
    "source": "dart:20260814003699",
    "score": 0.687,
    "url": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260814003699",
    "stock_code": "005930",
}


def test_rag_mode_merges_dart_docs(chat_app, monkeypatch):
    client, calls = chat_app
    seen = {}

    async def fake_dart(question, *, as_of=None, **k):
        seen["q"], seen["as_of"] = question, as_of
        return [DART_DOC], None

    monkeypatch.setattr(chat_mod, "dart_search_for_question", fake_dart)
    res = client.post("/api/chat", json={"question": "삼성전자 실적", "llm_mode": "rag", "as_of": "20220331"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert len(body["chunks"]) == 3                       # rag_search 2건 + DART 1건
    assert body["chunks"][-1]["source"] == "dart:20260814003699"
    assert body["citations"][-1]["url"].startswith("https://dart.fss.or.kr/")
    assert "dart_error" not in body
    assert seen == {"q": "삼성전자 실적", "as_of": "20220331"}
    assert calls["rag"] == ["삼성전자 실적"]                # 기존 rag_search 는 그대로 호출된다


def test_dart_failure_is_surfaced_and_rag_results_kept(chat_app, monkeypatch):
    client, _ = chat_app

    async def failing(*a, **k):
        return [], "DART 검색 실패: 서버 OPENAI_API_KEY가 설정되지 않았습니다"

    monkeypatch.setattr(chat_mod, "dart_search_for_question", failing)
    res = client.post("/api/chat", json={"question": "삼성전자 실적", "llm_mode": "rag"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["dart_error"].startswith("DART 검색 실패")
    assert len(body["chunks"]) == 2                       # 기존 검색 결과는 유지


def test_ollama_mode_puts_dart_into_rag_context(chat_app, monkeypatch):
    client, calls = chat_app

    async def fake_dart(question, *, as_of=None, **k):
        return [DART_DOC], None

    monkeypatch.setattr(chat_mod, "dart_search_for_question", fake_dart)
    res = client.post("/api/chat", json={"question": "삼성전자 실적"})
    assert res.status_code == 200, res.text
    ctx = calls["agent"][0]["rag_context"]
    assert "삼성전자 실적" in ctx                           # 기존 근거
    assert DART_DOC["title"] in ctx and "DX 부문" in ctx   # DART 근거


def test_dart_not_called_when_rag_disabled_outside_rag_mode(chat_app, monkeypatch):
    client, calls = chat_app
    called = []

    async def spy(*a, **k):
        called.append(a)
        return [DART_DOC], None

    monkeypatch.setattr(chat_mod, "dart_search_for_question", spy)
    res = client.post("/api/chat", json={"question": "삼성전자 실적", "use_rag": False})
    assert res.status_code == 200, res.text
    assert called == []
    assert calls["agent"][0]["rag_context"] == ""


def test_dart_helper_skips_when_no_company(monkeypatch):
    import asyncio
    from app.services import dart_rag

    async def boom(*a, **k):
        raise AssertionError("종목이 없으면 dart_search 를 호출하면 안 된다")

    monkeypatch.setattr(dart_rag, "dart_search", boom)
    assert asyncio.run(dart_rag.dart_search_for_question("요즘 금리 전망 알려줘")) == ([], None)


def test_dart_helper_passes_codes_and_as_of(monkeypatch):
    import asyncio
    from app.services import dart_rag
    calls = []

    async def fake(query, stock_code, *, as_of=None, top_k=5, **k):
        calls.append((stock_code, as_of, top_k))
        return [{"stock_code": stock_code}]

    monkeypatch.setattr(dart_rag, "dart_search", fake)
    docs, err = asyncio.run(dart_rag.dart_search_for_question("삼성전자와 SK하이닉스 비교", as_of="20220331"))
    assert err is None and len(docs) == 2
    assert calls == [("005930", "20220331", 3), ("000660", "20220331", 3)]   # SK(034730) 오탐 없음, .KS 접미사 제거


def test_dart_helper_returns_error_instead_of_raising(monkeypatch):
    import asyncio
    from app.services import dart_rag

    async def fail(*a, **k):
        raise dart_rag.DartCollectionError("컬렉션 없음")

    monkeypatch.setattr(dart_rag, "dart_search", fail)
    docs, err = asyncio.run(dart_rag.dart_search_for_question("삼성전자 사업 내용"))
    assert docs == [] and "컬렉션 없음" in err
