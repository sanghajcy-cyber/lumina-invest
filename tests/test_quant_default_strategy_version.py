"""DEFAULT_STRATEGY_VERSION 환경변수 오버라이드 (코드 기본값은 composite_v1)."""
from __future__ import annotations

import pytest

from app.services import quant_financials as qf

ENV = qf.ENV_DEFAULT_STRATEGY_VERSION


def test_env_name_is_stable():
    assert ENV == "QUANT_DEFAULT_STRATEGY_VERSION"


def test_default_is_v1_when_unset(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert qf._default_strategy_version() == "composite_v1"


@pytest.mark.parametrize("raw", ["", "   ", "\t"])
def test_blank_env_falls_back_to_v1(monkeypatch, raw):
    monkeypatch.setenv(ENV, raw)
    assert qf._default_strategy_version() == "composite_v1"


def test_env_overrides_to_v2_and_strips(monkeypatch):
    monkeypatch.setenv(ENV, "  composite_v2 ")
    assert qf._default_strategy_version() == "composite_v2"
