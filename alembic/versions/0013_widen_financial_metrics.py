"""financial_metrics 정밀도 확장

pit_financial_core_mart_fullmarket.parquet 실측 결과 revenue_ttm/equity_latest
등에서 raw XBRL 파싱 이상치가 그대로 승계되어 절대값 10^18 이상 값이 존재함
(financial_facts.amount_value와 동일 원인). 원본 보존 목적상 걸러내지 않고
컬럼 범위를 넓힌다.

Revision ID: 0013
Revises: 0012
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "q0013"
down_revision: Union[str, None] = "q0012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLS = ["revenue_ttm", "net_income_ttm", "equity_latest", "operating_cash_flow_ttm"]


def upgrade() -> None:
    for col in _COLS:
        op.alter_column("financial_metrics", col, type_=sa.Numeric(30, 2), existing_type=sa.Numeric(20, 2))


def downgrade() -> None:
    for col in _COLS:
        op.alter_column("financial_metrics", col, type_=sa.Numeric(20, 2), existing_type=sa.Numeric(30, 2))
