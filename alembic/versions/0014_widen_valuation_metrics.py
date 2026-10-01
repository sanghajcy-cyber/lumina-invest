"""valuation_metrics 정밀도 확장

실측 결과 per 최대값이 약 3.08억으로 NUMERIC(10,2) 범위(최대 9999만)를
초과하고, 최소값은 0.000008 수준으로 scale=2로는 반올림되어 소실됨.
4개 비율 컬럼 모두 NUMERIC(20,6)으로 넓힌다.

Revision ID: 0014
Revises: 0013
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0014"
down_revision: Union[str, None] = "0013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLS = ["per", "pbr", "pcr", "psr"]


def upgrade() -> None:
    for col in _COLS:
        op.alter_column("valuation_metrics", col, type_=sa.Numeric(20, 6), existing_type=sa.Numeric(10, 2))


def downgrade() -> None:
    for col in _COLS:
        op.alter_column("valuation_metrics", col, type_=sa.Numeric(10, 2), existing_type=sa.Numeric(20, 6))
