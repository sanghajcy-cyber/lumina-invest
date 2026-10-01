"""factor_scores.composite_top_pct 정밀도 확장

실측 결과 composite_top_pct 최대값이 100.0 (1위 종목의 백분위)이라
NUMERIC(6,4) 범위(최대 99.9999)를 초과함.

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0015"
down_revision: Union[str, None] = "0014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("factor_scores", "composite_top_pct", type_=sa.Numeric(10, 4), existing_type=sa.Numeric(6, 4))


def downgrade() -> None:
    op.alter_column("factor_scores", "composite_top_pct", type_=sa.Numeric(6, 4), existing_type=sa.Numeric(10, 4))
