"""financial_facts.context_ref 컬럼 길이 확장 (최대 실측 1,191자 -> TEXT로 변경)

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "q0011"
down_revision: Union[str, None] = "q0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("financial_facts", "context_ref", type_=sa.Text(), existing_type=sa.String(500))


def downgrade() -> None:
    op.alter_column("financial_facts", "context_ref", type_=sa.String(500), existing_type=sa.Text())
