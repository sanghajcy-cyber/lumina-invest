"""financial_facts.amount_value 정밀도 확장

실측 결과 일부 세그먼트/축 분해 context에서 raw XBRL 파싱 이상치로 보이는
절대값 10^18 이상인 amount_value가 존재함(예: 8.78e18). 원본 보존이 목적인
테이블이므로 걸러내지 않고 컬럼 범위를 넓혀 그대로 적재한다.

Revision ID: 0012
Revises: 0011
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0012"
down_revision: Union[str, None] = "0011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "financial_facts", "amount_value",
        type_=sa.Numeric(30, 2), existing_type=sa.Numeric(20, 2),
    )


def downgrade() -> None:
    op.alter_column(
        "financial_facts", "amount_value",
        type_=sa.Numeric(20, 2), existing_type=sa.Numeric(30, 2),
    )
