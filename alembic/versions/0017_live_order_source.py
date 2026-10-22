"""live_orders.source — 같은 KIS 계좌에 주문을 내는 주체(QUANT_AUTO·REBALANCE·MANUAL·ATR_REBAL) 구분

Revision ID: 0017
Revises: q0016_merge
Create Date: 2026-10-22

기존 행은 출처를 알 수 없어 QUANT_AUTO 로 채운다(이 컬럼 전에는 rebalance·수동 주문도 같은 경로로 기록됐다).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0017"
down_revision: Union[str, None] = "q0016_merge"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("live_orders", sa.Column("source", sa.String(20), nullable=False, server_default="QUANT_AUTO"))
    op.create_index("ix_live_orders_source_status", "live_orders", ["source", "status"])


def downgrade() -> None:
    op.drop_index("ix_live_orders_source_status", table_name="live_orders")
    op.drop_column("live_orders", "source")
