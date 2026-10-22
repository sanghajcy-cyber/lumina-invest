"""ATR 전략 4단계: 실행 상태(trigger·attempt·시각)와 회차 체결 반영 수량

Revision ID: 0019
Revises: 0018
Create Date: 2026-10-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0019"
down_revision: Union[str, None] = "0018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("strategy_runs", sa.Column("trigger", sa.String(10), nullable=False, server_default="auto"))
    op.add_column("strategy_runs", sa.Column("attempt", sa.Integer, nullable=False, server_default="0"))
    op.add_column("strategy_runs", sa.Column("started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("strategy_runs", sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("strategy_slices", sa.Column("attempt", sa.Integer, nullable=False, server_default="0"))
    op.add_column("strategy_slices", sa.Column("filled_qty", sa.Integer, nullable=False, server_default="0"))
    op.add_column("strategy_slices", sa.Column("avg_fill_price", sa.Float, nullable=False, server_default="0"))
    op.create_unique_constraint("uq_strategy_slices_round", "strategy_slices", ["run_id", "attempt", "symbol", "side", "seq"])


def downgrade() -> None:
    op.drop_constraint("uq_strategy_slices_round", "strategy_slices", type_="unique")
    for col in ("avg_fill_price", "filled_qty", "attempt"):
        op.drop_column("strategy_slices", col)
    for col in ("finished_at", "started_at", "attempt", "trigger"):
        op.drop_column("strategy_runs", col)
