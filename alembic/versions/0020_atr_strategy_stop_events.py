"""ATR 전략 5단계: 손절 이벤트

Revision ID: 0020
Revises: 0019
Create Date: 2026-10-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0020"
down_revision: Union[str, None] = "0019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

U = lambda: postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "strategy_stop_events",
        sa.Column("id", U(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("plan_id", U(), sa.ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", U(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("symbol", sa.String(6), nullable=False),
        sa.Column("name", sa.String(100), nullable=False, server_default=""),
        sa.Column("seq", sa.Integer, nullable=False, server_default="0"),
        sa.Column("status", sa.String(10), nullable=False, server_default="waiting"),
        sa.Column("trigger_price", sa.Float, nullable=False, server_default="0"),
        sa.Column("avg_price", sa.Float, nullable=False, server_default="0"),
        sa.Column("atr", sa.Float, nullable=True),
        sa.Column("stop_price", sa.Float, nullable=False, server_default="0"),
        sa.Column("basis", sa.String(40), nullable=False, server_default=""),
        sa.Column("quantity", sa.Integer, nullable=False, server_default="0"),
        sa.Column("client_order_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("live_order_id", U(), sa.ForeignKey("live_orders.id"), nullable=True),
        sa.Column("filled_qty", sa.Integer, nullable=False, server_default="0"),
        sa.Column("avg_fill_price", sa.Float, nullable=False, server_default="0"),
        sa.Column("reason", sa.String(300), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("plan_id", "trade_date", "symbol", "seq", name="uq_strategy_stop_events_seq"),
    )
    op.create_index("ix_strategy_stop_events_plan_day_symbol", "strategy_stop_events", ["plan_id", "trade_date", "symbol"])


def downgrade() -> None:
    op.drop_index("ix_strategy_stop_events_plan_day_symbol", table_name="strategy_stop_events")
    op.drop_table("strategy_stop_events")
