"""월초 ATR 리밸런싱 전략: 플랜·목표·전용 장부·실행 스냅샷·시분할 회차

Revision ID: 0018
Revises: 0017
Create Date: 2026-10-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0018"
down_revision: Union[str, None] = "0017"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

U = lambda: postgresql.UUID(as_uuid=True)


def _pk():
    return sa.Column("id", U(), primary_key=True, server_default=sa.text("gen_random_uuid()"))


def _ts(name):
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


def upgrade() -> None:
    op.create_table(
        "strategy_plans",
        _pk(),
        sa.Column("user_id", U(), sa.ForeignKey("users.id"), nullable=False, unique=True),
        sa.Column("strategy", sa.String(20), nullable=False, server_default="atr_rebal"),
        sa.Column("name", sa.String(60), nullable=False, server_default="월초 ATR 리밸런싱"),
        sa.Column("environment", sa.String(10), nullable=False, server_default="paper"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("dry_run", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("invest_amount", sa.Float, nullable=False, server_default="10000000"),
        sa.Column("max_tickers", sa.Integer, nullable=False, server_default="20"),
        sa.Column("atr_period", sa.Integer, nullable=False, server_default="14"),
        sa.Column("buy_band_atr_mult", sa.Float, nullable=False, server_default="1"),
        sa.Column("stop_atr_mult", sa.Float, nullable=False, server_default="2"),
        sa.Column("stop_loss_pct", sa.Float, nullable=False, server_default="-10"),
        sa.Column("max_order_amount", sa.Float, nullable=False, server_default="5000000"),
        sa.Column("max_orders_per_day", sa.Integer, nullable=False, server_default="1000"),
        sa.Column("target_source", sa.String(10), nullable=False, server_default="file"),
        sa.Column("target_file_name", sa.String(200), nullable=False, server_default=""),
        sa.Column("targets_uploaded_at", sa.DateTime(timezone=True), nullable=True),
        _ts("created_at"), _ts("updated_at"),
    )
    op.create_table(
        "strategy_targets",
        _pk(),
        sa.Column("plan_id", U(), sa.ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(6), nullable=False),
        sa.Column("name", sa.String(100), nullable=False, server_default=""),
        sa.Column("weight", sa.Float, nullable=False),
        sa.Column("weight_input", sa.Float, nullable=True),
        sa.Column("atr_input", sa.Float, nullable=True),
        sa.Column("position", sa.Integer, nullable=False, server_default="0"),
        _ts("created_at"),
        sa.UniqueConstraint("plan_id", "symbol", name="uq_strategy_targets_plan_symbol"),
    )
    op.create_table(
        "strategy_positions",
        _pk(),
        sa.Column("plan_id", U(), sa.ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(6), nullable=False),
        sa.Column("name", sa.String(100), nullable=False, server_default=""),
        sa.Column("quantity", sa.Integer, nullable=False, server_default="0"),
        sa.Column("avg_price", sa.Float, nullable=False, server_default="0"),
        sa.Column("atr", sa.Float, nullable=True),
        sa.Column("atr_source", sa.String(4), nullable=False, server_default="none"),
        sa.Column("atr_as_of", sa.Date, nullable=True),
        _ts("updated_at"),
        sa.UniqueConstraint("plan_id", "symbol", name="uq_strategy_positions_plan_symbol"),
    )
    op.create_table(
        "strategy_runs",
        _pk(),
        sa.Column("plan_id", U(), sa.ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", U(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("run_date", sa.Date, nullable=False),
        sa.Column("kind", sa.String(10), nullable=False),
        sa.Column("status", sa.String(10), nullable=False, server_default="planned"),
        sa.Column("invest_amount", sa.Float, nullable=False, server_default="0"),
        sa.Column("effective_budget", sa.Float, nullable=False, server_default="0"),
        sa.Column("cash_balance", sa.Float, nullable=False, server_default="0"),
        sa.Column("ledger_eval", sa.Float, nullable=False, server_default="0"),
        sa.Column("warnings", postgresql.JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("rows", postgresql.JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("summary", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("note", sa.String(300), nullable=False, server_default=""),
        _ts("created_at"), _ts("updated_at"),
    )
    op.create_index("ix_strategy_runs_plan_created", "strategy_runs", ["plan_id", "created_at"])
    op.create_index("uq_strategy_runs_rebalance_once", "strategy_runs", ["plan_id", "run_date"], unique=True,
                    postgresql_where=sa.text("kind = 'rebalance'"))
    op.create_table(
        "strategy_slices",
        _pk(),
        sa.Column("run_id", U(), sa.ForeignKey("strategy_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("plan_id", U(), sa.ForeignKey("strategy_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(6), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("quantity", sa.Integer, nullable=False, server_default="0"),
        sa.Column("order_type", sa.String(6), nullable=False, server_default="LIMIT"),
        sa.Column("price", sa.Float, nullable=False, server_default="0"),
        sa.Column("client_order_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("live_order_id", U(), sa.ForeignKey("live_orders.id"), nullable=True),
        sa.Column("status", sa.String(10), nullable=False, server_default="planned"),
        sa.Column("reason", sa.String(300), nullable=False, server_default=""),
        _ts("created_at"), _ts("updated_at"),
    )
    op.create_index("ix_strategy_slices_run_symbol", "strategy_slices", ["run_id", "symbol"])


def downgrade() -> None:
    op.drop_index("ix_strategy_slices_run_symbol", table_name="strategy_slices")
    op.drop_table("strategy_slices")
    op.drop_index("uq_strategy_runs_rebalance_once", table_name="strategy_runs")
    op.drop_index("ix_strategy_runs_plan_created", table_name="strategy_runs")
    op.drop_table("strategy_runs")
    op.drop_table("strategy_positions")
    op.drop_table("strategy_targets")
    op.drop_table("strategy_plans")
