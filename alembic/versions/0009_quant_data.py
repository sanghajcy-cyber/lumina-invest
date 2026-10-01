"""AI_Quant 정량 데이터: companies/재무/시세/밸류에이션/팩터스코어

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "companies",
        sa.Column("stock_code", sa.String(10), primary_key=True),
        sa.Column("corp_code", sa.String(10), nullable=False),
        sa.Column("company_name", sa.String(200), nullable=False),
        sa.Column("market", sa.String(20), nullable=True),
        sa.Column("listed_shares_latest", sa.BigInteger, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "company_sector_snapshots",
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), primary_key=True),
        sa.Column("as_of_date", sa.Date, primary_key=True),
        sa.Column("sector_effective_date", sa.Date, nullable=False),
        sa.Column("wics_code", sa.String(20), nullable=True),
        sa.Column("wics_name", sa.String(50), nullable=True),
        sa.Column("lookback_days", sa.Integer, nullable=True),
        sa.Column("source", sa.String(50), nullable=True),
    )
    op.create_index("idx_sector_snapshots_stock_date", "company_sector_snapshots", ["stock_code", sa.text("as_of_date DESC")])

    op.create_table(
        "financial_facts",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), nullable=False),
        sa.Column("fiscal_year", sa.Integer, nullable=False),
        sa.Column("report_period", sa.String(10), nullable=False),
        sa.Column("fs_div", sa.String(3), nullable=False),
        sa.Column("statement_type", sa.String(4), nullable=False),
        sa.Column("account_id", sa.String(50), nullable=False),
        sa.Column("account_name", sa.String(200), nullable=True),
        sa.Column("amount_value", sa.Numeric(20, 2), nullable=True),
        sa.Column("period_type", sa.String(10), nullable=False),
        sa.Column("period_start_date", sa.Date, nullable=True),
        sa.Column("period_end_date", sa.Date, nullable=True),
        sa.Column("currency", sa.String(10), nullable=True),
        sa.Column("unit", sa.String(20), nullable=True),
        sa.Column("rcept_no", sa.String(20), nullable=False),
        sa.Column("rcept_dt", sa.Date, nullable=False),
        sa.Column("version_seq", sa.Integer, nullable=False),
        sa.UniqueConstraint(
            "stock_code", "fiscal_year", "report_period", "fs_div", "statement_type",
            "account_id", "rcept_no", "version_seq",
            name="uq_financial_facts_natural_key",
        ),
    )
    op.create_index("idx_financial_facts_lookup", "financial_facts", ["stock_code", "fiscal_year", "report_period", "fs_div", "account_id"])
    op.create_index("idx_financial_facts_rcept_dt", "financial_facts", ["stock_code", "rcept_dt"])

    op.create_table(
        "financial_metrics",
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), primary_key=True),
        sa.Column("as_of_date", sa.Date, primary_key=True),
        sa.Column("fiscal_year", sa.Integer, nullable=False),
        sa.Column("report_period", sa.String(10), nullable=False),
        sa.Column("period_end_date", sa.Date, nullable=True),
        sa.Column("revenue_ttm", sa.Numeric(20, 2), nullable=True),
        sa.Column("net_income_ttm", sa.Numeric(20, 2), nullable=True),
        sa.Column("equity_latest", sa.Numeric(20, 2), nullable=True),
        sa.Column("operating_cash_flow_ttm", sa.Numeric(20, 2), nullable=True),
        sa.Column("source_lineage", postgresql.JSONB, nullable=True),
    )
    op.create_index("idx_financial_metrics_lookup", "financial_metrics", ["stock_code", sa.text("as_of_date DESC")])

    op.create_table(
        "market_prices",
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), primary_key=True),
        sa.Column("trade_date", sa.Date, primary_key=True),
        sa.Column("open_price", sa.Numeric(15, 2), nullable=True),
        sa.Column("high_price", sa.Numeric(15, 2), nullable=True),
        sa.Column("low_price", sa.Numeric(15, 2), nullable=True),
        sa.Column("close_price", sa.Numeric(15, 2), nullable=True),
        sa.Column("volume", sa.BigInteger, nullable=True),
        sa.Column("trade_value", sa.Numeric(20, 2), nullable=True),
        sa.Column("market_cap", sa.Numeric(20, 2), nullable=True),
        sa.Column("listed_shares", sa.BigInteger, nullable=True),
    )
    op.create_index("idx_market_prices_date", "market_prices", ["trade_date"])

    op.create_table(
        "valuation_metrics",
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), primary_key=True),
        sa.Column("as_of_date", sa.Date, primary_key=True),
        sa.Column("financial_as_of_date", sa.Date, nullable=True),
        sa.Column("per", sa.Numeric(10, 2), nullable=True),
        sa.Column("pbr", sa.Numeric(10, 2), nullable=True),
        sa.Column("pcr", sa.Numeric(10, 2), nullable=True),
        sa.Column("psr", sa.Numeric(10, 2), nullable=True),
    )
    op.create_index("idx_valuation_metrics_lookup", "valuation_metrics", ["stock_code", sa.text("as_of_date DESC")])

    op.create_table(
        "factor_scores",
        sa.Column("stock_code", sa.String(10), sa.ForeignKey("companies.stock_code"), primary_key=True),
        sa.Column("as_of_date", sa.Date, primary_key=True),
        sa.Column("strategy_version", sa.String(30), primary_key=True, server_default="composite_v1"),
        sa.Column("eps_growth_annual_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("peg", sa.Numeric(10, 4), nullable=True),
        sa.Column("value_score", sa.Numeric(10, 4), nullable=True),
        sa.Column("growth_score", sa.Numeric(10, 4), nullable=True),
        sa.Column("composite_score", sa.Numeric(10, 4), nullable=True),
        sa.Column("composite_rank", sa.Integer, nullable=True),
        sa.Column("composite_top_pct", sa.Numeric(6, 4), nullable=True),
    )
    op.create_index("idx_factor_scores_lookup", "factor_scores", ["stock_code", sa.text("as_of_date DESC"), "strategy_version"])
    op.create_index("idx_factor_scores_rank", "factor_scores", ["as_of_date", "strategy_version", "composite_rank"])


def downgrade() -> None:
    op.drop_index("idx_factor_scores_rank", table_name="factor_scores")
    op.drop_index("idx_factor_scores_lookup", table_name="factor_scores")
    op.drop_table("factor_scores")

    op.drop_index("idx_valuation_metrics_lookup", table_name="valuation_metrics")
    op.drop_table("valuation_metrics")

    op.drop_index("idx_market_prices_date", table_name="market_prices")
    op.drop_table("market_prices")

    op.drop_index("idx_financial_metrics_lookup", table_name="financial_metrics")
    op.drop_table("financial_metrics")

    op.drop_index("idx_financial_facts_rcept_dt", table_name="financial_facts")
    op.drop_index("idx_financial_facts_lookup", table_name="financial_facts")
    op.drop_table("financial_facts")

    op.drop_index("idx_sector_snapshots_stock_date", table_name="company_sector_snapshots")
    op.drop_table("company_sector_snapshots")

    op.drop_table("companies")
