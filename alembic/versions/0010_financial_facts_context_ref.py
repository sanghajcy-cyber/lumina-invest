"""financial_facts: context_ref 컬럼 추가 + 자연키 재정의

XBRL raw canonical 데이터는 (stock_code, fs_div, statement_type, account_id,
rcept_no, version_seq)만으로는 유일하지 않음 -- 같은 계정이 당기/전기/전전기
비교 데이터로, 또는 분기단독/누적 비교로, 또는 자본변동표처럼 세그먼트/구성요소
축으로 쪼개져 여러 행이 동시에 존재함. 실제 유일성을 보장하는 건 XBRL
context_ref 자체 (실측 검증 완료: 전체 1,845,351행에서 context_ref 포함 키로
중복 0건).

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "q0010"
down_revision: Union[str, None] = "q0009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("financial_facts", sa.Column("context_ref", sa.String(500), nullable=True))
    op.drop_constraint("uq_financial_facts_natural_key", "financial_facts", type_="unique")
    op.create_unique_constraint(
        "uq_financial_facts_natural_key",
        "financial_facts",
        ["stock_code", "fiscal_year", "report_period", "fs_div", "statement_type",
         "account_id", "rcept_no", "version_seq", "context_ref"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_financial_facts_natural_key", "financial_facts", type_="unique")
    op.create_unique_constraint(
        "uq_financial_facts_natural_key",
        "financial_facts",
        ["stock_code", "fiscal_year", "report_period", "fs_div", "statement_type",
         "account_id", "rcept_no", "version_seq"],
    )
    op.drop_column("financial_facts", "context_ref")
