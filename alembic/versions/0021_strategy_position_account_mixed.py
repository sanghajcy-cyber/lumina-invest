"""ATR 전략: strategy_positions.account_mixed — 계좌에 전략 밖 보유분이 섞였는지(KIS 평균가로 장부를 맞출지 판단)

Revision ID: 0021
Revises: 0020
Create Date: 2026-10-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0021"
down_revision: Union[str, None] = "0020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("strategy_positions", sa.Column("account_mixed", sa.Boolean, nullable=False, server_default=sa.false()))
    # 기존 장부: 리밸런싱 계획 스냅샷에 계좌 수량 > 장부 수량이었던 종목(전략 밖 보유분이 있던 종목)은 섞임으로 표시한다.
    # executor._freeze_atr 가 앞으로 하는 판단과 같다. 이게 없으면 첫 손절 감시 전에 직접 산 몫을 팔았을 때
    # 계좌 수량이 장부와 같아져 KIS 평균법 평균가(직접 산 몫이 섞인 값)로 장부 평균가를 덮어쓸 수 있다.
    op.execute("""
        UPDATE strategy_positions p SET account_mixed = true
        FROM strategy_runs r, jsonb_array_elements(r.rows) AS e
        WHERE r.plan_id = p.plan_id AND r.kind = 'rebalance' AND e->>'symbol' = p.symbol
          AND COALESCE((e->>'account_qty')::int, 0) > COALESCE((e->>'ledger_qty')::int, 0)
    """)


def downgrade() -> None:
    op.drop_column("strategy_positions", "account_mixed")
