"""harden orders and outbox tables

Revision ID: f12a34b56c78
Revises: e89b12d34567
Create Date: 2026-10-18 01:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f12a34b56c78"
down_revision: Union[str, Sequence[str], None] = "e89b12d34567"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column("cancellation_reason", sa.String(length=500), nullable=True),
    )
    op.add_column(
        "orders",
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
    )
    op.create_unique_constraint(
        "uq_orders_user_idempotency_key",
        "orders",
        ["user_id", "idempotency_key"],
    )

    op.add_column(
        "outbox_events",
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "outbox_events",
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.drop_index("ix_outbox_events_processed_created_at", table_name="outbox_events")
    op.create_index(
        "ix_outbox_events_unprocessed_created_at",
        "outbox_events",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("processed = false"),
        sqlite_where=sa.text("processed = 0"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_unprocessed_created_at", table_name="outbox_events"
    )
    op.create_index(
        "ix_outbox_events_processed_created_at",
        "outbox_events",
        ["processed", "created_at"],
        unique=False,
    )
    op.drop_column("outbox_events", "next_retry_at")
    op.drop_column("outbox_events", "last_error")
    op.drop_column("outbox_events", "attempts")

    op.drop_constraint("uq_orders_user_idempotency_key", "orders", type_="unique")
    op.drop_column("orders", "idempotency_key")
    op.drop_column("orders", "cancellation_reason")
