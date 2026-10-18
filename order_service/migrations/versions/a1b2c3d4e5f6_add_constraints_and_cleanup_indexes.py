"""add constraints and cleanup indexes

Revision ID: a1b2c3d4e5f6
Revises: f12a34b56c78
Create Date: 2026-10-18 02:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "f12a34b56c78"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Partial index for expiring pending orders
    op.create_index(
        "ix_orders_pending_created_at",
        "orders",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
        sqlite_where=sa.text("status = 'pending'"),
    )

    # Partial index for cleaning up processed outbox events
    op.create_index(
        "ix_outbox_events_processed_created_at",
        "outbox_events",
        ["created_at"],
        unique=False,
        postgresql_where=sa.text("processed = true"),
        sqlite_where=sa.text("processed = 1"),
    )

    # Table check constraints for data integrity
    op.create_check_constraint(
        "ck_orders_status",
        "orders",
        "status IN ('pending', 'reserved', 'ready_for_shipping', 'completed', 'cancelled')",
    )
    op.create_check_constraint(
        "ck_orders_total_amount",
        "orders",
        "total_amount >= 0",
    )
    op.create_check_constraint(
        "ck_order_items_quantity",
        "order_items",
        "quantity > 0",
    )
    op.create_check_constraint(
        "ck_order_items_price",
        "order_items",
        "price > 0",
    )


def downgrade() -> None:
    op.drop_constraint("ck_order_items_price", "order_items", type_="check")
    op.drop_constraint("ck_order_items_quantity", "order_items", type_="check")
    op.drop_constraint("ck_orders_total_amount", "orders", type_="check")
    op.drop_constraint("ck_orders_status", "orders", type_="check")

    op.drop_index("ix_outbox_events_processed_created_at", table_name="outbox_events")
    op.drop_index("ix_orders_pending_created_at", table_name="orders")
