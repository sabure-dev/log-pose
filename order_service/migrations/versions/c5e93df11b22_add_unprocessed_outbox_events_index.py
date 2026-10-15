"""add index on outbox_events processed and created_at

Revision ID: c5e93df11b22
Revises: 978dec8dd733
Create Date: 2026-10-15 22:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'c5e93df11b22'
down_revision: Union[str, Sequence[str], None] = '978dec8dd733'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        'ix_outbox_events_processed_created_at',
        'outbox_events',
        ['processed', 'created_at'],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_outbox_events_processed_created_at', table_name='outbox_events')
