from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "e89b12d34567"
down_revision: Union[str, Sequence[str], None] = "c5e93df11b22"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column("shipping_address", sa.String(length=500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("orders", "shipping_address")
