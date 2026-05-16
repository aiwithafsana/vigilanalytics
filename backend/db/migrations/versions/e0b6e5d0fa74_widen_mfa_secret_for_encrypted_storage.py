"""widen mfa_secret for encrypted storage

Switches ``users.mfa_secret`` from VARCHAR(64) to VARCHAR(256) so it can
hold Fernet ciphertext (~120 chars for a 32-byte plaintext).  At the DB
level the column remains a plain VARCHAR — the encryption layer is
implemented in the application as a SQLAlchemy ``TypeDecorator``
(see ``app/services/encryption.py``).

This migration intentionally does NOT backfill existing plaintext rows.
``decrypt_secret`` falls back to returning the raw value when Fernet
decryption fails, so legacy plaintext rows continue to work — but new
writes are encrypted.  Production deployment should backfill encrypted
ciphertext separately if any MFA-enabled users predate this migration.

Revision ID: e0b6e5d0fa74
Revises: 07a3036f1633
Create Date: 2026-05-10 21:47:42.632126
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e0b6e5d0fa74'
down_revision: Union[str, None] = '07a3036f1633'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'users', 'mfa_secret',
        existing_type=sa.VARCHAR(length=64),
        type_=sa.String(length=256),
        existing_nullable=True,
    )


def downgrade() -> None:
    # Truncation risk: any encrypted ciphertext longer than 64 chars will be
    # lost.  Acceptable for downgrade — caller is explicitly rolling back.
    op.alter_column(
        'users', 'mfa_secret',
        existing_type=sa.String(length=256),
        type_=sa.VARCHAR(length=64),
        existing_nullable=True,
    )
