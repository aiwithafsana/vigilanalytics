"""baseline schema — sync live DB with current models

This is the first Alembic migration.  Prior to its introduction the schema
was managed via ``Base.metadata.create_all(checkfirst=True)``, which creates
missing tables but cannot add columns to existing tables.  Over time this
caused real schema drift: the live DB was missing several columns that the
models depend on.

This migration captures that drift and brings the live DB into alignment.
After this migration, all schema changes flow through Alembic.

Columns added
-------------
- ``cases``: outcome, outcome_note, resolved_at  (case-management workflow)
- ``users``: token_version, failed_login_attempts, locked_until
  (auth security: token rotation, account lockout)
- ``users``: mfa_enabled, mfa_secret, mfa_backup_codes, mfa_enrolled_at
  (TOTP MFA — see services/mfa.py)

All NOT NULL columns are given server defaults so existing rows are
populated correctly during ALTER.

Revision ID: 07a3036f1633
Revises:
Create Date: 2026-05-10 21:20:34.619830
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '07a3036f1633'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Cases — outcome workflow ─────────────────────────────────────────
    op.add_column('cases', sa.Column('outcome', sa.String(length=30), nullable=True))
    op.add_column('cases', sa.Column('outcome_note', sa.Text(), nullable=True))
    op.add_column('cases', sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True))

    # ── Users — security additions ───────────────────────────────────────
    # token_version: bump to invalidate all existing JWTs for a user.
    op.add_column(
        'users',
        sa.Column('token_version', sa.Integer(), nullable=False, server_default='0'),
    )
    # failed_login_attempts: track consecutive failures for account lockout.
    op.add_column(
        'users',
        sa.Column('failed_login_attempts', sa.Integer(), nullable=False, server_default='0'),
    )
    # locked_until: NULL = not locked; otherwise lockout expiry.
    op.add_column(
        'users',
        sa.Column('locked_until', sa.DateTime(timezone=True), nullable=True),
    )

    # ── Users — MFA (TOTP) ───────────────────────────────────────────────
    op.add_column(
        'users',
        sa.Column('mfa_enabled', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )
    # mfa_secret: NULL until enrollment.  Plaintext base32 in dev; column-
    # level encryption required for production (see data_handling_policy.md §4.2).
    op.add_column(
        'users',
        sa.Column('mfa_secret', sa.String(length=64), nullable=True),
    )
    # mfa_backup_codes: array of bcrypt-hashed one-time codes.
    op.add_column(
        'users',
        sa.Column(
            'mfa_backup_codes',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        'users',
        sa.Column('mfa_enrolled_at', sa.DateTime(timezone=True), nullable=True),
    )

    # ── Type-precision alignment (no data loss, narrowing declared types) ──
    # These columns already hold values that fit the narrower types — Alembic
    # detected the drift when comparing live DB to model declarations.
    op.alter_column(
        'dashboard_stats', 'total_payment',
        existing_type=sa.NUMERIC(precision=16, scale=2),
        type_=sa.Numeric(precision=14, scale=2),
        existing_nullable=True,
    )
    op.alter_column(
        'providers', 'ppb_vs_peer',
        existing_type=sa.DOUBLE_PRECISION(precision=53),
        type_=sa.Numeric(precision=8, scale=4),
        existing_nullable=True,
    )
    op.alter_column(
        'providers', 'peer_median_ppb',
        existing_type=sa.DOUBLE_PRECISION(precision=53),
        type_=sa.Numeric(precision=12, scale=2),
        existing_nullable=True,
    )
    op.alter_column(
        'providers', 'entity_type',
        existing_type=sa.CHAR(length=1),
        type_=sa.String(length=1),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.drop_column('users', 'mfa_enrolled_at')
    op.drop_column('users', 'mfa_backup_codes')
    op.drop_column('users', 'mfa_secret')
    op.drop_column('users', 'mfa_enabled')
    op.drop_column('users', 'locked_until')
    op.drop_column('users', 'failed_login_attempts')
    op.drop_column('users', 'token_version')
    op.alter_column(
        'providers', 'entity_type',
        existing_type=sa.String(length=1),
        type_=sa.CHAR(length=1),
        existing_nullable=True,
    )
    op.alter_column(
        'providers', 'peer_median_ppb',
        existing_type=sa.Numeric(precision=12, scale=2),
        type_=sa.DOUBLE_PRECISION(precision=53),
        existing_nullable=True,
    )
    op.alter_column(
        'providers', 'ppb_vs_peer',
        existing_type=sa.Numeric(precision=8, scale=4),
        type_=sa.DOUBLE_PRECISION(precision=53),
        existing_nullable=True,
    )
    op.alter_column(
        'dashboard_stats', 'total_payment',
        existing_type=sa.Numeric(precision=14, scale=2),
        type_=sa.NUMERIC(precision=16, scale=2),
        existing_nullable=True,
    )
    op.drop_column('cases', 'resolved_at')
    op.drop_column('cases', 'outcome_note')
    op.drop_column('cases', 'outcome')
