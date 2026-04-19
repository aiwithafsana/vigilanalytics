"""
Scoring service stub.

In production this triggers the ML pipeline (ml/pipeline/score.py)
to re-score providers. For now it updates the scored_at timestamp
so the API has a hook to call.
"""
from datetime import datetime, timezone
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Provider


async def trigger_rescore(db: AsyncSession, npi: str | None = None) -> dict:
    """
    Mark provider(s) for rescoring. In production, this would enqueue
    a Celery/ARQ task that runs the ML pipeline.
    """
    stmt = update(Provider).values(scored_at=datetime.now(timezone.utc))
    if npi:
        stmt = stmt.where(Provider.npi == npi)

    result = await db.execute(stmt)
    count = result.rowcount

    return {"queued": count, "status": "pending"}
