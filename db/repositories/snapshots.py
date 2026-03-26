"""Repository helpers for snapshot persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db.models.snapshots import BotCycleSnapshot, MarketDataSnapshot, utc_now


def upsert_market_data_snapshot(
    db_session: Session,
    symbol: str,
    snapshot_type: str,
    payload: dict[str, Any],
    source_timestamp: datetime,
) -> MarketDataSnapshot:
    """Insert or update a market-data snapshot.

    Uses Postgres-native `ON CONFLICT` when available and a portable fallback otherwise.
    """

    dialect_name = db_session.bind.dialect.name if db_session.bind else ""

    if dialect_name == "postgresql":
        stmt = pg_insert(MarketDataSnapshot).values(
            symbol=symbol,
            snapshot_type=snapshot_type,
            source_timestamp=source_timestamp,
            payload=payload,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_market_snapshot_symbol_type_source_ts",
            set_={"payload": payload, "updated_at": utc_now},
        ).returning(MarketDataSnapshot)
        persisted = db_session.execute(stmt).scalar_one()
        db_session.commit()
        return persisted

    existing = db_session.execute(
        select(MarketDataSnapshot).where(
            MarketDataSnapshot.symbol == symbol,
            MarketDataSnapshot.snapshot_type == snapshot_type,
            MarketDataSnapshot.source_timestamp == source_timestamp,
        )
    ).scalar_one_or_none()

    if existing:
        existing.payload = payload
        existing.updated_at = utc_now()
        db_session.commit()
        db_session.refresh(existing)
        return existing

    created = MarketDataSnapshot(
        symbol=symbol,
        snapshot_type=snapshot_type,
        source_timestamp=source_timestamp,
        payload=payload,
    )
    db_session.add(created)
    db_session.commit()
    db_session.refresh(created)
    return created

def create_bot_cycle_snapshot(
    db_session: Session,
    cycle_id: str,
    payload: dict[str, Any],
) -> BotCycleSnapshot:
    snapshot = BotCycleSnapshot(cycle_id=cycle_id, payload=payload)
    db_session.add(snapshot)
    db_session.commit()
    db_session.refresh(snapshot)
    return snapshot
