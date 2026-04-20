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
        insert_stmt = pg_insert(MarketDataSnapshot).values(
            symbol=symbol,
            snapshot_type=snapshot_type,
            source_timestamp=source_timestamp,
            payload=payload,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_market_snapshot_symbol_type_source_ts",
            set_={"payload": payload, "updated_at": utc_now()},
        ).returning(MarketDataSnapshot)
        persisted = db_session.execute(upsert_stmt).scalar_one()
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


def get_latest_bot_cycle_snapshot(db_session: Session) -> BotCycleSnapshot | None:
    return db_session.execute(
        select(BotCycleSnapshot).order_by(BotCycleSnapshot.created_at.desc()).limit(1)
    ).scalar_one_or_none()


def get_bot_cycle_snapshot_by_cycle_id(db_session: Session, cycle_id: str) -> BotCycleSnapshot | None:
    return db_session.execute(
        select(BotCycleSnapshot).where(BotCycleSnapshot.cycle_id == cycle_id)
    ).scalar_one_or_none()


def list_bot_cycle_snapshots(
    db_session: Session,
    *,
    symbol: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 100,
) -> list[BotCycleSnapshot]:
    stmt = select(BotCycleSnapshot).order_by(BotCycleSnapshot.created_at.desc())
    if created_after is not None:
        stmt = stmt.where(BotCycleSnapshot.created_at >= created_after)
    if created_before is not None:
        stmt = stmt.where(BotCycleSnapshot.created_at <= created_before)

    if symbol is None:
        return list(db_session.execute(stmt.limit(limit)).scalars().all())

    snapshots = list(db_session.execute(stmt).scalars().all())
    symbol = symbol.upper()
    filtered = [
        snapshot
        for snapshot in snapshots
        if symbol in (snapshot.payload or {}).get("symbols", [])
        or symbol in ((snapshot.payload or {}).get("symbol_lineage", {}) or {})
    ]
    return filtered[:limit]


def query_blocked_cycle_symbols(
    db_session: Session,
    *,
    symbol: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in list_bot_cycle_snapshots(db_session, symbol=symbol, limit=limit):
        monitoring_payload = (snapshot.payload or {}).get("monitoring_decision", {}) or {}
        blocked_symbols = list(monitoring_payload.get("blocked_symbols", []) or [])
        for blocked_symbol in blocked_symbols:
            if symbol and blocked_symbol != symbol.upper():
                continue
            rows.append(
                {
                    "cycle_id": snapshot.cycle_id,
                    "created_at": snapshot.created_at,
                    "symbol": blocked_symbol,
                    "inaction_reasons": monitoring_payload.get("inaction_reasons", []),
                }
            )
    return rows


def reconstruct_symbol_cycle_history(
    db_session: Session,
    *,
    symbol: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for snapshot in reversed(list_bot_cycle_snapshots(db_session, symbol=symbol, limit=limit)):
        payload = snapshot.payload or {}
        symbol_lineage = (payload.get("symbol_lineage", {}) or {}).get(symbol.upper())
        if symbol_lineage is None:
            continue
        history.append(
            {
                "cycle_id": snapshot.cycle_id,
                "created_at": snapshot.created_at,
                "symbol": symbol.upper(),
                "lineage": symbol_lineage,
                "monitoring_decision": payload.get("monitoring_decision", {}),
                "scenario_regime": (payload.get("scenario_bundle", {}) or {}).get("regime_label"),
            }
        )
    return history
