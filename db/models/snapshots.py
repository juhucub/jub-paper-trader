#Portfolkio snapshot persistence model

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Integer, String, Float, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

def utc_now() -> datetime:
    """Helper for UTC timestamps."""
    return datetime.now(timezone.utc)

class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    total_value: Mapped[float] = mapped_column(Float)
    total_cost: Mapped[float] = mapped_column(Float)
    total_unrealized_pnl: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

class MarketDataSnapshot(Base):
    __tablename__ = "market_data_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "symbol", 
            "snapshot_type",
            "source_timestamp",
            name="uq_market_snapshot_symbol_type_source_ts",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    snapshot_type: Mapped[str] = mapped_column(String(16), index=True)  # e.g. "quote", "trade"
    source_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)  # timestamp from data source
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)  # raw market data payload
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

class BotCycleSnapshot(Base):
    __tablename__ = "bot_cycle_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cycle_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)