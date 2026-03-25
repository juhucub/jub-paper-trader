#Portfolio account persistence model

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import Integer, String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

def utc_now() -> datetime:
    """Helper for UTC timestamps."""
    return datetime.now(timezone.utc)

class PortfolioAccountState(Base):
    __tablename__ = "portfolio_account_state"   

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    cash: Mapped[float] = mapped_column(Float, default=0.0)
    equity: Mapped[float] = mapped_column(Float, default=0.0)
    peak_equity: Mapped[float] = mapped_column(Float, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    daily_realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    daily_date: Mapped[date] = mapped_column(Date, default=date.today)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

class TradeHistory(Base):
    __tablename__ = "trade_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    quantity: Mapped[float] = mapped_column(Float)
    side: Mapped[str] = mapped_column(String(8))
    price: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    order_id: Mapped[int] = mapped_column(Integer, ForeignKey("orders.id"), nullable=True)
    occured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)