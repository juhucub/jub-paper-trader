#position persistence model

from datetime import datetime

from sqlalchemy import Integer, String, Float, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    quantity: Mapped[float] = mapped_column(Float)
    avg_entry_price: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)