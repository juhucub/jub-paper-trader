#Portfolkio snapshot persistence model

from datetime import datetime

from sqlalchemy import Integer, String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    total_value: Mapped[float] = mapped_column(Float)
    total_cost: Mapped[float] = mapped_column(Float)
    total_unrealized_pnl: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)