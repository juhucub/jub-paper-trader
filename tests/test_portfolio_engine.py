from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.base import Base
from db.models.portfolio import PortfolioAccountState, TradeHistory
from db.models.positions import Position
from services.portfolio_engine import PortfolioEngine
from services.risk_guardrails import RiskGuardrails


class FakeAlpacaClient:
    def submit_order(self, **kwargs):
        return {"status": "submitted", **kwargs}


def _build_engine() -> PortfolioEngine:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()
    return PortfolioEngine(
        alpaca_client=FakeAlpacaClient(),
        risk_guardrails=RiskGuardrails(),
        db_session=session,
    )


def test_sync_and_mark_to_market_tracks_equity_drawdown_and_exposure():
    pe = _build_engine()

    exposure = pe.sync_account_state(
        account={"cash": 10_000.0, "equity": 10_000.0},
        positions=[{"symbol": "AAPL", "qty": 10, "avg_entry_price": 100.0, "current_price": 110.0}],
        orders=[],
    )

    assert exposure["open_positions"] == 1.0
    assert exposure["largest_position_pct"] > 0

    unrealized = pe.mark_to_market({"AAPL": 90.0})
    eq = pe.recalculate_equity()

    acct = pe.db_session.get(PortfolioAccountState, 1)
    assert unrealized == -100.0
    assert eq == 10_900.0
    assert acct is not None
    assert acct.max_drawdown == 200.0


def test_apply_fill_updates_cash_realized_pnl_and_trade_history():
    pe = _build_engine()
    pe.sync_account_state(
        account={"cash": 10_000.0, "equity": 10_000.0},
        positions=[{"symbol": "NVDA", "qty": 10, "avg_entry_price": 100.0, "current_price": 100.0}],
        orders=[],
    )

    result = pe.apply_fill({"symbol": "NVDA", "side": "sell", "qty": 4, "price": 120.0})

    pos = pe.db_session.query(Position).filter_by(symbol="NVDA").one()
    acct = pe.db_session.get(PortfolioAccountState, 1)
    trades = pe.db_session.query(TradeHistory).all()

    assert pos.quantity == 6
    assert round(pos.realized_pnl, 2) == 80.0
    assert acct is not None
    assert round(acct.cash, 2) == 10_480.0
    assert round(result["realized_pnl"], 2) == 80.0
    assert len(trades) == 1
