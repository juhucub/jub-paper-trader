from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agent_service.bot_cycle import BotCycleService
from agent_service.optimizer_qpo import OptimizerQPO
from db.base import Base
from db.models.orders import Order
from db.models.portfolio import PortfolioAccountState
from db.models.positions import Position
from db.models.snapshots import BotCycleSnapshot
from services.execution_router import ExecutionRouter
from services.position_sizer import PositionSizer
from services.portfolio_engine import PortfolioEngine
from services.risk_guardrails import RiskGuardrails


@dataclass(slots=True)
class FakePosition:
    symbol: str
    qty: float
    side: str = "long"
    avg_entry_price: float = 120.0
    current_price: float = 130.0
    market_value: float | None = None
    unrealized_pl: float | None = None


@dataclass(slots=True)
class FakeOrder:
    id: str
    symbol: str
    qty: float
    side: str
    type: str
    time_in_force: str
    status: str


@dataclass(slots=True)
class FakeAccount:
    id: str
    status: str
    currency: str
    buying_power: float
    equity: float


class FakeDataClient:
    def get_historical_bars(self, symbol: str, timeframe: str, limit: int, start: str | None = None, end: str | None = None):
        assert timeframe == "1Min"
        assert limit == 30
        _ = (start, end)
        if symbol == "AAPL":
            return [{"c": 100 + i, "v": 100_000} for i in range(30)]
        return [{"c": 200 - i, "v": 150_000} for i in range(30)]

    def get_latest_quote(self, symbol: str):
        return {"ap": 130.0 if symbol == "AAPL" else 170.0, "bp": 129.5}


class FakeTradingClient:
    def __init__(self):
        self.submissions: list[dict] = []

    def get_account(self):
        return FakeAccount(id="acct", status="ACTIVE", currency="USD", buying_power=10_000.0, equity=10_000.0)

    def get_positions(self):
        return [FakePosition(symbol="AAPL", qty=2.0)]

    def get_orders(self, status: str | None = None, limit: int | None = None):
        _ = (status, limit)
        return []

    def submit_order(self, **kwargs):
        self.submissions.append(kwargs)
        return FakeOrder(
            id=f"ord-{len(self.submissions)}",
            symbol=kwargs["symbol"],
            qty=kwargs["qty"],
            side=kwargs["side"],
            type=kwargs["type"],
            time_in_force=kwargs["time_in_force"],
            status="new",
        )


def _build_service() -> tuple[BotCycleService, FakeTradingClient, object]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()

    fake_trading = FakeTradingClient()
    portfolio_engine = PortfolioEngine(
        alpaca_client=fake_trading,
        risk_guardrails=RiskGuardrails(min_avg_dollar_volume=1.0),
        db_session=session,
    )

    service = BotCycleService(
        alpaca_data_client=FakeDataClient(),
        alpaca_client=fake_trading,
        risk_guardrails=RiskGuardrails(min_avg_dollar_volume=1.0),
        portfolio_engine=portfolio_engine,
        optimizer=OptimizerQPO(max_symbol_weight=0.4, cash_buffer=0.05),
        execution_router=ExecutionRouter(min_trade_notional=10.0),
        position_sizer=PositionSizer(min_notional=10.0, max_position_pct=0.4),
        db_session=session,
    )
    return service, fake_trading, session


def test_bot_cycle_persists_snapshot_submits_orders_and_reconciles():
    service, fake_trading, session = _build_service()

    result = service.run_cycle(["AAPL", "MSFT"])

    snapshots = session.query(BotCycleSnapshot).all()
    assert len(snapshots) == 1
    assert snapshots[0].cycle_id == result["cycle_id"]

    assert result["benchmark_symbol"] == "SPY"
    assert "AAPL" in result["target_weights"]
    assert isinstance(result["submitted_orders"], list)
    assert len(result["submitted_orders"]) + len(result["blocked_orders"]) >= 1
    assert len(fake_trading.submissions) == len(result["submitted_orders"])
    assert "policy_decisions" in result
    assert "exit_policy_actions" in result
    assert "target_weights" in result
    assert "adjusted_target_weights" in result
    assert "decision_summaries" in result

    account = session.get(PortfolioAccountState, 1)
    assert account is not None
    _ = session.query(Position).all()
    _ = session.query(Order).all()

def test_bot_cycle_sets_reason_when_all_signals_non_positive():
    service, _, _ = _build_service()

    result = service.run_cycle(["MSFT"])

    assert "MSFT" in result["decision_summaries"]
    assert result["decision_summaries"]["MSFT"]["decision_reason"] == "no_target_allocation"
    assert len(result["submitted_orders"]) >= 1


def test_bot_cycle_sets_reason_when_all_candidates_blocked():
    service, _, _ = _build_service()
    service.risk_guardrails = RiskGuardrails(min_avg_dollar_volume=1.0, max_position_pct=0.0001)

    result = service.run_cycle(["AAPL"])

    assert result["submitted_orders"] == []
    assert len(result["blocked_orders"]) >= 1
    assert result["decision_summaries"]["AAPL"]["decision_status"] == "BLOCKED"


def test_bot_cycle_persists_exit_policy_actions_and_triggers():
    service, _, _ = _build_service()
    service.exit_policy.stop_loss_pct = 0.001

    result = service.run_cycle(["AAPL"])

    action = result["exit_policy_actions"]["AAPL"]
    assert action["action"] == "EXIT"
    assert action["trigger"] == "take_profit_exit_band"

    summary = result["decision_summaries"]["AAPL"]
    assert summary["position_action"] == "EXIT"
    assert summary["position_action_trigger"] == "take_profit_exit_band"
