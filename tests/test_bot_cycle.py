from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agent_service.bot_cycle import BotCycleService
from agent_service.interfaces import (
    AllocationProposal,
    CycleReport,
    DecisionPolicyOutput,
    ExecutionResult,
    ExitPolicyOutput,
    MonitoringDecision,
    OrderProposal,
    PolicyConstraint,
    RiskAllocationDetail,
    ReconciliationResult,
    RiskAdjustedAllocation,
    ScenarioBundle,
    SignalBundle,
)
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
    filled_qty: float = 0.0
    limit_price: float | None = None
    submitted_at: datetime | str | None = None
    created_at: datetime | str | None = None


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
        latest = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
        base_price = 100.0 if symbol == "AAPL" else 200.0
        price_step = 1.0 if symbol == "AAPL" else -1.0
        if symbol == "AAPL":
            return [
                {"t": (latest - timedelta(minutes=(29 - i))).isoformat(), "c": base_price + i, "v": 100_000}
                for i in range(30)
            ]
        return [
            {"t": (latest - timedelta(minutes=(29 - i))).isoformat(), "c": base_price + (price_step * i), "v": 150_000}
            for i in range(30)
        ]

    def get_latest_quote(self, symbol: str):
        latest = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
        if symbol == "AAPL":
            return {"ap": 130.0, "bp": 129.5, "t": latest.isoformat()}
        return {"ap": 170.0, "bp": 169.5, "t": latest.isoformat()}


class FakeTradingClient:
    def __init__(self):
        self.submissions: list[dict] = []
        self.open_orders: list[FakeOrder] = []
        self.cancelled_order_ids: list[str] = []

    def get_account(self):
        return FakeAccount(id="acct", status="ACTIVE", currency="USD", buying_power=10_000.0, equity=10_000.0)

    def get_positions(self):
        return [FakePosition(symbol="AAPL", qty=2.0)]

    def get_orders(self, status: str | None = None, limit: int | None = None):
        _ = (status, limit)
        return list(self.open_orders)

    def cancel_order(self, order_id: str):
        self.cancelled_order_ids.append(order_id)

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

class CapturingRiskGuardrails(RiskGuardrails):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.last_portfolio_state: dict | None = None

    def validate_order(self, candidate_order: dict, portfolio_state: dict, market_state: dict) -> dict:
        self.last_portfolio_state = dict(portfolio_state)
        return super().validate_order(candidate_order, portfolio_state, market_state)


class RejectingRiskGuardrails(RiskGuardrails):
    def __init__(self, rejection_reason: str, **kwargs):
        super().__init__(**kwargs)
        self.rejection_reason = rejection_reason
        self.calls: list[dict[str, dict]] = []

    def validate_order(self, candidate_order: dict, portfolio_state: dict, market_state: dict) -> dict:
        self.calls.append(
            {
                "candidate_order": dict(candidate_order),
                "portfolio_state": dict(portfolio_state),
                "market_state": dict(market_state),
            }
        )
        return {"allowed": False, "reason": self.rejection_reason}


def _build_service() -> tuple[BotCycleService, FakeTradingClient, object]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()

    fake_trading = FakeTradingClient()
    portfolio_engine = PortfolioEngine(
        alpaca_client=cast(Any, fake_trading),
        risk_guardrails=RiskGuardrails(min_avg_dollar_volume=1.0),
        db_session=session,
    )

    service = BotCycleService(
        alpaca_data_client=FakeDataClient(),
        alpaca_client=fake_trading,
        risk_guardrails=RiskGuardrails(min_avg_dollar_volume=1.0, max_position_pct=0.5),
        portfolio_engine=portfolio_engine,
        optimizer=OptimizerQPO(max_symbol_weight=0.4, cash_buffer=0.05),
        execution_router=ExecutionRouter(min_trade_notional=10.0),
        position_sizer=PositionSizer(min_notional=10.0, max_position_pct=0.4),
        db_session=session,
    )
    service.decision_policy.max_market_volatility = 1.0
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
    assert len(result["submitted_orders"]) == result["execution_result"].submitted_count
    assert len(result["blocked_orders"]) == result["execution_result"].blocked_count
    assert len(fake_trading.submissions) == len(result["submitted_orders"])
    assert "policy_decisions" in result
    assert "exit_policy_actions" in result
    assert "target_weights" in result
    assert "adjusted_target_weights" in result
    assert "decision_summaries" in result
    assert "symbol_lineage" in result

    account = session.get(PortfolioAccountState, 1)
    assert account is not None
    _ = session.query(Position).all()
    _ = session.query(Order).all()


def test_bot_cycle_result_and_snapshot_include_typed_handoffs():
    service, _, session = _build_service()

    result = service.run_cycle(["AAPL", "MSFT"])
    snapshot = session.query(BotCycleSnapshot).one()

    assert isinstance(result["signal_bundle"], SignalBundle)
    assert isinstance(result["scenario_bundle"], ScenarioBundle)
    assert isinstance(result["allocation_proposal"], AllocationProposal)
    assert isinstance(result["risk_adjusted_allocation"], RiskAdjustedAllocation)
    assert isinstance(result["execution_result"], ExecutionResult)
    assert isinstance(result["reconciliation_result"], ReconciliationResult)
    assert isinstance(result["monitoring_decision"], MonitoringDecision)
    assert isinstance(result["cycle_report"], CycleReport)
    assert result["decision_policy_output"].approved_signal_bundle.lineage["source_decision_policy_at"] == result["decision_policy_output"].as_of
    assert result["exit_policy_output"].source_decision_policy_at == result["decision_policy_output"].as_of
    assert isinstance(result["order_proposals"], list)
    assert all(isinstance(order, OrderProposal) for order in result["order_proposals"])

    assert snapshot.payload["signal_bundle"]["benchmark_symbol"] == result["signal_bundle"].benchmark_symbol
    assert snapshot.payload["scenario_bundle"]["regime_label"] == result["scenario_bundle"].regime_label
    assert snapshot.payload["allocation_proposal"]["scenario_regime"] == result["allocation_proposal"].scenario_regime
    assert snapshot.payload["risk_adjusted_allocation"]["cash_buffer_applied"] == result["risk_adjusted_allocation"].cash_buffer_applied
    assert snapshot.payload["risk_adjusted_allocation"]["lineage"]["allocation_as_of"] == result["risk_adjusted_allocation"].as_of.isoformat()
    assert isinstance(snapshot.payload["risk_adjusted_allocation"]["symbol_details"], list)
    assert snapshot.payload["execution_result"]["submitted_count"] == result["execution_result"].submitted_count
    assert snapshot.payload["reconciliation_result"]["status"] == result["reconciliation_result"].status
    assert snapshot.payload["monitoring_decision"]["status"] == result["monitoring_decision"].status
    assert snapshot.payload["cycle_report"]["cycle_id"] == result["cycle_report"].cycle_id
    assert "AAPL" in snapshot.payload["symbol_lineage"]
    assert snapshot.payload["symbol_lineage"]["AAPL"]["decision_policy"] is not None
    assert [order_payload["symbol"] for order_payload in snapshot.payload["order_proposals"]] == [
        order.symbol for order in result["order_proposals"]
    ]
    assert [order_payload["source_layer"] for order_payload in snapshot.payload["order_proposals"]] == [
        order.source_layer for order in result["order_proposals"]
    ]

def test_bot_cycle_sets_reason_when_all_signals_non_positive():
    service, _, _ = _build_service()

    result = service.run_cycle(["MSFT"])

    assert "MSFT" in result["decision_summaries"]
    assert result["decision_summaries"]["MSFT"]["decision_reason"] == "short_rejected_long_only"
    assert result["submitted_orders"] == []
    assert result["monitoring_decision"].acted is False


def test_bot_cycle_keeps_exit_orders_when_entry_candidates_are_rejected():
    service, _, _ = _build_service()
    service.risk_guardrails = RiskGuardrails(min_avg_dollar_volume=1.0, max_position_pct=0.0001)
    service.exit_policy.take_profit_exit_pct = 0.001
    service.exit_policy.min_holding_minutes = 0

    result = service.run_cycle(["AAPL"])

    assert result["policy_decisions"]["AAPL"]["policy_action"] == "buy"
    assert result["exit_policy_actions"]["AAPL"]["action"] == "EXIT"
    assert any(order["side"] == "sell" for order in result["submitted_orders"])
    assert result["decision_summaries"]["AAPL"]["decision_status"] == "SUBMITTED"


def test_bot_cycle_persists_exit_policy_actions_and_triggers():
    service, _, _ = _build_service()
    service.exit_policy.stop_loss_pct = 0.001
    service.exit_policy.min_holding_minutes = 0

    result = service.run_cycle(["AAPL"])

    action = result["exit_policy_actions"]["AAPL"]
    assert action["action"] == "EXIT"
    assert action["trigger"] == "take_profit_exit_band"

    summary = result["decision_summaries"]["AAPL"]
    assert summary["position_action"] == "EXIT"
    assert summary["position_action_trigger"] == "take_profit_exit_band"


def test_bot_cycle_passes_daily_realized_pnl_to_risk_guardrails():
    service, _, session = _build_service()
    capturing_guardrails = CapturingRiskGuardrails(min_avg_dollar_volume=1.0)
    service.risk_guardrails = capturing_guardrails

    account_state = session.get(PortfolioAccountState, 1)
    assert account_state is None
    session.add(
        PortfolioAccountState(
            id=1,
            cash=10_000.0,
            equity=10_000.0,
            max_drawdown=0.0,
            daily_realized_pnl=-321.25,
            daily_date=datetime.now(timezone.utc).date(),
        )
    )
    session.commit()

    service.run_cycle(["AAPL"])

    assert capturing_guardrails.last_portfolio_state is not None
    assert capturing_guardrails.last_portfolio_state["daily_realized_pnl"] == -321.25


def test_bot_cycle_does_not_duplicate_sell_when_open_sell_reservation_exists():
    service, fake_trading, _ = _build_service()
    fake_trading.open_orders = [
        FakeOrder(
            id="existing-sell-1",
            symbol="AAPL",
            qty=2.0,
            side="sell",
            type="limit",
            time_in_force="day",
            status="new",
        )
    ]

    result = service.run_cycle(["AAPL"])

    assert result["submitted_orders"] == []
    assert result["blocked_orders"] == []
    assert result["decision_summaries"]["AAPL"]["decision_reason"] != "insufficient_qty_after_open_sell_reservations"
    assert result["monitoring_decision"].acted is False
    assert result["monitoring_decision"].inaction_reasons


def test_bot_cycle_revalidates_stale_order_replacements_through_risk(monkeypatch):
    service, fake_trading, _ = _build_service()
    rejecting_guardrails = RejectingRiskGuardrails(
        rejection_reason="max_position_pct_exceeded",
        min_avg_dollar_volume=1.0,
    )
    service.risk_guardrails = rejecting_guardrails
    service.order_ttl_seconds = 1
    service.order_replace_enabled = True
    service.order_replace_slippage_bps = 5.0
    service.order_replace_price_band_bps = 10.0

    fake_trading.open_orders = [
        FakeOrder(
            id="stale-buy-1",
            symbol="AAPL",
            qty=1.0,
            side="buy",
            type="limit",
            time_in_force="day",
            status="new",
            submitted_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
            limit_price=130.0,
        )
    ]

    monkeypatch.setattr(
        BotCycleService,
        "_build_signal_inputs",
        lambda self, symbols: (
            {"AAPL": {"last_price": 130.0, "avg_dollar_volume": 1_000_000.0}},
            {"AAPL": {"symbol": "AAPL"}},
            {},
        ),
    )
    monkeypatch.setattr(
        BotCycleService,
        "_plan_targets_and_deltas",
        lambda self, **kwargs: {
            "policy_decisions": {},
            "exit_policy_actions": {},
            "exit_policy_state": {},
            "target_weights": {},
            "optimizer_allocation_diagnostics": {},
            "adjusted_target_weights": {},
            "sized_targets": {},
            "portfolio_action_artifacts": [],
            "policy_input_signal_bundle": SignalBundle(
                as_of=datetime.now(timezone.utc),
                benchmark_symbol="SPY",
                intents=[],
            ),
            "signal_bundle": SignalBundle(as_of=datetime.now(timezone.utc), benchmark_symbol="SPY", intents=[]),
            "scenario_bundle": ScenarioBundle(as_of=datetime.now(timezone.utc), forecast_horizon="30m", regime_label="neutral", scenarios=[]),
            "allocation_proposal": AllocationProposal(
                as_of=datetime.now(timezone.utc),
                source_signal_bundle_at=datetime.now(timezone.utc),
                target_gross_exposure=0.0,
                cash_buffer=0.05,
                lines=[],
            ),
            "risk_adjusted_allocation": RiskAdjustedAllocation(as_of=datetime.now(timezone.utc), approved_lines=[]),
            "decision_policy_output": DecisionPolicyOutput(
                as_of=datetime.now(timezone.utc),
                approved_signal_bundle=SignalBundle(
                    as_of=datetime.now(timezone.utc),
                    benchmark_symbol="SPY",
                    intents=[],
                ),
                decisions=[],
            ),
            "exit_policy_output": ExitPolicyOutput(
                as_of=datetime.now(timezone.utc),
                adjusted_signal_bundle=SignalBundle(
                    as_of=datetime.now(timezone.utc),
                    benchmark_symbol="SPY",
                    intents=[],
                ),
                directives=[],
            ),
            "order_proposals": [],
            "no_delta_reason": "test_no_deltas",
            "open_sell_reservations": {},
            "current_positions": {"AAPL": 0.0},
            "equity": 10_000.0,
        },
    )
    monkeypatch.setattr(
        BotCycleService,
        "_reconcile_portfolio",
        lambda self, cycle_id, started_at, execution_result: ReconciliationResult(
            cycle_id=cycle_id,
            as_of=started_at,
            status="ok",
            account_state={"cash": 10_000.0},
        ),
    )

    result = service.run_cycle(["AAPL"])

    assert fake_trading.cancelled_order_ids == ["stale-buy-1"]
    assert fake_trading.submissions == []
    assert len(rejecting_guardrails.calls) == 1
    assert rejecting_guardrails.calls[0]["candidate_order"]["symbol"] == "AAPL"
    assert rejecting_guardrails.calls[0]["candidate_order"]["side"] == "buy"
    assert rejecting_guardrails.calls[0]["candidate_order"]["qty"] == 1.0
    assert result["submitted_orders"] == []
    assert any(
        action["action"] == "replace_blocked" or action["reason"] == "max_position_pct_exceeded"
        for action in result["order_lifecycle_actions"]
    )


def test_bot_cycle_defers_stale_order_replacements_to_primary_execution(monkeypatch):
    service, fake_trading, _ = _build_service()
    service.order_ttl_seconds = 1
    service.order_replace_enabled = True
    service.order_replace_slippage_bps = 5.0
    service.order_replace_price_band_bps = 10.0

    fake_trading.open_orders = [
        FakeOrder(
            id="stale-buy-2",
            symbol="AAPL",
            qty=1.0,
            side="buy",
            type="limit",
            time_in_force="day",
            status="new",
            submitted_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
            limit_price=130.0,
        )
    ]

    monkeypatch.setattr(
        BotCycleService,
        "_build_signal_inputs",
        lambda self, symbols: (
            {"AAPL": {"last_price": 130.0, "avg_dollar_volume": 1_000_000.0}},
            {"AAPL": {"symbol": "AAPL"}},
            {},
        ),
    )
    monkeypatch.setattr(
        BotCycleService,
        "_plan_targets_and_deltas",
        lambda self, **kwargs: {
            "policy_decisions": {},
            "exit_policy_actions": {},
            "exit_policy_state": {},
            "target_weights": {},
            "optimizer_allocation_diagnostics": {},
            "adjusted_target_weights": {},
            "sized_targets": {},
            "portfolio_action_artifacts": [],
            "policy_input_signal_bundle": SignalBundle(
                as_of=datetime.now(timezone.utc),
                benchmark_symbol="SPY",
                intents=[],
            ),
            "signal_bundle": SignalBundle(as_of=datetime.now(timezone.utc), benchmark_symbol="SPY", intents=[]),
            "scenario_bundle": ScenarioBundle(as_of=datetime.now(timezone.utc), forecast_horizon="30m", regime_label="neutral", scenarios=[]),
            "allocation_proposal": AllocationProposal(
                as_of=datetime.now(timezone.utc),
                source_signal_bundle_at=datetime.now(timezone.utc),
                target_gross_exposure=0.0,
                cash_buffer=0.05,
                lines=[],
            ),
            "risk_adjusted_allocation": RiskAdjustedAllocation(as_of=datetime.now(timezone.utc), approved_lines=[]),
            "decision_policy_output": DecisionPolicyOutput(
                as_of=datetime.now(timezone.utc),
                approved_signal_bundle=SignalBundle(
                    as_of=datetime.now(timezone.utc),
                    benchmark_symbol="SPY",
                    intents=[],
                ),
                decisions=[],
            ),
            "exit_policy_output": ExitPolicyOutput(
                as_of=datetime.now(timezone.utc),
                adjusted_signal_bundle=SignalBundle(
                    as_of=datetime.now(timezone.utc),
                    benchmark_symbol="SPY",
                    intents=[],
                ),
                directives=[],
            ),
            "order_proposals": [],
            "no_delta_reason": "test_no_deltas",
            "open_sell_reservations": {},
            "current_positions": {"AAPL": 0.0},
            "equity": 10_000.0,
        },
    )
    monkeypatch.setattr(
        BotCycleService,
        "_reconcile_portfolio",
        lambda self, cycle_id, started_at, execution_result: ReconciliationResult(
            cycle_id=cycle_id,
            as_of=started_at,
            status="ok",
            account_state={"cash": 10_000.0},
        ),
    )

    result = service.run_cycle(["AAPL"])

    assert fake_trading.cancelled_order_ids == ["stale-buy-2"]
    assert fake_trading.submissions == []
    assert any(
        action["action"] == "replacement_deferred"
        or action["reason"] == "replacement_deferred_to_primary_execution"
        for action in result["order_lifecycle_actions"]
    )


def test_bot_cycle_persists_symbol_lineage_and_monitoring_context():
    service, _, session = _build_service()

    result = service.run_cycle(["AAPL", "MSFT"])
    snapshot = session.query(BotCycleSnapshot).one()

    assert result["monitoring_decision"].diagnostics["scenario_context"]["regime_label"] == result["scenario_bundle"].regime_label
    assert "AAPL" in result["symbol_lineage"]
    assert result["symbol_lineage"]["AAPL"]["scenario"]["regime_label"] == result["scenario_bundle"].regime_label
    assert snapshot.payload["symbol_lineage"]["AAPL"]["monitoring"]["alerts"] is not None
    assert snapshot.payload["symbol_lineage"]["AAPL"]["execution"] is not None


def test_monitoring_decision_degrades_when_risk_blocks_allocation() -> None:
    now = datetime.now(timezone.utc)
    signal_bundle = SignalBundle(as_of=now, benchmark_symbol="SPY", intents=[])
    monitoring = BotCycleService._build_monitoring_decision(
        as_of=now,
        execution_result=ExecutionResult(
            cycle_id="cycle-1",
            as_of=now,
            attempts=[],
            summary="submitted=0 blocked=0",
        ),
        reconciliation_result=ReconciliationResult(
            cycle_id="cycle-1",
            as_of=now,
            status="ok",
            account_state={"cash": 10_000.0},
        ),
        decision_summaries={"AAPL": {"decision_status": "BLOCKED", "decision_reason": "risk_blocked"}},
        no_delta_reason="HAS_DELTAS",
        scenario_bundle=ScenarioBundle(as_of=now, forecast_horizon="30m", regime_label="neutral", scenarios=[]),
        risk_adjusted_allocation=RiskAdjustedAllocation(
            as_of=now,
            approved_lines=[],
            symbol_details=[
                RiskAllocationDetail(
                    symbol="AAPL",
                    status="blocked",
                    requested_weight=0.20,
                    approved_weight=0.0,
                    clip_amount=0.20,
                    reasons=[PolicyConstraint(code="max_position_pct_exceeded", message="too large")],
                )
            ],
        ),
        decision_policy_output=DecisionPolicyOutput(
            as_of=now,
            approved_signal_bundle=signal_bundle,
            decisions=[],
            source_signal_bundle_at=now,
        ),
        exit_policy_output=ExitPolicyOutput(
            as_of=now,
            adjusted_signal_bundle=signal_bundle,
            directives=[],
        ),
        portfolio_actions=[],
    )

    assert monitoring.status == "degraded"
    assert monitoring.blocked_symbols == ["AAPL"]
    assert "AAPL:max_position_pct_exceeded" in monitoring.inaction_reasons


def test_bot_cycle_uses_horizontal_debug_report(monkeypatch) -> None:
    service, _, _ = _build_service()
    captured: dict[str, object] = {}

    def _capture_report(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("agent_service.bot_cycle.print_cycle_debug_report", _capture_report)

    result = service.run_cycle(["AAPL", "MSFT"])

    assert captured["cycle_id"] == result["cycle_id"]
    assert captured["decision_summaries"] == result["decision_summaries"]
    assert captured["execution_result"] is result["execution_result"]
    assert captured["monitoring_decision"] is result["monitoring_decision"]
