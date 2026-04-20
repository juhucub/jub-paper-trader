from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from agent_service.exit_policy import ExitPolicy
from agent_service.interfaces import Scenario, ScenarioBundle, SignalBundle, SignalIntent


@dataclass(slots=True)
class _Position:
    symbol: str
    qty: float
    avg_entry_price: float
    opened_at: datetime | None = None


def test_exit_policy_respects_min_holding_minutes_for_take_profit() -> None:
    policy = ExitPolicy(stop_loss_pct=0.04, take_profit_exit_pct=0.02, min_holding_minutes=15)
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    position = _Position(symbol="AAPL", qty=1.0, avg_entry_price=100.0, opened_at=now - timedelta(minutes=5))

    output = policy.evaluate_positions(
        positions=[position],
        latest_prices={"AAPL": 103.0},
        signal_bundle=SignalBundle(
            as_of=now,
            benchmark_symbol="SPY",
            intents=[SignalIntent(symbol="AAPL", direction="long", action="buy", score=0.05, confidence=0.9)],
            lineage={
                "source_scenario_bundle_at": now,
                "source_decision_policy_at": now,
            },
        ),
        previous_payload={"exit_policy_state": {}, "signals": {}},
        scenario_bundle=ScenarioBundle(
            as_of=now,
            forecast_horizon="30m",
            regime_label="neutral",
            scenarios=[Scenario(name="base_case", regime="neutral", probability=1.0)],
        ),
        now_utc=now,
    )

    directive = output.directives[0]
    assert directive.action == "HOLD"
    assert directive.trigger == "none"
    assert output.adjusted_signal_bundle.intents[0].symbol == "AAPL"
    assert output.source_scenario_bundle_at == now
    assert output.source_decision_policy_at == now
    assert output.adjusted_signal_bundle.lineage["source_decision_policy_at"] == now


def test_exit_policy_emits_exit_directive_and_flattened_intent() -> None:
    policy = ExitPolicy(take_profit_exit_pct=0.02, min_holding_minutes=0)
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    position = _Position(symbol="AAPL", qty=1.0, avg_entry_price=100.0, opened_at=now - timedelta(minutes=60))
    bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[SignalIntent(symbol="AAPL", direction="long", action="buy", score=0.05, confidence=0.9)],
        lineage={
            "source_scenario_bundle_at": now,
            "source_decision_policy_at": now,
        },
    )

    output = policy.evaluate_positions(
        positions=[position],
        latest_prices={"AAPL": 103.0},
        signal_bundle=bundle,
        previous_payload={"exit_policy_state": {}, "signals": {}},
        now_utc=now,
    )

    directive = output.directives[0]
    adjusted_intent = output.adjusted_signal_bundle.intents[0]

    assert directive.action == "EXIT"
    assert directive.trigger == "take_profit_exit_band"
    assert directive.force_target_weight == 0.0
    assert adjusted_intent.direction == "flat"
    assert adjusted_intent.action == "sell"
