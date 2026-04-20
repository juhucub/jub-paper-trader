from __future__ import annotations

from datetime import datetime, timezone

from agent_service.decision_policy import DecisionPolicy
from agent_service.interfaces import Scenario, ScenarioBundle, SignalBundle, SignalIntent


def test_decision_policy_evaluate_emits_typed_output_and_approved_bundle() -> None:
    policy = DecisionPolicy(max_market_volatility=0.2, min_symbol_liquidity=100_000.0)
    now = datetime(2026, 4, 16, 15, 30, tzinfo=timezone.utc)
    signal_bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(
                symbol="AAPL",
                direction="long",
                action="buy",
                score=0.42,
                confidence=0.91,
                normalized_score=0.8,
                rank=1,
                rationale="trend and breadth aligned",
                diagnostics={"strength": 0.42, "expected_horizon": "30m", "z_score": 1.8},
            ),
            SignalIntent(
                symbol="MSFT",
                direction="short",
                action="sell",
                score=-0.2,
                confidence=0.88,
                rationale="mean reversion breakdown",
                diagnostics={"strength": 0.2, "expected_horizon": "30m"},
            ),
        ],
    )
    scenario_bundle = ScenarioBundle(
        as_of=now,
        forecast_horizon="30m",
        regime_label="neutral",
        scenarios=[Scenario(name="base_case", regime="neutral", probability=1.0)],
    )

    output = policy.evaluate(
        signal_bundle=signal_bundle,
        portfolio_state={
            "positions": {"MSFT": 5.0},
            "cash": 5_000.0,
            "equity": 10_000.0,
            "concentration": {"AAPL": 0.1, "MSFT": 0.15},
        },
        market_context={
            "volatility": 0.05,
            "liquidity": {"AAPL": 1_000_000.0, "MSFT": 800_000.0},
        },
        scenario_bundle=scenario_bundle,
        as_of=now,
    )

    decisions = {decision.symbol: decision for decision in output.decisions}

    assert output.source_signal_bundle_at == signal_bundle.as_of
    assert output.source_scenario_bundle_at == scenario_bundle.as_of
    assert output.approved_signal_bundle.as_of == now
    assert output.approved_signal_bundle.lineage["source_decision_policy_at"] == now
    assert [intent.symbol for intent in output.approved_signal_bundle.intents] == ["AAPL", "MSFT"]
    assert decisions["AAPL"].policy_action == "buy"
    assert decisions["AAPL"].diagnostics["regime_label"] == "neutral"
    assert decisions["MSFT"].policy_action == "sell"
    assert decisions["MSFT"].reason == "short_converted_to_exit_only"
    assert decisions["MSFT"].allow_exit_only is True
    assert decisions["MSFT"].approved_intent is not None
    assert decisions["MSFT"].approved_intent.direction == "flat"


def test_decision_policy_skips_low_confidence_and_high_volatility_entries() -> None:
    policy = DecisionPolicy(max_market_volatility=0.05, min_symbol_liquidity=100.0, min_confidence=0.5)
    now = datetime(2026, 4, 16, 15, 30, tzinfo=timezone.utc)
    bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(symbol="AAPL", direction="long", action="buy", score=0.2, confidence=0.3),
            SignalIntent(symbol="NVDA", direction="long", action="buy", score=0.25, confidence=0.9),
        ],
    )

    output = policy.evaluate(
        signal_bundle=bundle,
        portfolio_state={"positions": {}, "cash": 10_000.0, "equity": 10_000.0, "concentration": {}},
        market_context={"volatility": 0.08, "liquidity": {"AAPL": 1_000_000.0, "NVDA": 1_000_000.0}},
        as_of=now,
    )

    decisions = {decision.symbol: decision for decision in output.decisions}

    assert decisions["AAPL"].policy_action == "skip"
    assert decisions["AAPL"].constraints[0].code == "low_confidence"
    assert decisions["NVDA"].policy_action == "skip"
    assert decisions["NVDA"].constraints[0].code == "high_market_volatility"
    assert output.approved_signal_bundle.intents == []
