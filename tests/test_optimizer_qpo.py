from __future__ import annotations

from datetime import datetime, timezone

from agent_service.interfaces import Scenario, ScenarioBundle, SignalBundle, SignalIntent
from agent_service.optimizer_qpo import OptimizerQPO


def _build_signal_bundle(now: datetime) -> SignalBundle:
    return SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(
                symbol="AAPL",
                direction="long",
                action="buy",
                score=0.35,
                confidence=0.9,
                expected_return=0.04,
                normalized_score=0.8,
                rank=1,
                diagnostics={
                    "regime_evidence": {"volatility": 0.03},
                    "execution_risk": {"liquidity_risk": 0.05},
                    "uncertainty": {"score": 0.10},
                },
            ),
            SignalIntent(
                symbol="MSFT",
                direction="long",
                action="buy",
                score=0.18,
                confidence=0.7,
                expected_return=0.02,
                normalized_score=0.5,
                rank=2,
                diagnostics={
                    "regime_evidence": {"volatility": 0.04},
                    "execution_risk": {"liquidity_risk": 0.08},
                    "uncertainty": {"score": 0.15},
                },
            ),
        ],
    )


def test_optimizer_qpo_builds_typed_optimizer_input() -> None:
    now = datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc)
    optimizer = OptimizerQPO(max_symbol_weight=0.4, cash_buffer=0.05)
    signal_bundle = _build_signal_bundle(now)
    scenario_bundle = ScenarioBundle(
        as_of=now,
        forecast_horizon="30m",
        regime_label="risk_on",
        scenarios=[
            Scenario(
                name="base_case",
                regime="risk_on",
                probability=0.6,
                symbol_impacts={
                    "AAPL": {"shocked_return": 0.05},
                    "MSFT": {"shocked_return": 0.03},
                },
            ),
            Scenario(
                name="downside_stress",
                regime="risk_off",
                probability=0.4,
                symbol_impacts={
                    "AAPL": {"shocked_return": -0.04},
                    "MSFT": {"shocked_return": -0.03},
                },
            ),
        ],
    )

    optimizer_input = optimizer.build_optimizer_input(
        signal_bundle=signal_bundle,
        scenario_bundle=scenario_bundle,
        current_positions={"AAPL": 2.0},
        latest_prices={"AAPL": 100.0, "MSFT": 100.0},
        equity=10_000.0,
    )

    assert optimizer_input.constraints is not None
    assert optimizer_input.current_weights["AAPL"] == 0.02
    assert optimizer_input.scenario_returns["downside_stress"]["AAPL"] == -0.04
    assert optimizer_input.confidence_by_symbol["MSFT"] == 0.7


def test_optimizer_qpo_emits_constraint_aware_diagnostics() -> None:
    now = datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc)
    optimizer = OptimizerQPO(max_symbol_weight=0.3, cash_buffer=0.05, max_turnover=0.1)
    signal_bundle = _build_signal_bundle(now)
    scenario_bundle = ScenarioBundle(
        as_of=now,
        forecast_horizon="30m",
        regime_label="risk_on",
        scenarios=[
            Scenario(
                name="base_case",
                regime="risk_on",
                probability=0.5,
                symbol_impacts={
                    "AAPL": {"shocked_return": 0.05},
                    "MSFT": {"shocked_return": 0.025},
                },
            ),
            Scenario(
                name="downside_stress",
                regime="risk_off",
                probability=0.5,
                symbol_impacts={
                    "AAPL": {"shocked_return": -0.05},
                    "MSFT": {"shocked_return": -0.02},
                },
            ),
        ],
    )

    proposal = optimizer.optimize_allocation(
        signal_bundle=signal_bundle,
        scenario_bundle=scenario_bundle,
        current_positions={"AAPL": 8.0, "MSFT": 0.0},
        latest_prices={"AAPL": 100.0, "MSFT": 100.0},
        equity=10_000.0,
    )

    diagnostics = proposal.diagnostics["per_symbol"]
    weights = {line.symbol: line.target_weight for line in proposal.lines}

    assert proposal.constraints_requested["max_symbol_weight"] == 0.3
    assert proposal.diagnostics["backend_name"] == "scenario_mean_cvar_proxy"
    assert diagnostics["AAPL"]["final_weight"] <= 0.3
    assert diagnostics["AAPL"]["turnover_contribution"] >= 0.0
    assert sum(weights.values()) <= proposal.target_gross_exposure + 1e-9

