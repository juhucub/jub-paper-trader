from __future__ import annotations

from datetime import datetime, timezone

from agent_service.interfaces import SignalBundle, SignalIntent
from agent_service.scenario import ScenarioGenerator


def test_scenario_generator_emits_multi_scenario_auditable_bundle() -> None:
    now = datetime(2026, 4, 16, 15, 30, tzinfo=timezone.utc)
    bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(symbol="AAPL", direction="long", action="buy", score=0.4, confidence=0.8, normalized_score=0.7, rank=1),
            SignalIntent(symbol="MSFT", direction="long", action="buy", score=0.2, confidence=0.7, normalized_score=0.4, rank=2),
        ],
    )

    scenario_bundle = ScenarioGenerator().build(
        signal_bundle=bundle,
        market_state={
            "volatility": 0.04,
            "liquidity": {"AAPL": 2_000_000.0, "MSFT": 1_500_000.0},
        },
        as_of=now,
    )

    scenario_names = [scenario.name for scenario in scenario_bundle.scenarios]
    probability_sum = sum(scenario.probability for scenario in scenario_bundle.scenarios)

    assert scenario_names == [
        "base_case",
        "downside_stress",
        "volatility_expansion",
        "liquidity_stress",
        "regime_conditioned",
    ]
    assert round(probability_sum, 8) == 1.0
    assert scenario_bundle.regime_label == "risk_on"
    assert scenario_bundle.regime_confidence > 0.0
    assert scenario_bundle.source_signal_bundle_at == now
    assert scenario_bundle.lineage["signal_bundle_as_of"] == now.isoformat()
    assert "scenario_matrix" in scenario_bundle.diagnostics
    assert "AAPL" in scenario_bundle.scenarios[0].symbol_impacts
    assert "shocked_return" in scenario_bundle.scenarios[0].symbol_impacts["AAPL"]


def test_scenario_generator_marks_anomalies_for_missing_signals_and_stress() -> None:
    now = datetime(2026, 4, 16, 15, 30, tzinfo=timezone.utc)
    scenario_bundle = ScenarioGenerator().build(
        signal_bundle=SignalBundle(as_of=now, benchmark_symbol="SPY", intents=[]),
        market_state={"volatility": 0.12, "liquidity": {"AAPL": 25_000.0}},
        as_of=now,
    )

    assert "no_signal_intents" in scenario_bundle.anomaly_flags
    assert "elevated_market_volatility" in scenario_bundle.anomaly_flags
    assert "liquidity_stress_detected" in scenario_bundle.anomaly_flags
    assert scenario_bundle.scenarios[1].regime == "risk_off"
    assert scenario_bundle.scenarios[3].name == "liquidity_stress"
