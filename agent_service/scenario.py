"""Scenario-layer artifacts for the live trading cycle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from statistics import mean, pstdev
from typing import Any

from agent_service.interfaces import Scenario, ScenarioBundle, SignalBundle


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return default
    if not isfinite(candidate):
        return default
    return candidate


def _clamp(value: float, floor: float, ceiling: float) -> float:
    return max(floor, min(ceiling, value))


@dataclass(slots=True, frozen=True)
class ScenarioGenerator:
    """Build auditable scenario bundles from typed signal inputs and market context."""

    forecast_horizon: str = "30m"
    elevated_volatility_threshold: float = 0.08
    elevated_liquidity_stress_threshold: float = 0.06
    elevated_dispersion_threshold: float = 0.45

    def build(
        self,
        signal_bundle: SignalBundle,
        market_state: dict[str, Any],
        as_of: datetime | None = None,
    ) -> ScenarioBundle:
        scenario_as_of = as_of or signal_bundle.as_of
        metrics = self._collect_metrics(signal_bundle=signal_bundle, market_state=market_state)

        regime_probabilities = self._compute_regime_probabilities(metrics)
        regime_label, regime_confidence = max(regime_probabilities.items(), key=lambda item: item[1])
        anomaly_flags, uncertainty_flags = self._build_flags(signal_bundle=signal_bundle, metrics=metrics)
        scenarios = self._build_scenarios(
            signal_bundle=signal_bundle,
            regime_label=regime_label,
            regime_confidence=regime_confidence,
            regime_probabilities=regime_probabilities,
            metrics=metrics,
            uncertainty_flags=uncertainty_flags,
            anomaly_flags=anomaly_flags,
        )

        scenario_matrix = {
            scenario.name: {
                symbol: impact.get("shocked_return", 0.0)
                for symbol, impact in scenario.symbol_impacts.items()
            }
            for scenario in scenarios
        }

        notes = [
            "Scenario layer emits explicit base, downside, volatility, liquidity, and regime-conditioned cases.",
            "Scenario symbol impacts are optimizer-ready and remain proposal-only.",
        ]

        return ScenarioBundle(
            as_of=scenario_as_of,
            forecast_horizon=self.forecast_horizon,
            regime_label=regime_label,
            scenarios=scenarios,
            regime_confidence=regime_confidence,
            regime_probabilities=regime_probabilities,
            anomaly_flags=anomaly_flags,
            notes=notes,
            diagnostics={
                "signal_summary": {
                    "intent_count": metrics["intent_count"],
                    "average_score": metrics["average_score"],
                    "average_confidence": metrics["average_confidence"],
                    "score_dispersion": metrics["score_dispersion"],
                    "long_fraction": metrics["long_fraction"],
                    "short_fraction": metrics["short_fraction"],
                },
                "market_inputs": {
                    "volatility": metrics["market_volatility"],
                    "liquidity_stress": metrics["liquidity_stress"],
                    "benchmark_return": metrics["benchmark_return"],
                },
                "regime_evidence": metrics["regime_evidence"],
                "uncertainty_flags": uncertainty_flags,
                "scenario_matrix": scenario_matrix,
                "dominant_regime": regime_label,
            },
            source_signal_bundle_at=signal_bundle.as_of,
            lineage={
                "signal_bundle_as_of": signal_bundle.as_of.isoformat(),
                "signal_model_name": signal_bundle.model_name,
            },
        )

    def _collect_metrics(self, signal_bundle: SignalBundle, market_state: dict[str, Any]) -> dict[str, Any]:
        intents = signal_bundle.intents
        scores = [float(intent.score) for intent in intents]
        confidences = [float(intent.confidence) for intent in intents]

        intent_count = len(intents)
        average_score = mean(scores) if scores else 0.0
        average_confidence = mean(confidences) if confidences else 0.0
        score_dispersion = pstdev(scores) if len(scores) > 1 else 0.0
        long_count = sum(1 for intent in intents if intent.direction == "long")
        short_count = sum(1 for intent in intents if intent.direction == "short")
        long_fraction = (long_count / intent_count) if intent_count else 0.0
        short_fraction = (short_count / intent_count) if intent_count else 0.0

        market_volatility_raw = market_state.get("volatility")
        liquidity_stress_raw = market_state.get("liquidity_stress", market_state.get("illiquidity_score"))
        market_volatility = _safe_float(market_volatility_raw, 0.0)
        if liquidity_stress_raw is None:
            liquidity_map = market_state.get("liquidity", {}) or {}
            liquidity_values = [
                _safe_float(value, 0.0)
                for value in liquidity_map.values()
                if _safe_float(value, 0.0) > 0.0
            ]
            average_liquidity = mean(liquidity_values) if liquidity_values else 0.0
            liquidity_stress = (
                # Multi-million ADV names should only trip stress once liquidity
                # meaningfully degrades, not under ordinary liquid conditions.
                _clamp(100_000.0 / max(average_liquidity, 1.0), 0.0, 1.0)
                if average_liquidity > 0.0
                else 0.0
            )
        else:
            liquidity_stress = _safe_float(liquidity_stress_raw, 0.0)

        benchmark_return = _safe_float(market_state.get("benchmark_return", market_state.get("market_return")), 0.0)

        risk_on_signal = _clamp(
            max(average_score, 0.0) * 2.4
            + max(benchmark_return, 0.0) * 3.0
            + max(0.0, 1.0 - (market_volatility / max(self.elevated_volatility_threshold, 1e-6))) * 0.35,
            0.0,
            1.0,
        )
        risk_off_signal = _clamp(
            max(-average_score, 0.0) * 2.4
            + max(
                0.0,
                (market_volatility - self.elevated_volatility_threshold)
                / max(self.elevated_volatility_threshold, 1e-6),
            )
            * 0.55
            + max(0.0, liquidity_stress - self.elevated_liquidity_stress_threshold) * 4.0,
            0.0,
            1.0,
        )
        neutral_signal = _clamp(
            0.25
            + max(0.0, 0.55 - abs(average_score) * 3.0)
            + max(0.0, 0.30 - abs(benchmark_return) * 2.5),
            0.0,
            1.0,
        )

        return {
            "intent_count": intent_count,
            "average_score": average_score,
            "average_confidence": average_confidence,
            "score_dispersion": score_dispersion,
            "long_fraction": long_fraction,
            "short_fraction": short_fraction,
            "market_volatility": market_volatility,
            "liquidity_stress": liquidity_stress,
            "benchmark_return": benchmark_return,
            "regime_evidence": {
                "risk_on": risk_on_signal,
                "neutral": neutral_signal,
                "risk_off": risk_off_signal,
            },
        }

    def _compute_regime_probabilities(self, metrics: dict[str, Any]) -> dict[str, float]:
        evidence = dict(metrics["regime_evidence"])
        total = sum(evidence.values())
        if total <= 0.0:
            return {"risk_on": 0.25, "neutral": 0.50, "risk_off": 0.25}
        return {label: value / total for label, value in evidence.items()}

    def _build_flags(self, signal_bundle: SignalBundle, metrics: dict[str, Any]) -> tuple[list[str], list[str]]:
        anomaly_flags: list[str] = []
        uncertainty_flags: list[str] = []

        if not signal_bundle.intents:
            anomaly_flags.append("no_signal_intents")
        if float(metrics["market_volatility"]) >= self.elevated_volatility_threshold:
            anomaly_flags.append("elevated_market_volatility")
        if float(metrics["liquidity_stress"]) >= self.elevated_liquidity_stress_threshold:
            anomaly_flags.append("liquidity_stress_detected")

        if float(metrics["average_confidence"]) < 0.45:
            uncertainty_flags.append("low_signal_confidence")
        if int(metrics["intent_count"]) <= 1:
            uncertainty_flags.append("thin_signal_sample")
        if float(metrics["score_dispersion"]) >= self.elevated_dispersion_threshold:
            uncertainty_flags.append("dispersed_signal_scores")

        return anomaly_flags, uncertainty_flags

    def _build_scenarios(
        self,
        signal_bundle: SignalBundle,
        regime_label: str,
        regime_confidence: float,
        regime_probabilities: dict[str, float],
        metrics: dict[str, Any],
        uncertainty_flags: list[str],
        anomaly_flags: list[str],
    ) -> list[Scenario]:
        market_volatility = float(metrics["market_volatility"])
        liquidity_stress = float(metrics["liquidity_stress"])
        downside_severity = _clamp(0.02 + market_volatility * 0.9 + liquidity_stress * 0.6, 0.02, 0.22)
        stress_severity = _clamp(0.015 + market_volatility * 0.8 + liquidity_stress * 0.9, 0.015, 0.20)

        raw_probabilities = {
            "base_case": max(regime_confidence, 0.28),
            "downside_stress": max(regime_probabilities.get("risk_off", 0.0), 0.14),
            "volatility_expansion": max(market_volatility * 6.0, 0.12),
            "liquidity_stress": max(liquidity_stress * 7.0, 0.12),
            "regime_conditioned": max(regime_confidence * 0.75, 0.18),
        }
        probability_total = sum(raw_probabilities.values()) or 1.0
        probabilities = {
            name: probability / probability_total
            for name, probability in raw_probabilities.items()
        }

        base_shock_map = self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.25, upside_scale=0.60)
        downside_shock_map = self._build_shock_map(signal_bundle=signal_bundle, downside_scale=1.00, upside_scale=0.00)
        volatility_shock_map = self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.55, upside_scale=0.05)
        liquidity_shock_map = self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.70, upside_scale=0.00)
        regime_shock_map = self._build_regime_conditioned_shock_map(signal_bundle=signal_bundle, regime_label=regime_label)

        scenario_specs: list[dict[str, Any]] = [
            {
                "name": "base_case",
                "regime": regime_label,
                "probability": probabilities["base_case"],
                "confidence": regime_confidence,
                "expected_volatility": _clamp(market_volatility or 0.01, 0.01, 1.0),
                "expected_drawdown": _clamp(downside_severity * 0.45, 0.01, 0.12),
                "liquidity_stress": liquidity_stress,
                "shock_map": base_shock_map,
                "rationale": f"Base case reflects the dominant regime {regime_label}.",
            },
            {
                "name": "downside_stress",
                "regime": "risk_off",
                "probability": probabilities["downside_stress"],
                "confidence": _clamp(regime_probabilities.get("risk_off", 0.0) + liquidity_stress * 0.5, 0.0, 1.0),
                "expected_volatility": _clamp(max(market_volatility * 1.4, market_volatility + 0.03), 0.03, 1.0),
                "expected_drawdown": downside_severity,
                "liquidity_stress": liquidity_stress,
                "shock_map": downside_shock_map,
                "rationale": "Downside stress captures deteriorating signal conviction and broad risk-off pressure.",
            },
            {
                "name": "volatility_expansion",
                "regime": "risk_off" if market_volatility >= self.elevated_volatility_threshold else "neutral",
                "probability": probabilities["volatility_expansion"],
                "confidence": _clamp(market_volatility * 5.0, 0.0, 1.0),
                "expected_volatility": _clamp(max(market_volatility * 1.8, 0.05), 0.05, 1.0),
                "expected_drawdown": _clamp(stress_severity * 0.9, 0.02, 0.18),
                "liquidity_stress": _clamp(liquidity_stress * 0.8, 0.0, 1.0),
                "shock_map": volatility_shock_map,
                "rationale": "Volatility expansion isolates higher realized vol even without maximum liquidity stress.",
            },
            {
                "name": "liquidity_stress",
                "regime": "risk_off" if liquidity_stress >= self.elevated_liquidity_stress_threshold else "neutral",
                "probability": probabilities["liquidity_stress"],
                "confidence": _clamp(liquidity_stress * 5.0, 0.0, 1.0),
                "expected_volatility": _clamp(max(market_volatility * 1.3, 0.03 + liquidity_stress * 0.5), 0.03, 1.0),
                "expected_drawdown": stress_severity,
                "liquidity_stress": _clamp(max(liquidity_stress, 0.08), 0.0, 1.0),
                "shock_map": liquidity_shock_map,
                "rationale": "Liquidity stress isolates execution fragility and spread widening risk.",
            },
            {
                "name": "regime_conditioned",
                "regime": regime_label,
                "probability": probabilities["regime_conditioned"],
                "confidence": regime_confidence,
                "expected_volatility": _clamp(
                    market_volatility * (0.9 if regime_label == "risk_on" else 1.15 if regime_label == "neutral" else 1.45),
                    0.01,
                    1.0,
                ),
                "expected_drawdown": _clamp(
                    downside_severity * (0.5 if regime_label == "risk_on" else 0.8 if regime_label == "neutral" else 1.15),
                    0.01,
                    0.22,
                ),
                "liquidity_stress": _clamp(liquidity_stress * (0.8 if regime_label == "risk_on" else 1.0 if regime_label == "neutral" else 1.2), 0.0, 1.0),
                "shock_map": regime_shock_map,
                "rationale": "Regime-conditioned case adapts shocked returns to the dominant market backdrop.",
            },
        ]

        scenarios: list[Scenario] = []
        for spec in scenario_specs:
            scenarios.append(
                Scenario(
                    name=spec["name"],
                    regime=spec["regime"],
                    probability=spec["probability"],
                    confidence=spec["confidence"],
                    expected_volatility=spec["expected_volatility"],
                    expected_drawdown=spec["expected_drawdown"],
                    liquidity_stress=spec["liquidity_stress"],
                    rationale=spec["rationale"],
                    shock_map=spec["shock_map"],
                    symbol_impacts=self._build_symbol_impacts(
                        signal_bundle=signal_bundle,
                        scenario_name=spec["name"],
                        shock_map=spec["shock_map"],
                        regime_probabilities=regime_probabilities,
                        expected_drawdown=spec["expected_drawdown"],
                        expected_volatility=spec["expected_volatility"],
                        liquidity_stress=spec["liquidity_stress"],
                        uncertainty_flags=uncertainty_flags,
                        anomaly_flags=anomaly_flags,
                        scenario_confidence=spec["confidence"],
                    ),
                )
            )
        return scenarios

    def _build_shock_map(
        self,
        signal_bundle: SignalBundle,
        downside_scale: float,
        upside_scale: float,
    ) -> dict[str, float]:
        shock_map: dict[str, float] = {signal_bundle.benchmark_symbol: round(-0.01 * downside_scale, 4)}
        for intent in signal_bundle.intents:
            directional_edge = (
                abs(intent.expected_return)
                or abs(intent.score) * 0.04
                or abs(intent.normalized_score) * 0.03
            )
            downside = directional_edge * downside_scale
            upside = directional_edge * upside_scale
            if intent.direction == "short":
                shock = round(downside - upside, 4)
            elif intent.direction == "long":
                shock = round(upside - downside, 4)
            else:
                shock = 0.0
            shock_map[intent.symbol] = shock
        return shock_map

    def _build_regime_conditioned_shock_map(self, signal_bundle: SignalBundle, regime_label: str) -> dict[str, float]:
        if regime_label == "risk_on":
            return self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.20, upside_scale=0.70)
        if regime_label == "risk_off":
            return self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.90, upside_scale=0.05)
        return self._build_shock_map(signal_bundle=signal_bundle, downside_scale=0.45, upside_scale=0.25)

    def _build_symbol_impacts(
        self,
        signal_bundle: SignalBundle,
        scenario_name: str,
        shock_map: dict[str, float],
        regime_probabilities: dict[str, float],
        expected_drawdown: float,
        expected_volatility: float,
        liquidity_stress: float,
        uncertainty_flags: list[str],
        anomaly_flags: list[str],
        scenario_confidence: float,
    ) -> dict[str, dict[str, Any]]:
        symbol_impacts: dict[str, dict[str, Any]] = {}
        for intent in signal_bundle.intents:
            signal_uncertainty = dict(intent.diagnostics.get("uncertainty", {}))
            shock = float(shock_map.get(intent.symbol, 0.0))
            symbol_impacts[intent.symbol] = {
                "scenario": scenario_name,
                "direction": intent.direction,
                "action": intent.action,
                "shock": shock,
                "shocked_return": float(intent.expected_return) + shock,
                "score": intent.score,
                "confidence": intent.confidence,
                "expected_return": intent.expected_return,
                "downside_severity": expected_drawdown,
                "expected_volatility": expected_volatility,
                "liquidity_stress": liquidity_stress,
                "scenario_confidence": scenario_confidence,
                "uncertainty_score": float(signal_uncertainty.get("score", 0.0) or 0.0),
                "anomaly_flags": sorted(
                    set(anomaly_flags + list(signal_uncertainty.get("anomaly_flags", [])))
                ),
                "uncertainty_flags": list(uncertainty_flags),
                "rationale": intent.rationale,
                "regime_probabilities": regime_probabilities,
            }
        return symbol_impacts
