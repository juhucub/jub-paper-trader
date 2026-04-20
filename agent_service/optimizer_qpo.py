"""Scenario-aware optimizer layer used by the orchestration cycle."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from statistics import mean
from typing import Any

from agent_service.interfaces import (
    AllocationLine,
    AllocationProposal,
    OptimizerConstraintSet,
    OptimizerDiagnostics,
    OptimizerInput,
    ScenarioBundle,
    SignalBundle,
)


def _clamp(value: float, floor: float, ceiling: float) -> float:
    return max(floor, min(ceiling, value))


@dataclass(slots=True)
class ScenarioMeanCvarProxyAllocator:
    """Pure-Python CPU-safe allocator with scenario-aware downside penalties."""

    backend_name: str = "scenario_mean_cvar_proxy"
    downside_penalty: float = 1.35
    volatility_penalty: float = 0.55
    liquidity_penalty: float = 0.45
    uncertainty_penalty: float = 0.30
    minimum_confidence: float = 0.05

    def allocate(self, optimizer_input: OptimizerInput) -> tuple[dict[str, float], OptimizerDiagnostics]:
        constraints = optimizer_input.constraints or OptimizerConstraintSet(
            max_symbol_weight=0.20,
            cash_buffer=0.05,
        )
        scenario_probabilities = optimizer_input.scenario_probabilities or self._equal_scenario_probabilities(
            optimizer_input.scenario_returns
        )

        composite_scores: dict[str, float] = {}
        per_symbol: dict[str, dict[str, Any]] = {}
        weighted_tail_losses: list[float] = []

        for symbol, expected_return in optimizer_input.expected_returns.items():
            scenario_returns = {
                name: float(symbol_returns.get(symbol, 0.0) or 0.0)
                for name, symbol_returns in optimizer_input.scenario_returns.items()
            }
            weighted_downside = sum(
                max(-scenario_return, 0.0) * float(scenario_probabilities.get(name, 0.0) or 0.0)
                for name, scenario_return in scenario_returns.items()
            )
            worst_losses = sorted((max(-scenario_return, 0.0) for scenario_return in scenario_returns.values()), reverse=True)
            tail_loss = mean(worst_losses[: max(1, len(worst_losses) // 2)]) if worst_losses else 0.0
            weighted_tail_losses.append(weighted_downside)

            confidence = float(optimizer_input.confidence_by_symbol.get(symbol, 0.0) or 0.0)
            confidence = max(confidence, self.minimum_confidence)
            volatility_penalty = float(optimizer_input.volatility_by_symbol.get(symbol, 0.0) or 0.0)
            liquidity_penalty = float(optimizer_input.liquidity_risk_by_symbol.get(symbol, 0.0) or 0.0)
            uncertainty_penalty = float(optimizer_input.uncertainty_by_symbol.get(symbol, 0.0) or 0.0)
            current_weight = float(optimizer_input.current_weights.get(symbol, 0.0) or 0.0)
            explicit_cost = (
                float(constraints.transaction_cost_bps + constraints.slippage_bps) / 10_000.0
            ) * max(0.35, 1.0 - (confidence * 0.4))

            upside_score = max(expected_return, 0.0) * confidence
            composite_score = (
                upside_score
                - (self.downside_penalty * weighted_downside)
                - (0.35 * tail_loss)
                - (self.volatility_penalty * volatility_penalty)
                - (self.liquidity_penalty * liquidity_penalty)
                - (self.uncertainty_penalty * uncertainty_penalty)
                - explicit_cost
            )
            if constraints.long_only and expected_return <= 0.0:
                composite_score = 0.0
            composite_scores[symbol] = max(composite_score, 0.0)
            per_symbol[symbol] = {
                "expected_return": expected_return,
                "upside_score": upside_score,
                "weighted_downside": weighted_downside,
                "tail_loss": tail_loss,
                "volatility_penalty": volatility_penalty,
                "liquidity_penalty": liquidity_penalty,
                "uncertainty_penalty": uncertainty_penalty,
                "explicit_cost": explicit_cost,
                "composite_score": max(composite_score, 0.0),
                "current_weight": current_weight,
                "scenario_returns": scenario_returns,
            }

        investable_exposure = max(0.0, min(constraints.max_gross_exposure, 1.0 - constraints.cash_buffer))
        infeasibility_reasons: list[str] = []
        if investable_exposure <= 0.0:
            infeasibility_reasons.append("non_positive_investable_exposure")
        if sum(composite_scores.values()) <= 0.0:
            infeasibility_reasons.append("no_positive_scenario_scores")

        if infeasibility_reasons:
            for symbol, payload in per_symbol.items():
                payload["final_weight"] = 0.0
                payload["turnover_contribution"] = abs(
                    float(optimizer_input.current_weights.get(symbol, 0.0) or 0.0)
                )
            diagnostics = OptimizerDiagnostics(
                as_of=optimizer_input.as_of,
                backend_name=self.backend_name,
                objective_summary={"investable_exposure": investable_exposure},
                per_symbol=per_symbol,
                infeasible=True,
                infeasibility_reasons=infeasibility_reasons,
                scenario_tail_loss=max(weighted_tail_losses, default=0.0),
                source_signal_bundle_at=optimizer_input.source_signal_bundle_at,
                source_scenario_bundle_at=optimizer_input.source_scenario_bundle_at,
                lineage=dict(optimizer_input.lineage),
            )
            return {optimizer_input.benchmark_symbol: 0.0}, diagnostics

        target_weights = self._project_weights_with_caps(
            scores=composite_scores,
            investable_exposure=investable_exposure,
            max_symbol_weight=constraints.max_symbol_weight,
        )
        target_weights = self._apply_turnover_cap(
            target_weights=target_weights,
            current_weights=optimizer_input.current_weights,
            max_turnover=constraints.max_turnover,
        )

        turnover_estimate = sum(
            abs(float(target_weights.get(symbol, 0.0)) - float(optimizer_input.current_weights.get(symbol, 0.0) or 0.0))
            for symbol in set(target_weights) | set(optimizer_input.current_weights)
        )
        transaction_cost_estimate = turnover_estimate * (
            float(constraints.transaction_cost_bps + constraints.slippage_bps) / 10_000.0
        )

        total_weight = sum(target_weights.values())
        cost_adjusted_limit = max(0.0, investable_exposure - transaction_cost_estimate)
        if total_weight > cost_adjusted_limit > 0.0:
            scaler = cost_adjusted_limit / total_weight
            for symbol in list(target_weights):
                target_weights[symbol] *= scaler

        for symbol, payload in per_symbol.items():
            payload["final_weight"] = float(target_weights.get(symbol, 0.0))
            payload["turnover_contribution"] = abs(
                float(target_weights.get(symbol, 0.0))
                - float(optimizer_input.current_weights.get(symbol, 0.0) or 0.0)
            )

        objective_summary = {
            "investable_exposure": investable_exposure,
            "target_expected_return": sum(
                float(target_weights.get(symbol, 0.0)) * float(optimizer_input.expected_returns.get(symbol, 0.0) or 0.0)
                for symbol in target_weights
            ),
            "transaction_cost_estimate": transaction_cost_estimate,
            "turnover_estimate": turnover_estimate,
        }
        diagnostics = OptimizerDiagnostics(
            as_of=optimizer_input.as_of,
            backend_name=self.backend_name,
            objective_summary=objective_summary,
            per_symbol=per_symbol,
            infeasible=False,
            infeasibility_reasons=[],
            turnover_estimate=turnover_estimate,
            transaction_cost_estimate=transaction_cost_estimate,
            scenario_tail_loss=max(weighted_tail_losses, default=0.0),
            diagnostics={
                "scenario_probabilities": scenario_probabilities,
                "regime_label": optimizer_input.regime_label,
            },
            source_signal_bundle_at=optimizer_input.source_signal_bundle_at,
            source_scenario_bundle_at=optimizer_input.source_scenario_bundle_at,
            lineage=dict(optimizer_input.lineage),
        )
        target_weights[optimizer_input.benchmark_symbol] = 0.0
        return target_weights, diagnostics

    @staticmethod
    def _equal_scenario_probabilities(scenario_returns: dict[str, dict[str, float]]) -> dict[str, float]:
        if not scenario_returns:
            return {}
        probability = 1.0 / len(scenario_returns)
        return {name: probability for name in scenario_returns}

    @staticmethod
    def _project_weights_with_caps(
        scores: dict[str, float],
        investable_exposure: float,
        max_symbol_weight: float,
    ) -> dict[str, float]:
        remaining_symbols = {symbol for symbol, score in scores.items() if score > 0.0}
        weights = {symbol: 0.0 for symbol in scores}
        remaining_budget = investable_exposure

        while remaining_symbols and remaining_budget > 0.0:
            total_score = sum(scores[symbol] for symbol in remaining_symbols)
            if total_score <= 0.0:
                break
            capped_this_round = False
            for symbol in list(remaining_symbols):
                proposed_weight = remaining_budget * (scores[symbol] / total_score)
                if proposed_weight > max_symbol_weight:
                    weights[symbol] = max_symbol_weight
                    remaining_budget = max(0.0, remaining_budget - max_symbol_weight)
                    remaining_symbols.remove(symbol)
                    capped_this_round = True
            if capped_this_round:
                continue
            for symbol in remaining_symbols:
                weights[symbol] = remaining_budget * (scores[symbol] / total_score)
            break
        return weights

    @staticmethod
    def _apply_turnover_cap(
        target_weights: dict[str, float],
        current_weights: dict[str, float],
        max_turnover: float,
    ) -> dict[str, float]:
        turnover = sum(
            abs(float(target_weights.get(symbol, 0.0)) - float(current_weights.get(symbol, 0.0) or 0.0))
            for symbol in set(target_weights) | set(current_weights)
        )
        if turnover <= max_turnover or turnover <= 0.0:
            return target_weights

        scaler = max_turnover / turnover
        capped: dict[str, float] = {}
        for symbol in set(target_weights) | set(current_weights):
            current_weight = float(current_weights.get(symbol, 0.0) or 0.0)
            proposed_weight = float(target_weights.get(symbol, 0.0))
            capped[symbol] = _clamp(current_weight + ((proposed_weight - current_weight) * scaler), 0.0, 1.0)
        return capped


@dataclass(slots=True)
class NvidiaQPOAdapter:
    """Optional future adapter seam for an NVIDIA-backed optimizer backend."""

    backend_name: str = "nvidia_qpo_adapter"

    def allocate(self, optimizer_input: OptimizerInput) -> tuple[dict[str, float], OptimizerDiagnostics]:
        raise NotImplementedError(
            "NVIDIA QPO integration is intentionally deferred in the CPU-safe base implementation."
        )


@dataclass(slots=True)
class OptimizerQPO:
    """Typed optimizer boundary with a pure-Python scenario-aware fallback allocator."""

    max_symbol_weight: float = 0.20
    cash_buffer: float = 0.05
    max_gross_exposure: float = 0.95
    max_turnover: float = 0.35
    turnover_penalty: float = 0.15
    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0
    allocator: ScenarioMeanCvarProxyAllocator = field(default_factory=ScenarioMeanCvarProxyAllocator)

    def optimize_allocation(
        self,
        signal_bundle: SignalBundle,
        scenario_bundle: ScenarioBundle,
        current_positions: dict[str, float] | None = None,
        latest_prices: dict[str, float] | None = None,
        equity: float | None = None,
    ) -> AllocationProposal:
        optimizer_input = self.build_optimizer_input(
            signal_bundle=signal_bundle,
            scenario_bundle=scenario_bundle,
            current_positions=current_positions,
            latest_prices=latest_prices,
            equity=equity,
        )
        target_weights, optimizer_diagnostics = self.allocator.allocate(optimizer_input)

        lines: list[AllocationLine] = []
        for intent in signal_bundle.intents:
            symbol = intent.symbol
            lines.append(
                AllocationLine(
                    symbol=symbol,
                    target_weight=float(target_weights.get(symbol, 0.0)),
                    confidence=float(intent.confidence),
                    rationale=intent.rationale or f"allocated from {scenario_bundle.regime_label} scenario set",
                    diagnostics={
                        **optimizer_diagnostics.per_symbol.get(symbol, {}),
                        "direction": intent.direction,
                        "action": intent.action,
                        "score": intent.score,
                        "expected_return": intent.expected_return,
                    },
                )
            )

        diagnostics_payload = asdict(optimizer_diagnostics)
        diagnostics_payload["optimizer_input"] = asdict(optimizer_input)
        diagnostics_payload["scenario_notes"] = list(scenario_bundle.notes)
        diagnostics_payload["scenario_anomaly_flags"] = list(scenario_bundle.anomaly_flags)

        return AllocationProposal(
            as_of=signal_bundle.as_of,
            source_signal_bundle_at=signal_bundle.as_of,
            source_scenario_bundle_at=scenario_bundle.as_of,
            target_gross_exposure=sum(
                float(target_weights.get(intent.symbol, 0.0))
                for intent in signal_bundle.intents
            ),
            cash_buffer=self.cash_buffer,
            lines=lines,
            scenario_regime=scenario_bundle.regime_label,
            optimizer_name=self.__class__.__name__,
            constraints_requested={
                "max_symbol_weight": self.max_symbol_weight,
                "cash_buffer": self.cash_buffer,
                "max_gross_exposure": self.max_gross_exposure,
                "max_turnover": self.max_turnover,
                "transaction_cost_bps": self.transaction_cost_bps,
                "slippage_bps": self.slippage_bps,
            },
            diagnostics=diagnostics_payload,
            lineage={
                "source_signal_bundle_at": signal_bundle.as_of.isoformat(),
                "source_scenario_bundle_at": scenario_bundle.as_of.isoformat(),
                "allocator_backend": optimizer_diagnostics.backend_name,
            },
        )

    def build_optimizer_input(
        self,
        signal_bundle: SignalBundle,
        scenario_bundle: ScenarioBundle,
        current_positions: dict[str, float] | None = None,
        latest_prices: dict[str, float] | None = None,
        equity: float | None = None,
    ) -> OptimizerInput:
        current_positions = current_positions or {}
        latest_prices = latest_prices or {}
        current_weights: dict[str, float] = {}
        if equity and equity > 0.0:
            for symbol, qty in current_positions.items():
                price = float(latest_prices.get(symbol, 0.0) or 0.0)
                if price <= 0.0:
                    continue
                current_weights[symbol] = (float(qty) * price) / equity

        expected_returns: dict[str, float] = {}
        confidence_by_symbol: dict[str, float] = {}
        volatility_by_symbol: dict[str, float] = {}
        liquidity_risk_by_symbol: dict[str, float] = {}
        uncertainty_by_symbol: dict[str, float] = {}
        for intent in signal_bundle.intents:
            expected_returns[intent.symbol] = float(intent.expected_return)
            confidence_by_symbol[intent.symbol] = float(intent.confidence)
            regime_evidence = dict(intent.diagnostics.get("regime_evidence", {}))
            execution_risk = dict(intent.diagnostics.get("execution_risk", {}))
            uncertainty = dict(intent.diagnostics.get("uncertainty", {}))
            volatility_by_symbol[intent.symbol] = float(
                regime_evidence.get("volatility", signal_bundle.feature_snapshot.get(intent.symbol, {}).get("volatility", 0.0))
                or 0.0
            )
            liquidity_risk_by_symbol[intent.symbol] = float(execution_risk.get("liquidity_risk", 0.0) or 0.0)
            uncertainty_by_symbol[intent.symbol] = float(uncertainty.get("score", 0.0) or 0.0)

        scenario_returns: dict[str, dict[str, float]] = {}
        for scenario in scenario_bundle.scenarios:
            scenario_returns[scenario.name] = {
                symbol: float(
                    impact.get("shocked_return", scenario.shock_map.get(symbol, 0.0))
                    or 0.0
                )
                for symbol, impact in scenario.symbol_impacts.items()
            }

        constraints = OptimizerConstraintSet(
            max_symbol_weight=self.max_symbol_weight,
            cash_buffer=self.cash_buffer,
            max_gross_exposure=self.max_gross_exposure,
            max_turnover=self.max_turnover,
            turnover_penalty=self.turnover_penalty,
            transaction_cost_bps=self.transaction_cost_bps,
            slippage_bps=self.slippage_bps,
        )
        return OptimizerInput(
            as_of=signal_bundle.as_of,
            benchmark_symbol=signal_bundle.benchmark_symbol,
            expected_returns=expected_returns,
            scenario_returns=scenario_returns,
            current_weights=current_weights,
            confidence_by_symbol=confidence_by_symbol,
            volatility_by_symbol=volatility_by_symbol,
            liquidity_risk_by_symbol=liquidity_risk_by_symbol,
            uncertainty_by_symbol=uncertainty_by_symbol,
            regime_label=scenario_bundle.regime_label,
            scenario_probabilities={
                scenario.name: float(scenario.probability)
                for scenario in scenario_bundle.scenarios
            },
            constraints=constraints,
            diagnostics={
                "scenario_names": [scenario.name for scenario in scenario_bundle.scenarios],
                "signal_count": len(signal_bundle.intents),
            },
            source_signal_bundle_at=signal_bundle.as_of,
            source_scenario_bundle_at=scenario_bundle.as_of,
            lineage={
                "signal_bundle_as_of": signal_bundle.as_of.isoformat(),
                "scenario_bundle_as_of": scenario_bundle.as_of.isoformat(),
            },
        )

    def optimize_target_weights(
        self,
        signal_intents: dict[str, float | dict[str, float | str]],
        benchmark_symbol: str = "SPY",
        return_diagnostics: bool = False,
    ) -> dict[str, float] | tuple[dict[str, float], dict[str, Any]]:
        """Legacy compatibility helper for older tests and callers."""

        positive_scores: dict[str, float] = {}
        candidate_diagnostics: dict[str, dict[str, Any]] = {}
        for symbol, signal in signal_intents.items():
            if isinstance(signal, dict):
                normalized_score = float(signal.get("normalized_score", 0.0) or 0.0)
                confidence = float(signal.get("confidence", 0.0) or 0.0)
                raw_score = float(signal.get("raw_score", signal.get("strength", 0.0)) or 0.0)
                direction = str(signal.get("direction", "flat")).lower()
                allocation_score = max(0.0, normalized_score) * max(confidence, 0.0)
                if direction != "long":
                    allocation_score = 0.0
                positive_scores[symbol] = allocation_score
                candidate_diagnostics[symbol] = {
                    "raw_score": raw_score,
                    "normalized_score": normalized_score,
                    "allocation_score": allocation_score,
                }
            else:
                positive_scores[symbol] = max(float(signal), 0.0)
                candidate_diagnostics[symbol] = {
                    "raw_score": float(signal),
                    "normalized_score": float(signal),
                    "allocation_score": max(float(signal), 0.0),
                }

        total_score = sum(positive_scores.values())
        investable = max(0.0, min(self.max_gross_exposure, 1.0 - self.cash_buffer))
        if total_score <= 0.0:
            weights = {benchmark_symbol: 0.0}
            diagnostics = {
                "target_gross_exposure": 0.0,
                "investable_exposure_limit": investable,
                "per_symbol": candidate_diagnostics,
            }
            return (weights, diagnostics) if return_diagnostics else weights

        weights = {
            symbol: min(investable * (score / total_score), self.max_symbol_weight)
            for symbol, score in positive_scores.items()
        }
        total_weight = sum(weights.values())
        if total_weight > 0.0 and total_weight != investable:
            scaler = investable / total_weight
            for symbol in list(weights):
                weights[symbol] *= scaler
        weights[benchmark_symbol] = 0.0
        diagnostics = {
            "target_gross_exposure": sum(weights.values()),
            "investable_exposure_limit": investable,
            "per_symbol": candidate_diagnostics,
        }
        return (weights, diagnostics) if return_diagnostics else weights
