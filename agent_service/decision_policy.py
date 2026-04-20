from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast, overload

from backend.core.settings import get_settings

from agent_service.interfaces import (
    DecisionPolicyContext,
    DecisionPolicyDecision,
    DecisionPolicyOutput,
    PolicyConstraint,
    ScenarioBundle,
    SignalBundle,
    SignalIntent,
)
from agent_service.interfaces.contracts import Action, Direction, PolicyAction


@dataclass(slots=True)
class DecisionPolicy:
    min_confidence: float = 0.2
    min_score: float = 0.0
    max_position_concentration: float = 0.45
    max_market_volatility: float = 0.08
    min_symbol_liquidity: float = 0.0
    use_structured_signals: bool = get_settings().bot_use_structured_signals

    def evaluate_typed(
        self,
        signal_bundle: SignalBundle,
        context: DecisionPolicyContext,
        scenario_bundle: ScenarioBundle | None = None,
    ) -> DecisionPolicyOutput:
        """Apply deterministic portfolio checks to typed statistical signal intents."""

        decisions: list[DecisionPolicyDecision] = []
        approved_intents: list[SignalIntent] = []

        current_positions = context.current_positions or {}
        concentration = context.concentration_by_symbol or {}
        liquidity_map = context.liquidity_by_symbol or {}
        scenario_regime = context.scenario_regime or (scenario_bundle.regime_label if scenario_bundle else None)

        for intent in signal_bundle.intents:
            strength = self._extract_strength(intent)
            current_qty = float(current_positions.get(intent.symbol, 0.0) or 0.0)
            is_short_intent = intent.action == "sell" or intent.direction == "short"
            allow_exit_only = is_short_intent and current_qty > 0.0

            constraints: list[PolicyConstraint] = []
            if intent.action not in {"buy", "sell", "hold"}:
                constraints.append(
                    PolicyConstraint("unsupported_signal_action", "Signal action is not supported by the policy.")
                )
            if intent.confidence < self.min_confidence:
                constraints.append(
                    PolicyConstraint(
                        "low_confidence",
                        "Signal confidence fell below the deterministic entry floor.",
                        {"min_confidence": self.min_confidence},
                    )
                )
            if intent.action == "buy" and intent.score <= self.min_score:
                constraints.append(
                    PolicyConstraint(
                        "non_positive_score",
                        "Buy intent had a non-positive score, so the policy failed closed.",
                        {"min_score": self.min_score},
                    )
                )
            if intent.action == "buy" and context.portfolio_cash <= 0.0:
                constraints.append(
                    PolicyConstraint("insufficient_cash", "No cash was available for a new long entry.")
                )
            if intent.action == "buy" and context.market_volatility > self.max_market_volatility:
                constraints.append(
                    PolicyConstraint(
                        "high_market_volatility",
                        "Market volatility exceeded the entry threshold.",
                        {"max_market_volatility": self.max_market_volatility},
                    )
                )
            if intent.action == "buy" and float(concentration.get(intent.symbol, 0.0)) >= self.max_position_concentration:
                constraints.append(
                    PolicyConstraint(
                        "position_concentration_limit",
                        "The symbol already sat at or above the concentration cap.",
                        {"max_position_concentration": self.max_position_concentration},
                    )
                )
            if intent.action == "buy" and float(liquidity_map.get(intent.symbol, 0.0)) < self.min_symbol_liquidity:
                constraints.append(
                    PolicyConstraint(
                        "insufficient_liquidity",
                        "Symbol liquidity was below the minimum threshold.",
                        {"min_symbol_liquidity": self.min_symbol_liquidity},
                    )
                )
            if is_short_intent and not allow_exit_only:
                constraints.append(
                    PolicyConstraint(
                        "short_rejected_long_only",
                        "The strategy remains long-only unless a short intent is reducing an existing long.",
                    )
                )

            policy_action: PolicyAction = intent.action
            reason = "policy_approved"
            approved_intent: SignalIntent | None = None

            if constraints:
                policy_action = "skip"
                reason = constraints[0].code
            elif allow_exit_only:
                reason = "short_converted_to_exit_only"
                approved_intent = self._build_exit_only_intent(intent)
            elif intent.action == "hold":
                reason = "signal_hold"
            else:
                approved_intent = self._build_approved_buy_intent(intent, strength)

            decision = DecisionPolicyDecision(
                symbol=intent.symbol,
                requested_intent=intent,
                approved_intent=approved_intent,
                policy_action=policy_action,
                reason=reason,
                constraints=constraints,
                allow_exit_only=allow_exit_only,
                diagnostics={
                    "market_volatility": float(context.market_volatility),
                    "symbol_liquidity": float(liquidity_map.get(intent.symbol, 0.0) or 0.0),
                    "position_concentration": float(concentration.get(intent.symbol, 0.0) or 0.0),
                    "current_qty": current_qty,
                    "regime_label": scenario_regime,
                    "scenario_regime": scenario_regime,
                    "expected_horizon": str(intent.diagnostics.get("expected_horizon", "30m")),
                },
            )
            decisions.append(decision)
            if approved_intent is not None:
                approved_intents.append(approved_intent)

        approved_signal_bundle = SignalBundle(
            as_of=context.as_of,
            benchmark_symbol=signal_bundle.benchmark_symbol,
            intents=approved_intents,
            feature_snapshot=signal_bundle.feature_snapshot,
            model_name=signal_bundle.model_name,
            notes=[*signal_bundle.notes, "decision_policy_filtered"],
            lineage={
                **signal_bundle.lineage,
                "source_signal_bundle_at": signal_bundle.as_of,
                "source_scenario_bundle_at": scenario_bundle.as_of if scenario_bundle else context.source_scenario_bundle_at,
                "source_decision_policy_at": context.as_of,
            },
        )
        return DecisionPolicyOutput(
            as_of=context.as_of,
            approved_signal_bundle=approved_signal_bundle,
            decisions=decisions,
            notes=["Deterministic decision policy evaluated all candidate intents."],
            diagnostics={
                "portfolio_cash": float(context.portfolio_cash),
                "portfolio_equity": float(context.portfolio_equity),
                "market_volatility": float(context.market_volatility),
                "symbols_evaluated": len(signal_bundle.intents),
                "symbols_approved": len(approved_intents),
            },
            source_signal_bundle_at=signal_bundle.as_of,
            source_scenario_bundle_at=scenario_bundle.as_of if scenario_bundle else context.source_scenario_bundle_at,
            lineage={
                "signal_bundle_as_of": signal_bundle.as_of,
                "scenario_bundle_as_of": scenario_bundle.as_of if scenario_bundle else context.source_scenario_bundle_at,
            },
        )

    @overload
    def evaluate(
        self,
        signals: None = None,
        portfolio_state: dict[str, Any] | None = None,
        market_context: dict[str, Any] | None = None,
        *,
        signal_bundle: SignalBundle,
        as_of: datetime | None = None,
        benchmark_symbol: str = "SPY",
        scenario_bundle: ScenarioBundle | None = None,
    ) -> DecisionPolicyOutput: ...

    @overload
    def evaluate(
        self,
        signals: dict[str, dict[str, float | str]],
        portfolio_state: dict[str, Any],
        market_context: dict[str, Any],
        *,
        signal_bundle: None = None,
        as_of: datetime | None = None,
        benchmark_symbol: str = "SPY",
        scenario_bundle: ScenarioBundle | None = None,
    ) -> dict[str, Any]: ...

    def evaluate(
        self,
        signals: dict[str, dict[str, float | str]] | None = None,
        portfolio_state: dict[str, Any] | None = None,
        market_context: dict[str, Any] | None = None,
        *,
        signal_bundle: SignalBundle | None = None,
        as_of: datetime | None = None,
        benchmark_symbol: str = "SPY",
        scenario_bundle: ScenarioBundle | None = None,
    ) -> DecisionPolicyOutput | dict[str, Any]:
        """Typed-first policy API with a legacy dict wrapper for older callers."""

        resolved_as_of = as_of or datetime.now(timezone.utc)
        portfolio_state = portfolio_state or {}
        market_context = market_context or {}
        resolved_signal_bundle = signal_bundle or self._legacy_signals_to_bundle(
            signals=signals or {},
            as_of=resolved_as_of,
            benchmark_symbol=benchmark_symbol,
        )
        context = DecisionPolicyContext(
            as_of=resolved_as_of,
            portfolio_cash=float(portfolio_state.get("cash", 0.0) or 0.0),
            portfolio_equity=float(portfolio_state.get("equity", 0.0) or 0.0),
            current_positions={
                str(symbol).upper(): float(qty or 0.0)
                for symbol, qty in (portfolio_state.get("positions", {}) or {}).items()
            },
            concentration_by_symbol={
                str(symbol).upper(): float(weight or 0.0)
                for symbol, weight in (portfolio_state.get("concentration", {}) or {}).items()
            },
            market_volatility=float(market_context.get("volatility", 0.0) or 0.0),
            liquidity_by_symbol={
                str(symbol).upper(): float(value or 0.0)
                for symbol, value in (market_context.get("liquidity", {}) or {}).items()
            },
            scenario_regime=scenario_bundle.regime_label if scenario_bundle else None,
            source_signal_bundle_at=resolved_signal_bundle.as_of,
            source_scenario_bundle_at=scenario_bundle.as_of if scenario_bundle else None,
        )
        output = self.evaluate_typed(
            signal_bundle=resolved_signal_bundle,
            context=context,
            scenario_bundle=scenario_bundle,
        )
        if signal_bundle is not None:
            return output
        return {
            "approved_candidates": self._output_to_legacy_candidates(output),
            "decisions": self._output_to_legacy_decisions(output),
            "artifact": output,
        }

    @staticmethod
    def _extract_strength(intent: SignalIntent) -> float:
        raw_strength = float(intent.diagnostics.get("strength", abs(intent.score)) or 0.0)
        if intent.direction == "short":
            return -abs(raw_strength)
        if intent.direction == "flat":
            return 0.0
        return abs(raw_strength)

    @staticmethod
    def _build_approved_buy_intent(intent: SignalIntent, strength: float) -> SignalIntent:
        diagnostics = dict(intent.diagnostics)
        diagnostics["strength"] = max(strength, 0.0)
        diagnostics["policy_action"] = "buy"
        diagnostics["policy_reason"] = "policy_approved"
        return SignalIntent(
            symbol=intent.symbol,
            direction="long",
            action="buy",
            score=float(intent.score),
            confidence=float(intent.confidence),
            expected_return=float(intent.expected_return),
            normalized_score=float(intent.normalized_score),
            rank=int(intent.rank),
            rationale=intent.rationale,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _build_exit_only_intent(intent: SignalIntent) -> SignalIntent:
        diagnostics = dict(intent.diagnostics)
        diagnostics["strength"] = 0.0
        diagnostics["policy_action"] = "sell"
        diagnostics["policy_reason"] = "short_converted_to_exit_only"
        return SignalIntent(
            symbol=intent.symbol,
            direction="flat",
            action="hold",
            score=0.0,
            confidence=float(intent.confidence),
            expected_return=0.0,
            normalized_score=float(intent.normalized_score),
            rank=int(intent.rank),
            rationale=intent.rationale,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _legacy_signals_to_bundle(
        signals: dict[str, dict[str, float | str]],
        as_of: datetime,
        benchmark_symbol: str,
    ) -> SignalBundle:
        intents: list[SignalIntent] = []
        universe_size = len(signals)
        for symbol, signal in signals.items():
            direction_str = str(signal.get("direction", "flat")).lower()
            action_str = str(signal.get("action", "hold")).lower()
            strength = float(signal.get("strength", abs(float(signal.get("score", 0.0) or 0.0))) or 0.0)
            score = float(signal.get("score", 0.0) or 0.0)
            if direction_str in {"long", "short", "flat"}:
                action_str = {"long": "buy", "short": "sell", "flat": "hold"}[direction_str]
                if "score" not in signal:
                    score = strength if direction_str == "long" else -strength if direction_str == "short" else 0.0
            diagnostics = {
                key: value
                for key, value in signal.items()
                if key
                not in {
                    "direction",
                    "action",
                    "score",
                    "confidence",
                    "expected_return",
                    "normalized_score",
                    "rank",
                    "rationale",
                }
            }
            diagnostics.setdefault("strength", strength)
            diagnostics.setdefault("expected_horizon", str(signal.get("expected_horizon", "30m")))
            diagnostics.setdefault("universe_size", int(signal.get("universe_size", universe_size) or universe_size))
            direction: Direction = cast(
                Direction,
                direction_str if direction_str in {"long", "short", "flat"} else "flat",
            )
            action: Action = cast(Action, action_str if action_str in {"buy", "sell", "hold"} else "hold")
            intents.append(
                SignalIntent(
                    symbol=str(symbol).upper(),
                    direction=direction,
                    action=action,
                    score=score,
                    confidence=float(signal.get("confidence", 0.0) or 0.0),
                    expected_return=float(signal.get("expected_return", 0.0) or 0.0),
                    normalized_score=float(signal.get("normalized_score", signal.get("z_score", 0.0)) or 0.0),
                    rank=int(signal.get("rank", 0) or 0),
                    rationale=str(signal.get("rationale", "")),
                    diagnostics=diagnostics,
                )
            )
        return SignalBundle(
            as_of=as_of,
            benchmark_symbol=benchmark_symbol,
            intents=intents,
            model_name="legacy_signal_dict",
        )

    def _output_to_legacy_candidates(self, output: DecisionPolicyOutput) -> dict[str, float | dict[str, float | str]]:
        approved_by_symbol = {intent.symbol: intent for intent in output.approved_signal_bundle.intents}
        payload: dict[str, float | dict[str, float | str]] = {}
        for decision in output.decisions:
            if decision.policy_action not in {"buy", "sell"} or decision.approved_intent is None:
                continue
            if not self.use_structured_signals:
                payload[decision.symbol] = (
                    max(decision.requested_intent.score, 0.0) if decision.policy_action == "buy" else 0.0
                )
                continue
            intent = approved_by_symbol[decision.symbol]
            payload[decision.symbol] = {
                "direction": intent.direction,
                "strength": float(intent.diagnostics.get("strength", 0.0) or 0.0),
                "confidence": intent.confidence,
                "expected_horizon": str(decision.diagnostics.get("expected_horizon", "30m")),
                "raw_score": decision.requested_intent.score,
                "z_score": float(intent.diagnostics.get("z_score", 0.0) or 0.0),
                "rank": int(intent.rank),
                "rank_bucket": str(intent.diagnostics.get("rank_bucket", "HOLD")),
                "normalized_score": float(intent.normalized_score),
                "universe_size": int(intent.diagnostics.get("universe_size", len(output.decisions)) or len(output.decisions)),
            }
        return payload

    @staticmethod
    def _output_to_legacy_decisions(output: DecisionPolicyOutput) -> dict[str, dict[str, Any]]:
        return {
            decision.symbol: {
                "policy_action": decision.policy_action,
                "policy_reason": decision.reason,
                "portfolio_constraints_triggered": [constraint.code for constraint in decision.constraints],
                "score": decision.requested_intent.score,
                "confidence": decision.requested_intent.confidence,
                "signal_action": decision.requested_intent.action,
            }
            for decision in output.decisions
        }
