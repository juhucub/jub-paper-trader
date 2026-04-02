from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.core.settings import get_settings

@dataclass(slots=True)
class DecisionPolicy:
    min_confidence: float = 0.2
    min_score: float = 0.0
    max_position_concentration: float = 0.45
    max_market_volatility: float = 0.08
    min_symbol_liquidity: float = 0.0
    use_structured_signals: bool = get_settings().bot_use_structured_signals

    def evaluate(
        self,
        signals: dict[str, dict[str, float | str]],
        portfolio_state: dict[str, Any],
        market_context: dict[str, Any],
    ) -> dict[str, Any]:
        decisions: dict[str, dict[str, Any]] = {}
        approved_candidates: dict[str, float | dict[str, float | str]] = {}

        cash = float(portfolio_state.get("cash", 0.0))
        concentration = portfolio_state.get("concentration", {}) or {}
        volatility = float(market_context.get("volatility", 0.0))
        liquidity_map = market_context.get("liquidity", {}) or {}

        for symbol, signal in signals.items():
            action = str(signal.get("action", "hold")).lower()
            score = float(signal.get("score", 0.0))
            confidence = float(signal.get("confidence", 0.0))
            direction = str(signal.get("direction", "flat")).lower()
            strength = float(signal.get("strength", abs(score)))
            expected_horizon = str(signal.get("expected_horizon", "30m"))
            current_qty = float((portfolio_state.get("positions", {}) or {}).get(symbol, 0.0))
            is_short_intent = action == "sell" or direction == "short"
            allow_exit_only = is_short_intent and current_qty > 0.0

            if direction in {"long", "short", "flat"}:
                action = {"long": "buy", "short": "sell", "flat": "hold"}[direction]
                if "score" not in signal:
                    score = strength if direction == "long" else -strength if direction == "short" else 0.0
            constraints: list[str] = []

            if action not in {"buy", "sell", "hold"}:
                constraints.append("unsupported_signal_action")
            if confidence < self.min_confidence:
                constraints.append("low_confidence")
            if action == "buy" and score <= self.min_score:
                constraints.append("non_positive_score")
            if action == "buy" and cash <= 0.0:
                constraints.append("insufficient_cash")
            if action == "buy" and volatility > self.max_market_volatility:
                constraints.append("high_market_volatility")
            if action == "buy" and float(concentration.get(symbol, 0.0)) >= self.max_position_concentration:
                constraints.append("position_concentration_limit")
            if action == "buy" and float(liquidity_map.get(symbol, 0.0)) < self.min_symbol_liquidity:
                constraints.append("insufficient_liquidity")
            if is_short_intent and not allow_exit_only:
                constraints.append("short_rejected_long_only")

            policy_action = action
            policy_reason = "policy_approved"
            if constraints:
                policy_action = "skip"
                policy_reason = constraints[0]
            elif allow_exit_only:
                policy_reason = "short_converted_to_exit_only"
            elif action == "hold":
                policy_reason = "signal_hold"

            decisions[symbol] = {
                "policy_action": policy_action,
                "policy_reason": policy_reason,
                "portfolio_constraints_triggered": constraints,
                "score": score,
                "confidence": confidence,
                "signal_action": action,
            }
            if policy_action in {"buy", "sell"}:
                if self.use_structured_signals:
                    approved_candidates[symbol] = {
                        "direction": "long" if policy_action == "buy" else "flat",
                        "strength": max(strength, 0.0) if policy_action == "buy" else 0.0,
                        "confidence": confidence,
                        "expected_horizon": expected_horizon,
                        "raw_score": score,
                        "z_score": float(signal.get("z_score", 0.0)),
                        "rank": int(signal.get("rank", 0) or 0),
                        "rank_bucket": str(signal.get("rank_bucket", "HOLD")),
                        "normalized_score": float(signal.get("normalized_score", signal.get("z_score", 0.0))),
                        "universe_size": int(signal.get("universe_size", len(signals))),
                    }
                else:
                    approved_candidates[symbol] = max(score, 0.0) if policy_action == "buy" else 0.0


        return {
            "approved_candidates": approved_candidates,
            "decisions": decisions,
        }
