#Lightweight optimizer layer used by the orchestration cycle.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

@dataclass(slots=True)
class OptimizerQPO:
    """
    Deterministic stand-in for NVIDIA QP optimizer.

    v1 behavior:
    - Convert directional signal intents  into long-only target weights.
    - Reserve a configurable cash buffer.
    - Add benchmark tracking metadata for future v3 extensions.
    """

    max_symbol_weight: float = 0.20
    cash_buffer: float = 0.05

    def optimize_target_weights(
        self,
        signal_intents: dict[str, float | dict[str, float | str]],
        benchmark_symbol: str = "SPY",
        return_diagnostics: bool = False,
    ) -> dict[str, float] | tuple[dict[str, float], dict[str, Any]]:
        positive: dict[str, float] = {}
        candidate_diagnostics: dict[str, dict[str, float]] = {}
        normalized_abs_values: list[float] = []
        for symbol, signal in signal_intents.items():
            if isinstance(signal, dict):
                direction = str(signal.get("direction", "flat")).lower()
                strength = float(signal.get("strength", 0.0))
                confidence = float(signal.get("confidence", 1.0))
                raw_score = float(signal.get("raw_score", strength if direction == "long" else 0.0))
                z_score = float(signal.get("z_score", 0.0))
                normalized_score = float(signal.get("normalized_score", max(-2.5, min(2.5, z_score)) / 2.5))
                normalized_score = max(-1.0, min(1.0, normalized_score))
                rank = int(signal.get("rank", 0) or 0)
                universe_size = int(signal.get("universe_size", len(signal_intents)) or len(signal_intents))
                rank_pct = (
                    1.0 - ((rank - 1) / max(1, universe_size - 1))
                    if rank > 0 and universe_size > 1
                    else 0.5
                )
                normalized_abs_values.append(abs(normalized_score))

                if direction == "long":
                    raw_component = max(strength, 0.0) * max(confidence, 0.0)
                    normalized_component = max(normalized_score, 0.0)
                    rank_component = max(rank_pct, 0.0)
                    allocation_score = (0.65 * normalized_component) + (0.25 * rank_component) + (0.10 * raw_component)
                    positive[symbol] = max(allocation_score, 0.0)
                    candidate_diagnostics[symbol] = {
                        "raw_score": raw_score,
                        "raw_contribution": raw_component,
                        "z_score": z_score,
                        "normalized_score": normalized_score,
                        "normalized_allocation_contribution": normalized_component,
                        "rank_component": rank_component,
                        "allocation_score": allocation_score,
                    }
                else:
                    positive[symbol] = 0.0
                    candidate_diagnostics[symbol] = {
                        "raw_score": raw_score,
                        "raw_contribution": 0.0,
                        "z_score": z_score,
                        "normalized_score": normalized_score,
                        "normalized_allocation_contribution": min(normalized_score, 0.0),
                        "rank_component": 0.0,
                        "allocation_score": 0.0,
                    }
                continue
            positive[symbol] = max(float(signal), 0.0)
            candidate_diagnostics[symbol] = {
                "raw_score": float(signal),
                "raw_contribution": max(float(signal), 0.0),
                "z_score": 0.0,
                "normalized_score": 0.0,
                "normalized_allocation_contribution": 0.0,
                "rank_component": 0.5,
                "allocation_score": max(float(signal), 0.0),
            }
        total_signal = sum(positive.values())
        if total_signal <= 0:
            empty = {benchmark_symbol: 0.0}
            diagnostics = {
                "target_gross_exposure": 0.0,
                "investable_exposure_limit": max(0.0, 1.0 - self.cash_buffer),
                "gross_scaler": 0.0,
                "per_symbol": candidate_diagnostics,
            }
            return (empty, diagnostics) if return_diagnostics else empty

        investable = max(0.0, 1.0 - self.cash_buffer)
        if normalized_abs_values:
            gross_scaler = min(1.0, max(0.15, sum(normalized_abs_values) / len(normalized_abs_values)))
        else:
            gross_scaler = 1.0
        target_gross_exposure = investable * gross_scaler
        target_weights: dict[str, float] = {}
        for symbol, score in positive.items():
            raw_weight = target_gross_exposure * (score / total_signal)
            target_weights[symbol] = min(raw_weight, self.max_symbol_weight)

        allocated = sum(target_weights.values())
        if allocated > 0 and allocated != target_gross_exposure:
            scale = target_gross_exposure / allocated
            for symbol in list(target_weights):
                target_weights[symbol] *= scale

        for symbol, payload in candidate_diagnostics.items():
            payload["final_relative_weight"] = float(target_weights.get(symbol, 0.0))

        target_weights[benchmark_symbol] = 0.0
        diagnostics = {
            "target_gross_exposure": target_gross_exposure,
            "investable_exposure_limit": investable,
            "gross_scaler": gross_scaler,
            "per_symbol": candidate_diagnostics,
        }
        return (target_weights, diagnostics) if return_diagnostics else target_weights
